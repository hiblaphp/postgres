<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\QueryException;
use SplQueue;
use Throwable;

/**
 * @internal This is a low-level, internal class. DO NOT USE IT DIRECTLY.
 *
 * Represents a single, raw non-blocking connection to the PostgreSQL server using libpq.
 * Manages the protocol state machine and asynchronous I/O polling via the Event Loop.
 */
class Connection
{
    private ConnectionState $state = ConnectionState::DISCONNECTED;

    /** @var \PgSql\Connection|resource|null */
    private mixed $connection = null;

    /**
     * @var resource|null Underlying socket file descriptor.
     */
    private mixed $socket = null;

    /** @var SplQueue<CommandRequest> */
    private SplQueue $commandQueue;
    private ?CommandRequest $currentCommand = null;

    private ?Promise $connectPromise = null;
    private ?string $pollWatcherId = null;
    private ?string $queryWatcherId = null;
    private ?string $pollWatcherType = null; // <-- add this

    /** @var array<int, mixed> Results collected during a multi-result or single-row cycle */
    private array $accumulatedResults = [];
    private ?Throwable $queryError = null;

    public function __construct(private readonly PgSqlConfig $config)
    {
        $this->commandQueue = new SplQueue();
    }

    public static function create(PgSqlConfig $config): PromiseInterface
    {
        $connection = new self($config);
        return $connection->connect();
    }

    public function connect(): PromiseInterface
    {
        if ($this->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(new \LogicException('Connection is already active'));
        }

        $this->state = ConnectionState::CONNECTING;
        $this->connectPromise = new Promise();

        error_clear_last();

        $this->connection = @pg_connect(
            $this->config->toConnectionString(),
            PGSQL_CONNECT_ASYNC | PGSQL_CONNECT_FORCE_NEW
        );

        if ($this->connection === false) {
            $error = error_get_last();
            $this->state = ConnectionState::CLOSED;
            return Promise::rejected(new ConnectionException(
                "Failed to initiate connection: " . ($error['message'] ?? 'Unknown error')
            ));
        }

        $this->socket = @pg_socket($this->connection);

        if ($this->socket === false) {
            $this->state = ConnectionState::CLOSED;
            @pg_close($this->connection);
            return Promise::rejected(new ConnectionException("Failed to retrieve underlying socket descriptor"));
        }

        $this->pollWatcherId = Loop::addWriteWatcher($this->socket, $this->handleConnectPoll(...));
        $this->pollWatcherType = 'write';

        return $this->connectPromise;
    }

    public function query(string $sql, array $params = []): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql, $params);
    }

    public function streamQuery(string $sql, array $params = [], int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);
        $promise = $this->enqueueCommand(CommandRequest::TYPE_STREAM, $sql, $params, $stream);
        $stream->bindCommandPromise($promise);

        return $promise->then(fn() => $stream);
    }

    public function ping(): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PING);
    }

    public function reset(): PromiseInterface
    {
        $status = $this->getTransactionStatus();

        if ($status === PGSQL_TRANSACTION_ACTIVE || $status === PGSQL_TRANSACTION_INTRANS || $status === PGSQL_TRANSACTION_INERROR) {
            return $this->enqueueCommand(CommandRequest::TYPE_QUERY, 'ROLLBACK')
                ->then(fn() => $this->enqueueCommand(CommandRequest::TYPE_RESET, 'DISCARD ALL'));
        }

        return $this->enqueueCommand(CommandRequest::TYPE_RESET, 'DISCARD ALL');
    }

    public function isReady(): bool
    {
        return $this->state === ConnectionState::READY;
    }

    public function isClosed(): bool
    {
        return $this->state === ConnectionState::CLOSED;
    }

    public function getProcessId(): int
    {
        return $this->connection ? pg_get_pid($this->connection) : 0;
    }

    public function getTransactionStatus(): int
    {
        return $this->connection ? pg_transaction_status($this->connection) : PGSQL_TRANSACTION_UNKNOWN;
    }

    public function close(): void
    {
        if ($this->state === ConnectionState::CLOSED) {
            return;
        }

        $this->state = ConnectionState::CLOSED;

        $this->clearWatchers();

        if ($this->connection !== null) {
            @pg_close($this->connection);
            $this->connection = null;
        }

        $exception = new ConnectionException('Connection was closed');

        if ($this->connectPromise?->isPending()) {
            $this->connectPromise->reject($exception);
        }

        if ($this->currentCommand !== null) {
            $this->currentCommand->promise->reject($exception);
            $this->currentCommand = null;
        }

        while (!$this->commandQueue->isEmpty()) {
            $this->commandQueue->dequeue()->promise->reject($exception);
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    private function enqueueCommand(string $type, string $sql = '', array $params = [], mixed $context = null): PromiseInterface
    {
        if ($this->state === ConnectionState::CLOSED) {
            return Promise::rejected(new ConnectionException('Connection is closed'));
        }

        $promise = new Promise();
        $command = new CommandRequest($type, $promise, $sql, $params, $context);

        $this->commandQueue->enqueue($command);

        $promise->onCancel(function () use ($command) {
            if ($this->currentCommand === $command) {
                if ($this->config->enableServerSideCancellation && $this->connection !== null) {
                    @pg_cancel_query($this->connection);
                }
            } else {
                $this->removeFromQueue($command);
            }
        });

        $this->processNextCommand();
        return $promise;
    }

    private function removeFromQueue(CommandRequest $command): void
    {
        $temp = new SplQueue();
        while (!$this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();
            if ($cmd !== $command) {
                $temp->enqueue($cmd);
            }
        }
        $this->commandQueue = $temp;
    }

    private function processNextCommand(): void
    {
        if ($this->state !== ConnectionState::READY || $this->commandQueue->isEmpty() || $this->currentCommand !== null) {
            return;
        }

        $this->currentCommand = $this->commandQueue->dequeue();
        $this->state = ConnectionState::QUERYING;
        $this->accumulatedResults = [];
        $this->queryError = null;

        $cmd = $this->currentCommand;

        try {
            if ($cmd->type === CommandRequest::TYPE_PING) {
                if (pg_connection_status($this->connection) === PGSQL_CONNECTION_OK) {
                    $this->finishCommand(null, true);
                } else {
                    $this->finishCommand(new ConnectionException('Ping failed. Connection is unhealthy.'));
                }
                return;
            }

            [$sql, $params] = QueryParser::parse($cmd->sql, $cmd->params);

            $params = $this->normalizeParams($params);

            if ($params === []) {
                $sent = @pg_send_query($this->connection, $sql);
            } else {
                $sent = @pg_send_query_params($this->connection, $sql, $params);
            }

            if (!$sent) {
                throw new QueryException("Failed to send query: " . pg_last_error($this->connection));
            }

            if ($cmd->type === CommandRequest::TYPE_STREAM) {
                @\pg_set_chunked_rows_size($this->connection, 1);
            }
            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
        } catch (Throwable $e) {
            $this->finishCommand($e);
        }
    }

    private function handleConnectPoll(): void
    {
        $freshSocket = $this->getFreshSocket();

        if ($freshSocket === false) {
            $errorMsg = $this->connection ? (pg_last_error($this->connection) ?: 'Failed to retrieve socket during polling') : 'Connection lost';
            $this->state = ConnectionState::CLOSED;
            $this->clearWatchers();
            @pg_close($this->connection);
            $this->connection = null;
            $promise = $this->connectPromise;
            $this->connectPromise = null;
            $promise?->reject(new ConnectionException($errorMsg));
            return;
        }

        $this->socket = $freshSocket;

        $status = @pg_connect_poll($this->connection);

        if ($status === PGSQL_POLLING_FAILED) {
            $errorMsg = pg_last_error($this->connection) ?: 'Connection polling failed';

            $promise = $this->connectPromise;
            $this->connectPromise = null;
            $this->close();
            $promise?->reject(new ConnectionException($errorMsg));
            return;
        }

        if ($status === PGSQL_POLLING_OK) {
            $this->clearWatchers();
            $this->state = ConnectionState::READY;

            $promise = $this->connectPromise;
            $this->connectPromise = null;
            $promise?->resolve($this);

            $this->processNextCommand();
            return;
        }

        $this->clearWatchers();

        if ($status === PGSQL_POLLING_READING) {
            $this->pollWatcherId = Loop::addReadWatcher($freshSocket, $this->handleConnectPoll(...));
            $this->pollWatcherType = 'read';
        } elseif ($status === PGSQL_POLLING_WRITING) {
            $this->pollWatcherId = Loop::addWriteWatcher($freshSocket, $this->handleConnectPoll(...));
            $this->pollWatcherType = 'write';
        }
    }

    private function handleQueryResult(): void
    {
        if (!@pg_consume_input($this->connection)) {
            $this->finishCommand(new ConnectionException("Connection lost during query: " . pg_last_error($this->connection)));
            return;
        }

        while (!@pg_connection_busy($this->connection)) {
            $res = @pg_get_result($this->connection);

            if ($res === false) {
                if ($this->queryError) {
                    $this->finishCommand($this->queryError);
                    return;
                }

                if ($this->currentCommand->type === CommandRequest::TYPE_STREAM) {
                    $this->currentCommand->context->complete();
                    $this->finishCommand(null, null);
                } else {
                    $this->processAccumulatedResults();
                }
                return;
            }

            $status = pg_result_status($res);

            if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
                $errorMsg = pg_result_error($res);
                $this->queryError = new QueryException($errorMsg !== '' ? $errorMsg : 'Unknown query error');
                continue;
            }

            if ($this->currentCommand->type === CommandRequest::TYPE_STREAM) {
                if ($status === PGSQL_TUPLES_CHUNK) {
                    $row = pg_fetch_assoc($res);
                    if ($row !== false) {
                        $this->currentCommand->context->push($row);
                    }
                }
            } else {
                $this->accumulatedResults[] = $res;
            }
        }
    }

    private function processAccumulatedResults(): void
    {
        $res = end($this->accumulatedResults);

        if ($res === false) {
            $this->finishCommand(null, new Result(connectionId: $this->getProcessId()));
            return;
        }

        $rows = pg_fetch_all($res) ?: [];
        $affected = pg_affected_rows($res);
        $insertedOid = pg_last_oid($res) !== false ? (int) pg_last_oid($res) : null;

        $fields = [];
        $numFields = pg_num_fields($res);
        for ($i = 0; $i < $numFields; $i++) {
            $fields[] = pg_field_name($res, $i);
        }

        $resultObj = new Result(
            affectedRows: $affected,
            connectionId: $this->getProcessId(),
            insertedOid: $insertedOid,
            columns: $fields,
            rows: $rows
        );

        $this->finishCommand(null, $resultObj);
    }

    private function finishCommand(?Throwable $error = null, mixed $value = null): void
    {
        if ($this->queryWatcherId !== null) {
            Loop::removeReadWatcher($this->queryWatcherId);
            $this->queryWatcherId = null;
        }

        $cmd = $this->currentCommand;
        $this->currentCommand = null;
        $this->state = ConnectionState::READY;
        $this->accumulatedResults = [];

        if ($error) {
            $cmd?->promise->reject($error);
        } else {
            $cmd?->promise->resolve($value);
        }

        $this->processNextCommand();
    }

    private function normalizeParams(array $params): array
    {
        $normalized = [];
        foreach ($params as $param) {
            if (is_bool($param)) {
                $normalized[] = $param ? '1' : '0';
            } else {
                $normalized[] = $param;
            }
        }

        return $normalized;
    }

    /**
     * Fetches the current socket from libpq for the active connection.
     *
     * [FIX #2] This must be called on every pg_connect_poll() iteration rather than
     * reading the cached $this->socket, because libpq may replace the underlying file
     * descriptor between poll calls (documented in libpq: "do not assume that the socket
     * remains the same across PQconnectPoll calls").
     *
     * @return resource|false
     */
    private function getFreshSocket(): mixed
    {
        if ($this->connection === null) {
            return false;
        }

        $socket = @pg_socket($this->connection);
        return $socket !== false ? $socket : false;
    }

    /**
     * Utility to cleanly stop all Event Loop watchers.
     * This prevents resource leaks and dangling callbacks when the connection
     * is closed or a command is finalized.
     */
    private function clearWatchers(): void
    {
        if ($this->pollWatcherId !== null) {
            if ($this->pollWatcherType === 'read') {
                Loop::removeReadWatcher($this->pollWatcherId);
            } else {
                Loop::removeWriteWatcher($this->pollWatcherId);
            }
            $this->pollWatcherId = null;
            $this->pollWatcherType = null; 
        }

        if ($this->queryWatcherId !== null) {
            Loop::removeReadWatcher($this->queryWatcherId);
            $this->queryWatcherId = null;
        }
    }
}
