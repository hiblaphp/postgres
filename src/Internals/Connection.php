<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\Handlers\ConnectHandler;
use Hibla\Postgres\Handlers\QueryHandler;
use Hibla\Postgres\Handlers\StreamHandler;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;
use SplQueue;
use Throwable;

/**
 * @internal This is a low‑level, internal class. DO NOT USE IT DIRECTLY.
 *
 * Represents a single, raw non‑blocking connection to the PostgreSQL server using libpq.
 * Manages the protocol state machine and asynchronous I/O polling via the Event Loop.
 *
 * Streaming strategy (in priority order):
 *  1. pg_set_chunked_rows_size  — libpq 17+ / PHP 8.4+: true libpq‑level chunking.
 *  2. Server‑side cursor        — any PostgreSQL version: DECLARE / FETCH batches give
 *                                 real TCP backpressure; memory stays O(bufferSize).
 */
class Connection
{
    private ConnectionState $state = ConnectionState::DISCONNECTED;

    /**
     * @var \PgSql\Connection|resource|null
     */
    private mixed $connection = null;

    /**
     * @var resource|null
     */
    private mixed $socket = null;

    /**
     * @var SplQueue<CommandRequest>
     */
    private SplQueue $commandQueue;

    private ?CommandRequest $currentCommand = null;

    private ?Promise $connectPromise = null;

    private ?string $pollWatcherId = null;

    private ?string $queryWatcherId = null;

    private ?string $pollWatcherType = null;

    /**
     * @var array<int, mixed> Results collected during a multi‑result cycle
     */
    private array $accumulatedResults = [];

    private ?Throwable $queryError = null;

    // Handlers
    private readonly StreamHandler $streamHandler;

    private readonly QueryHandler $queryHandler;

    public function __construct(private readonly PgSqlConfig $config)
    {
        $this->commandQueue = new SplQueue();
        $this->streamHandler = new StreamHandler();
        $this->queryHandler = new QueryHandler($this->streamHandler);
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
                'Failed to initiate connection: ' . ($error['message'] ?? 'Unknown error')
            ));
        }

        $this->socket = @pg_socket($this->connection);
        if ($this->socket === false) {
            $this->state = ConnectionState::CLOSED;
            @pg_close($this->connection);

            return Promise::rejected(new ConnectionException('Failed to retrieve underlying socket descriptor'));
        }

        $this->pollWatcherId = Loop::addWriteWatcher($this->socket, fn () => ConnectHandler::handlePoll($this));
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
        $commandPromise = $this->enqueueCommand(CommandRequest::TYPE_STREAM, $sql, $params, $stream);
        $stream->bindCommandPromise($commandPromise);

        return Promise::resolved($stream);
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
                ->then(fn () => $this->enqueueCommand(CommandRequest::TYPE_RESET, 'DISCARD ALL'))
            ;
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

        while (! $this->commandQueue->isEmpty()) {
            $this->commandQueue->dequeue()->promise->reject($exception);
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    // ── Accessors needed by handlers ─────────────────────────────────────

    /**
     * @internal
     */
    public function getConnectionResource(): mixed
    {
        return $this->connection;
    }

    /**
     * @internal
     */
    public function setConnectionResource(mixed $conn): void
    {
        $this->connection = $conn;
    }

    /**
     * @internal
     */
    public function getSocket(): mixed
    {
        return $this->socket;
    }

    /**
     * @internal
     */
    public function setSocket(mixed $socket): void
    {
        $this->socket = $socket;
    }

    /**
     * @internal
     */
    public function getState(): ConnectionState
    {
        return $this->state;
    }

    /**
     * @internal
     */
    public function setState(ConnectionState $state): void
    {
        $this->state = $state;
    }

    /**
     * @internal
     */
    public function getCurrentCommand(): ?CommandRequest
    {
        return $this->currentCommand;
    }

    /**
     * @internal
     */
    public function setCurrentCommand(?CommandRequest $cmd): void
    {
        $this->currentCommand = $cmd;
    }

    /**
     * @internal
     */
    public function getConnectPromise(): ?Promise
    {
        return $this->connectPromise;
    }

    /**
     * @internal
     */
    public function setConnectPromise(?Promise $promise): void
    {
        $this->connectPromise = $promise;
    }

    /**
     * @internal
     */
    public function setPollWatcher(?string $id, ?string $type): void
    {
        $this->pollWatcherId = $id;
        $this->pollWatcherType = $type;
    }

    /**
     * @internal
     */
    public function getQueryWatcherId(): ?string
    {
        return $this->queryWatcherId;
    }

    /**
     * @internal
     */
    public function setQueryWatcherId(?string $id): void
    {
        $this->queryWatcherId = $id;
    }

    /**
     * @internal
     */
    public function getAccumulatedResults(): array
    {
        return $this->accumulatedResults;
    }

    /**
     * @internal
     */
    public function addAccumulatedResult(mixed $res): void
    {
        $this->accumulatedResults[] = $res;
    }

    /**
     * @internal
     */
    public function getQueryError(): ?Throwable
    {
        return $this->queryError;
    }

    /**
     * @internal
     */
    public function setQueryError(?Throwable $error): void
    {
        $this->queryError = $error;
    }

    /**
     * @internal
     */
    public function getQueryHandler(): QueryHandler
    {
        return $this->queryHandler;
    }

    /**
     * @internal
     */
    public function getStreamHandler(): StreamHandler
    {
        return $this->streamHandler;
    }

    /**
     * @internal
     */
    public function normalizeParams(array $params): array
    {
        return array_map(fn ($p) => is_bool($p) ? ($p ? '1' : '0') : $p, $params);
    }

    /**
     * @internal
     */
    public function getFreshSocket(): mixed
    {
        if ($this->connection === null) {
            return false;
        }
        $socket = @pg_socket($this->connection);

        return $socket !== false ? $socket : false;
    }

    /**
     * @internal
     */
    public function clearWatchers(): void
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

    /**
     * @internal
     */
    public function finishCommand(?Throwable $error = null, mixed $value = null): void
    {
        if ($this->queryWatcherId !== null) {
            Loop::removeReadWatcher($this->queryWatcherId);
            $this->queryWatcherId = null;
        }

        $cmd = $this->currentCommand;
        $this->currentCommand = null;
        $this->state = ConnectionState::READY;
        $this->accumulatedResults = [];
        $this->queryError = null;
        $this->streamHandler->resetPaused();

        // Reset streaming state
        $this->streamHandler->reset();

        if ($error) {
            if ($cmd?->type === CommandRequest::TYPE_STREAM && $cmd->context !== null) {
                $cmd->context->error($error);
            }
            $cmd?->promise->reject($error);
        } else {
            $cmd?->promise->resolve($value);
        }

        $this->processNextCommand();
    }

    /**
     * @internal called by resume callback and from ConnectHandler
     */
    public function resumeStream(): void
    {
        $this->streamHandler->resume($this);
    }

    // ── Command queue ─────────────────────────────────────────────────────

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
        while (! $this->commandQueue->isEmpty()) {
            $cmd = $this->commandQueue->dequeue();
            if ($cmd !== $command) {
                $temp->enqueue($cmd);
            }
        }
        $this->commandQueue = $temp;
    }

    /**
     * @internal
     */
    public function processNextCommand(): void
    {
        if ($this->state !== ConnectionState::READY || $this->commandQueue->isEmpty() || $this->currentCommand !== null) {
            return;
        }

        $this->currentCommand = $this->commandQueue->dequeue();
        $this->state = ConnectionState::QUERYING;
        $this->accumulatedResults = [];
        $this->queryError = null;

        $this->queryHandler->startCommand($this);
    }
}
