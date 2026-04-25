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
 *
 * Streaming strategy (in priority order):
 *  1. pg_set_chunked_rows_size  — libpq 17+ / PHP 8.4+: true libpq-level chunking.
 *  2. Server-side cursor        — any PostgreSQL version: DECLARE / FETCH batches give
 *                                 real TCP backpressure; memory stays O(bufferSize).
 */
class Connection
{
    private ConnectionState $state = ConnectionState::DISCONNECTED;

    /** @var \PgSql\Connection|resource|null */
    private mixed $connection = null;

    /** @var resource|null */
    private mixed $socket = null;

    /** @var SplQueue<CommandRequest> */
    private SplQueue $commandQueue;
    private ?CommandRequest $currentCommand = null;

    private ?Promise $connectPromise = null;
    private ?string $pollWatcherId = null;
    private ?string $queryWatcherId = null;
    private ?string $pollWatcherType = null;

    /** @var array<int, mixed> Results collected during a multi-result cycle */
    private array $accumulatedResults = [];
    private ?Throwable $queryError = null;
    private bool $isStreamPaused = false;

    private const CURSOR_PHASE_NONE = 0; // Not in cursor mode
    private const CURSOR_PHASE_BEGIN = 1; // Awaiting BEGIN result
    private const CURSOR_PHASE_DECLARE = 2; // Awaiting DECLARE result
    private const CURSOR_PHASE_FETCH = 3; // Awaiting FETCH result / paused between fetches
    private const CURSOR_PHASE_CLOSE = 4; // Awaiting CLOSE [+ COMMIT] result
    private const CURSOR_PHASE_ROLLBACK = 5; // Awaiting ROLLBACK result after error

    private int        $cursorPhase = self::CURSOR_PHASE_NONE;
    private string     $cursorName = '_hibla_cursor';
    private string     $cursorSQL = '';
    private array      $cursorParams = [];
    private bool       $cursorOwnsTransaction = false;
    private ?Throwable $cursorError = null;

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
                'Failed to initiate connection: ' . ($error['message'] ?? 'Unknown error')
            ));
        }

        $this->socket = @pg_socket($this->connection);

        if ($this->socket === false) {
            $this->state = ConnectionState::CLOSED;
            @pg_close($this->connection);

            return Promise::rejected(new ConnectionException('Failed to retrieve underlying socket descriptor'));
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
        $commandPromise = $this->enqueueCommand(CommandRequest::TYPE_STREAM, $sql, $params, $stream);
        $stream->bindCommandPromise($commandPromise);

        // Resolve immediately so the consumer can start iterating before all rows
        // arrive. Errors surface through stream->error() → getIterator() throws.
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

    // ── Command queue ─────────────────────────────────────────────────────────

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
        $querySent = false;

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

            // For streaming without chunked-rows support, use server-side cursors
            // so that PostgreSQL only transmits one batch at a time, keeping memory
            // consumption bounded to O(bufferSize) regardless of result set size.
            if ($cmd->type === CommandRequest::TYPE_STREAM && ! function_exists('pg_set_chunked_rows_size')) {
                $cmd->context->setResumeCallback($this->resumeStream(...));
                $this->initCursorStream($sql, $params);

                return;
            }

            // Normal path: send the query directly
            if ($params === []) {
                $sent = @pg_send_query($this->connection, $sql);
            } else {
                $sent = @pg_send_query_params($this->connection, $sql, $params);
            }

            if (! $sent) {
                throw new QueryException('Failed to send query: ' . pg_last_error($this->connection));
            }

            $querySent = true;

            if ($cmd->type === CommandRequest::TYPE_STREAM) {
                $cmd->context->setResumeCallback($this->resumeStream(...));
                // pg_set_chunked_rows_size is guaranteed available here (checked above)
                @pg_set_chunked_rows_size($this->connection, $cmd->context->bufferSize);
            }

            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));

        } catch (Throwable $e) {
            if ($querySent && $this->connection !== null) {
                @pg_cancel_query($this->connection);
            }
            $this->finishCommand($e);
        }
    }

    // ── Cursor-based streaming ────────────────────────────────────────────────

    /**
     * Kicks off the cursor protocol. Sends BEGIN if we are not already inside a
     * transaction (so we can COMMIT/ROLLBACK around the cursor lifetime), then
     * DECLARE, then the first FETCH.
     */
    private function initCursorStream(string $sql, array $params): void
    {
        $this->cursorSQL = $sql;
        $this->cursorParams = $params;
        $this->cursorOwnsTransaction = ($this->getTransactionStatus() === PGSQL_TRANSACTION_IDLE);

        if ($this->cursorOwnsTransaction) {
            $this->cursorPhase = self::CURSOR_PHASE_BEGIN;
            $sent = @pg_send_query($this->connection, 'BEGIN');
        } else {
            $this->cursorPhase = self::CURSOR_PHASE_DECLARE;
            $sent = $this->dispatchCursorDeclare();
        }

        if (! $sent) {
            $this->cursorPhase = self::CURSOR_PHASE_NONE;
            $this->finishCommand(new QueryException(
                'Failed to init cursor stream: ' . pg_last_error($this->connection)
            ));

            return;
        }

        $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
    }

    private function dispatchCursorDeclare(): bool
    {
        $sql = "DECLARE {$this->cursorName} NO SCROLL CURSOR FOR {$this->cursorSQL}";

        if ($this->cursorParams === []) {
            return (bool) @pg_send_query($this->connection, $sql);
        }

        // The $1..$n placeholders inside the cursor's SELECT are bound to these
        // parameter values via the extended query protocol.
        return (bool) @pg_send_query_params($this->connection, $sql, $this->cursorParams);
    }

    private function dispatchCursorFetch(): void
    {
        $fetchSize = $this->currentCommand->context->bufferSize;
        $sent = @pg_send_query($this->connection, "FETCH {$fetchSize} FROM {$this->cursorName}");

        if (! $sent) {
            $this->cursorPhase = self::CURSOR_PHASE_NONE;
            $this->finishCommand(new QueryException(
                'Failed to send FETCH: ' . pg_last_error($this->connection)
            ));

            return;
        }

        // Re-arm the read watcher if pauseStream() removed it
        if ($this->queryWatcherId === null) {
            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
        }
    }

    private function dispatchCursorClose(): void
    {
        // Bundle COMMIT with CLOSE when we own the transaction so the two
        // acknowledgements arrive together and we only need one round-trip.
        $sql = $this->cursorOwnsTransaction
            ? "CLOSE {$this->cursorName}; COMMIT"
            : "CLOSE {$this->cursorName}";
        $sent = @pg_send_query($this->connection, $sql);

        if (! $sent) {
            // Best effort: mark the stream complete and move on.
            $this->cursorPhase = self::CURSOR_PHASE_NONE;
            $this->currentCommand->context->complete();
            $this->finishCommand(null, null);

            return;
        }

        $this->cursorPhase = self::CURSOR_PHASE_CLOSE;

        if ($this->queryWatcherId === null) {
            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
        }
    }

    private function dispatchCursorRollback(Throwable $error): void
    {
        $this->cursorError = $error;
        $sent = @pg_send_query($this->connection, 'ROLLBACK');

        if (! $sent) {
            $this->cursorPhase = self::CURSOR_PHASE_NONE;
            $this->finishCommand($error);

            return;
        }

        $this->cursorPhase = self::CURSOR_PHASE_ROLLBACK;

        if ($this->queryWatcherId === null) {
            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
        }
    }

    /**
     * Advances the cursor state machine by one step.
     * Called from handleQueryResult() whenever the connection is not busy.
     */
    private function stepCursorPhase(): void
    {
        if (@pg_connection_busy($this->connection)) {
            return; // More data still arriving; watcher will fire again
        }

        $res = @pg_get_result($this->connection);
        if ($res === false) {
            return; // No complete result in buffer yet
        }

        $status = pg_result_status($res);

        // Errors in any non-terminal phase: surface them and clean up the transaction.
        if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
            $msg = pg_result_error($res);
            @pg_free_result($res);
            $error = new QueryException($msg !== '' ? $msg : 'Unknown query error');

            if ($this->cursorOwnsTransaction
                && $this->cursorPhase !== self::CURSOR_PHASE_ROLLBACK
                && $this->cursorPhase !== self::CURSOR_PHASE_CLOSE
            ) {
                $this->dispatchCursorRollback($error);
            } else {
                $this->cursorPhase = self::CURSOR_PHASE_NONE;
                $this->finishCommand($error);
            }

            return;
        }

        switch ($this->cursorPhase) {

            case self::CURSOR_PHASE_BEGIN:
                @pg_free_result($res);
                $this->cursorPhase = self::CURSOR_PHASE_DECLARE;
                if (! $this->dispatchCursorDeclare()) {
                    $this->dispatchCursorRollback(new QueryException(
                        'Failed to send DECLARE: ' . pg_last_error($this->connection)
                    ));
                }

                break;

            case self::CURSOR_PHASE_DECLARE:
                @pg_free_result($res);
                $this->cursorPhase = self::CURSOR_PHASE_FETCH;
                $this->dispatchCursorFetch();

                break;

            case self::CURSOR_PHASE_FETCH:
                $rowCount = pg_num_rows($res);

                if ($rowCount > 0) {
                    while ($row = pg_fetch_assoc($res)) {
                        $this->currentCommand->context->push($row);
                    }
                }
                @pg_free_result($res);

                if ($rowCount === 0) {
                    // Cursor exhausted — close it and signal the stream
                    $this->dispatchCursorClose();

                    return;
                }

                // BACKPRESSURE: buffer full — pause until consumer drains below half.
                // resumeStream() will call dispatchCursorFetch() to get the next batch.
                if ($this->currentCommand->context->isFull()) {
                    $this->pauseStream();

                    return;
                }

                $this->dispatchCursorFetch();

                break;

            case self::CURSOR_PHASE_CLOSE:
                @pg_free_result($res);

                // If we sent "CLOSE name; COMMIT", drain the COMMIT acknowledgement too.
                // After pg_consume_input() both responses are already in libpq's buffer,
                // so pg_connection_busy() will return false immediately.
                while (! @pg_connection_busy($this->connection)) {
                    $extra = @pg_get_result($this->connection);
                    if ($extra === false) {
                        break;
                    }
                    @pg_free_result($extra);
                }

                $this->cursorPhase = self::CURSOR_PHASE_NONE;
                $this->currentCommand->context->complete();
                $this->finishCommand(null, null);

                break;

            case self::CURSOR_PHASE_ROLLBACK:
                @pg_free_result($res);
                $error = $this->cursorError;
                $this->cursorPhase = self::CURSOR_PHASE_NONE;
                $this->cursorError = null;
                $this->finishCommand($error);

                break;
        }
    }

    // ── I/O handlers ──────────────────────────────────────────────────────────

    private function handleConnectPoll(): void
    {
        $freshSocket = $this->getFreshSocket();

        if ($freshSocket === false) {
            $errorMsg = $this->connection
                ? (pg_last_error($this->connection) ?: 'Failed to retrieve socket during polling')
                : 'Connection lost';
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
        if (! @pg_consume_input($this->connection)) {
            $this->finishCommand(new ConnectionException(
                'Connection lost during query: ' . pg_last_error($this->connection)
            ));

            return;
        }

        // Route to the appropriate handler based on whether we are running in
        // cursor mode (server-side batches) or chunked/standard mode.
        if ($this->cursorPhase !== self::CURSOR_PHASE_NONE) {
            $this->stepCursorPhase();
        } else {
            $this->drainResults();
        }
    }

    private function drainResults(): void
    {
        while (! @pg_connection_busy($this->connection)) {

            // BACKPRESSURE: stop draining if the consumer's buffer is full.
            // pauseStream() removes the read watcher; resumeStream() re-enters here.
            if ($this->currentCommand?->type === CommandRequest::TYPE_STREAM) {
                if ($this->currentCommand->context->isFull()) {
                    $this->pauseStream();

                    return;
                }
            }

            $res = @pg_get_result($this->connection);

            if ($res === false) {
                if ($this->queryError !== null) {
                    $this->finishCommand($this->queryError);

                    return;
                }

                if ($this->currentCommand?->type === CommandRequest::TYPE_STREAM) {
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
                @pg_free_result($res);

                continue; // Keep draining so libpq's buffer is fully flushed
            }

            if ($this->currentCommand?->type === CommandRequest::TYPE_STREAM) {
                $isChunk = defined('PGSQL_TUPLES_CHUNK') && $status === PGSQL_TUPLES_CHUNK;
                $isSingle = defined('PGSQL_SINGLE_TUPLE') && $status === PGSQL_SINGLE_TUPLE;
                $isOk = $status === PGSQL_TUPLES_OK;

                if ($isChunk || $isSingle || $isOk) {
                    while ($row = pg_fetch_assoc($res)) {
                        $this->currentCommand->context->push($row);
                    }
                }
                @pg_free_result($res);
            } else {
                $this->accumulatedResults[] = $res;
            }
        }

        // Server still sending data. Re-arm the read watcher if it was removed
        // (e.g., by a resume→re-pause cycle) so we get called again when the
        // next chunk arrives.
        if (! $this->isStreamPaused && $this->queryWatcherId === null && $this->currentCommand !== null) {
            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
        }
    }

    private function pauseStream(): void
    {
        $this->isStreamPaused = true;
        if ($this->queryWatcherId !== null) {
            Loop::removeReadWatcher($this->queryWatcherId);
            $this->queryWatcherId = null;
        }
    }

    public function resumeStream(): void
    {
        if (! $this->isStreamPaused) {
            return;
        }
        $this->isStreamPaused = false;

        if ($this->currentCommand === null) {
            return;
        }

        if ($this->cursorPhase === self::CURSOR_PHASE_FETCH) {
            // Cursor mode: request the next batch from the server
            $this->dispatchCursorFetch();

            return;
        }

        // Chunked mode: drain whatever libpq already has in its internal buffer
        $this->drainResults();

        if (! $this->isStreamPaused && $this->queryWatcherId === null && $this->currentCommand !== null) {
            $this->queryWatcherId = Loop::addReadWatcher($this->socket, $this->handleQueryResult(...));
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

        $this->finishCommand(null, new Result(
            affectedRows: $affected,
            connectionId: $this->getProcessId(),
            insertedOid:  $insertedOid,
            columns:      $fields,
            rows:         $rows,
        ));
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
        $this->isStreamPaused = false;

        // Reset cursor state unconditionally
        $this->cursorPhase = self::CURSOR_PHASE_NONE;
        $this->cursorSQL = '';
        $this->cursorParams = [];
        $this->cursorOwnsTransaction = false;
        $this->cursorError = null;

        if ($error) {
            // For stream commands the consumer is already iterating — it will never
            // see a promise rejection. Push the error into the stream so that
            // getIterator() throws on the next iteration.
            if ($cmd?->type === CommandRequest::TYPE_STREAM && $cmd->context !== null) {
                $cmd->context->error($error);
            }
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

    /** @return resource|false */
    private function getFreshSocket(): mixed
    {
        if ($this->connection === null) {
            return false;
        }
        $socket = @pg_socket($this->connection);

        return $socket !== false ? $socket : false;
    }

    /**
     * Removes all active Event Loop watchers.
     * Called on close or command finalization to prevent resource leaks.
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
