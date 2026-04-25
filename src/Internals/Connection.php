<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\Handlers\ConnectHandler;
use Hibla\Postgres\Handlers\CursorHandler;
use Hibla\Postgres\Handlers\QueryResultHandler;
use Hibla\Postgres\Handlers\StreamHandler;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\QueryException;
use SplQueue;
use Throwable;

/**
 * @internal Low-level non-blocking PostgreSQL connection.
 *
 * This class acts purely as a coordinator:
 *   - Owns the ConnectionContext (shared mutable state).
 *   - Implements ConnectionBridge so handlers can call back into lifecycle
 *     methods without a circular class dependency.
 *   - Delegates all I/O and protocol logic to the four handler classes.
 */
class Connection implements ConnectionBridge
{
    private readonly ConnectionContext    $ctx;

    private readonly ConnectHandler       $connectHandler;

    private readonly CursorHandler        $cursorHandler;

    private readonly QueryResultHandler   $queryResultHandler;

    private readonly StreamHandler        $streamHandler;

    public function __construct(private readonly PgSqlConfig $config)
    {
        $this->ctx = new ConnectionContext();

        // Wire handlers together. Order matters: cursor and queryResult are
        // independent of each other, but stream depends on both.
        $this->cursorHandler = new CursorHandler($this->ctx, $this);
        $this->queryResultHandler = new QueryResultHandler($this->ctx, $this, $this->cursorHandler);
        $this->streamHandler = new StreamHandler($this->ctx, $this, $this->cursorHandler, $this->queryResultHandler);
        $this->connectHandler = new ConnectHandler($this->ctx, $this);
    }

    public static function create(PgSqlConfig $config): PromiseInterface
    {
        return (new self($config))->connect();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public function connect(): PromiseInterface
    {
        if ($this->ctx->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(new \LogicException('Connection is already active'));
        }

        $this->ctx->state = ConnectionState::CONNECTING;
        $this->ctx->connectPromise = new Promise();

        error_clear_last();

        $this->ctx->connection = @pg_connect(
            $this->config->toConnectionString(),
            PGSQL_CONNECT_ASYNC | PGSQL_CONNECT_FORCE_NEW
        );

        if ($this->ctx->connection === false) {
            $error = error_get_last();
            $this->ctx->state = ConnectionState::CLOSED;

            return Promise::rejected(new ConnectionException(
                'Failed to initiate connection: ' . ($error['message'] ?? 'Unknown error')
            ));
        }

        $this->ctx->socket = @pg_socket($this->ctx->connection);

        if ($this->ctx->socket === false) {
            $this->ctx->state = ConnectionState::CLOSED;
            @pg_close($this->ctx->connection);

            return Promise::rejected(new ConnectionException('Failed to retrieve underlying socket descriptor'));
        }

        $this->ctx->pollWatcherId = Loop::addWriteWatcher($this->ctx->socket, $this->connectHandler->handle(...));
        $this->ctx->pollWatcherType = 'write';

        return $this->ctx->connectPromise;
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

        if ($status === PGSQL_TRANSACTION_ACTIVE
            || $status === PGSQL_TRANSACTION_INTRANS
            || $status === PGSQL_TRANSACTION_INERROR
        ) {
            return $this->enqueueCommand(CommandRequest::TYPE_QUERY, 'ROLLBACK')
                ->then(fn () => $this->enqueueCommand(CommandRequest::TYPE_RESET, 'DISCARD ALL'))
            ;
        }

        return $this->enqueueCommand(CommandRequest::TYPE_RESET, 'DISCARD ALL');
    }

    public function isReady(): bool
    {
        return $this->ctx->state === ConnectionState::READY;
    }

    public function isClosed(): bool
    {
        return $this->ctx->state === ConnectionState::CLOSED;
    }

    public function getProcessId(): int
    {
        return $this->ctx->connection ? pg_get_pid($this->ctx->connection) : 0;
    }

    public function getTransactionStatus(): int
    {
        return $this->ctx->connection
            ? pg_transaction_status($this->ctx->connection)
            : PGSQL_TRANSACTION_UNKNOWN;
    }

    public function close(): void
    {
        if ($this->ctx->state === ConnectionState::CLOSED) {
            return;
        }

        $this->ctx->state = ConnectionState::CLOSED;
        $this->clearAllWatchers();

        if ($this->ctx->connection !== null) {
            @pg_close($this->ctx->connection);
            $this->ctx->connection = null;
        }

        $exception = new ConnectionException('Connection was closed');

        if ($this->ctx->connectPromise?->isPending()) {
            $this->ctx->connectPromise->reject($exception);
        }

        if ($this->ctx->currentCommand !== null) {
            $this->ctx->currentCommand->promise->reject($exception);
            $this->ctx->currentCommand = null;
        }

        while (! $this->ctx->commandQueue->isEmpty()) {
            $this->ctx->commandQueue->dequeue()->promise->reject($exception);
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    // ── ConnectionBridge implementation ───────────────────────────────────────

    public function onConnectReady(): void
    {
        // ConnectHandler has already set state = READY and cleared poll watchers.
        $promise = $this->ctx->connectPromise;
        $this->ctx->connectPromise = null;
        $promise?->resolve($this); // callers receive the Connection instance
        $this->processNextCommand();
    }

    public function onConnectFailed(string $errorMsg): void
    {
        // Null the promise first so close() does not double-reject it.
        $promise = $this->ctx->connectPromise;
        $this->ctx->connectPromise = null;
        $this->close();
        $promise?->reject(new ConnectionException($errorMsg));
    }

    public function finishCommand(?Throwable $error = null, mixed $value = null): void
    {
        $this->removeQueryReadWatcher();

        $cmd = $this->ctx->currentCommand;
        $this->ctx->currentCommand = null;
        $this->ctx->state = ConnectionState::READY;
        $this->ctx->accumulatedResults = [];
        $this->ctx->isStreamPaused = false;

        $this->ctx->cursor->reset();

        if ($error !== null) {
            // For stream commands the consumer is already iterating — it will never
            // see a promise rejection. Push the error into the stream instead.
            if ($cmd?->type === CommandRequest::TYPE_STREAM && $cmd->context !== null) {
                $cmd->context->error($error);
            }
            $cmd?->promise->reject($error);
        } else {
            $cmd?->promise->resolve($value);
        }

        $this->processNextCommand();
    }

    public function processNextCommand(): void
    {
        if ($this->ctx->state !== ConnectionState::READY
            || $this->ctx->commandQueue->isEmpty()
            || $this->ctx->currentCommand !== null
        ) {
            return;
        }

        $this->ctx->currentCommand = $this->ctx->commandQueue->dequeue();
        $this->ctx->state = ConnectionState::QUERYING;
        $this->ctx->accumulatedResults = [];
        $this->ctx->queryError = null;

        $cmd = $this->ctx->currentCommand;
        $querySent = false;

        try {
            if ($cmd->type === CommandRequest::TYPE_PING) {
                $healthy = pg_connection_status($this->ctx->connection) === PGSQL_CONNECTION_OK;
                $this->finishCommand(
                    $healthy ? null : new ConnectionException('Ping failed. Connection is unhealthy.'),
                    $healthy ? true : null,
                );

                return;
            }

            [$sql, $params] = QueryParser::parse($cmd->sql, $cmd->params);
            $params = $this->normalizeParams($params);

            // For streaming without chunked-rows support, use server-side cursors
            // so PostgreSQL only transmits one batch at a time.
            if ($cmd->type === CommandRequest::TYPE_STREAM && ! function_exists('pg_set_chunked_rows_size')) {
                $cmd->context->setResumeCallback($this->streamHandler->resume(...));
                $this->cursorHandler->init($sql, $params);

                return;
            }

            $sent = $params === []
                ? @pg_send_query($this->ctx->connection, $sql)
                : @pg_send_query_params($this->ctx->connection, $sql, $params);

            if (! $sent) {
                throw new QueryException('Failed to send query: ' . pg_last_error($this->ctx->connection));
            }

            $querySent = true;

            if ($cmd->type === CommandRequest::TYPE_STREAM) {
                $cmd->context->setResumeCallback($this->streamHandler->resume(...));
                // pg_set_chunked_rows_size is guaranteed available here (checked above).
                @pg_set_chunked_rows_size($this->ctx->connection, $cmd->context->bufferSize);
            }

            $this->addQueryReadWatcher();

        } catch (Throwable $e) {
            if ($querySent && $this->ctx->connection !== null) {
                @pg_cancel_query($this->ctx->connection);
            }
            $this->finishCommand($e);
        }
    }

    public function addQueryReadWatcher(): void
    {
        if ($this->ctx->queryWatcherId === null && $this->ctx->socket !== null) {
            $this->ctx->queryWatcherId = Loop::addReadWatcher(
                $this->ctx->socket,
                $this->queryResultHandler->handle(...)
            );
        }
    }

    public function removeQueryReadWatcher(): void
    {
        if ($this->ctx->queryWatcherId !== null) {
            Loop::removeReadWatcher($this->ctx->queryWatcherId);
            $this->ctx->queryWatcherId = null;
        }
    }

    public function pauseStream(): void
    {
        $this->streamHandler->pause();
    }

    public function drainResults(): void
    {
        $this->queryResultHandler->drain();
    }


    private function enqueueCommand(
        string $type,
        string $sql = '',
        array  $params = [],
        mixed  $context = null,
    ): PromiseInterface {
        if ($this->ctx->state === ConnectionState::CLOSED) {
            return Promise::rejected(new ConnectionException('Connection is closed'));
        }

        $promise = new Promise();
        $command = new CommandRequest($type, $promise, $sql, $params, $context);
        $this->ctx->commandQueue->enqueue($command);

        $promise->onCancel(function () use ($command) {
            if ($this->ctx->currentCommand === $command) {
                if ($this->config->enableServerSideCancellation && $this->ctx->connection !== null) {
                    @pg_cancel_query($this->ctx->connection);
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
        while (! $this->ctx->commandQueue->isEmpty()) {
            $cmd = $this->ctx->commandQueue->dequeue();
            if ($cmd !== $command) {
                $temp->enqueue($cmd);
            }
        }
        $this->ctx->commandQueue = $temp;
    }

    private function normalizeParams(array $params): array
    {
        return array_map(
            static fn (mixed $p) => is_bool($p) ? ($p ? '1' : '0') : $p,
            $params
        );
    }

    private function clearAllWatchers(): void
    {
        if ($this->ctx->pollWatcherId !== null) {
            if ($this->ctx->pollWatcherType === 'read') {
                Loop::removeReadWatcher($this->ctx->pollWatcherId);
            } else {
                Loop::removeWriteWatcher($this->ctx->pollWatcherId);
            }
            $this->ctx->pollWatcherId = null;
            $this->ctx->pollWatcherType = null;
        }

        $this->removeQueryReadWatcher();
    }
}
