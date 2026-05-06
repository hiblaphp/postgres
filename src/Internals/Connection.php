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
    private readonly ConnectionContext $ctx;

    private readonly ConnectHandler $connectHandler;

    private readonly CursorHandler $cursorHandler;

    private readonly QueryResultHandler $queryResultHandler;

    private readonly StreamHandler $streamHandler;

    private readonly PgSqlConfig $config;

    /**
     * Monotonically increasing counter used to generate unique statement names.
     */
    private int $stmtCounter = 0;

    /**
     * Tracks in-flight pg_cancel_backend promises keyed by process ID (PID).
     *
     * Keying by PID is intentional: PostgreSQL has exactly one running query
     * per backend at any moment, so a second cancel for the same PID before
     * the first resolves is redundant. The idempotency guard in
     * dispatchCancelBackend() prevents the second call from overwriting the
     * first promise and orphaning it.
     *
     * Each promise is created and registered SYNCHRONOUSLY inside
     * dispatchCancelBackend() before nextTick() schedules the work, so
     * close() always sees a non-empty map regardless of when it runs
     * relative to the callback.
     *
     * Entries are removed once the cancel promise settles, keeping the
     * map lean across the connection lifetime.
     *
     * @var array<int, Promise<mixed>>
     */
    private array $pendingCancels = [];

    /**
     * Set to true when a query was cancelled mid-execution via pg_cancel_backend.
     *
     * The pool MUST check this via wasQueryCancelled() and wait for the
     * connection to return to READY state before reuse. Unlike MySQL's KILL
     * QUERY, pg_cancel_backend causes the server to send an ErrorResponse on
     * the main wire, which QueryResultHandler will consume and route through
     * finishCommand() — resetting state back to READY automatically.
     *
     * The flag exists purely as a signal to the pool layer so it can defer
     * returning this connection until that drain cycle completes.
     */
    private bool $wasQueryCancelled = false;

    /**
     * @param PgSqlConfig|array<string, mixed>|string $config
     */
    public function __construct(PgSqlConfig|array|string $config)
    {
        $this->config = match (true) {
            $config instanceof PgSqlConfig => $config,
            \is_array($config) => PgSqlConfig::fromArray($config),
            \is_string($config) => PgSqlConfig::fromUri($config),
        };

        $this->ctx = new ConnectionContext();

        $this->cursorHandler = new CursorHandler($this->ctx, $this);
        $this->queryResultHandler = new QueryResultHandler($this->ctx, $this, $this->cursorHandler);
        $this->streamHandler = new StreamHandler($this->ctx, $this, $this->cursorHandler, $this->queryResultHandler);
        $this->connectHandler = new ConnectHandler($this->ctx, $this);
    }

    /**
     * @param PgSqlConfig|array<string, mixed>|string $config
     *
     * @return PromiseInterface<self>
     */
    public static function create(PgSqlConfig|array|string $config): PromiseInterface
    {
        return new self($config)->connect();
    }

    /**
     * @return PromiseInterface<self>
     */
    public function connect(): PromiseInterface
    {
        if ($this->ctx->state !== ConnectionState::DISCONNECTED) {
            return Promise::rejected(new \LogicException('Connection is already active'));
        }

        $this->ctx->state = ConnectionState::CONNECTING;

        /** @var Promise<self> $connectPromise */
        $connectPromise = new Promise();
        $this->ctx->connectPromise = $connectPromise;

        error_clear_last();

        $connection = @pg_connect(
            $this->config->toConnectionString(),
            PGSQL_CONNECT_ASYNC | PGSQL_CONNECT_FORCE_NEW
        );

        if ($connection === false) {
            $error = error_get_last();
            $this->ctx->state = ConnectionState::CLOSED;

            return Promise::rejected(new ConnectionException(
                'Failed to initiate connection: ' . ($error['message'] ?? 'Unknown error')
            ));
        }

        $this->ctx->connection = $connection;

        $socket = @pg_socket($this->ctx->connection);

        if ($socket === false) {
            $this->ctx->state = ConnectionState::CLOSED;
            @pg_close($this->ctx->connection);
            $this->ctx->connection = null;

            return Promise::rejected(new ConnectionException('Failed to retrieve underlying socket descriptor'));
        }

        $this->ctx->socket = $socket;

        $this->ctx->pollWatcherId = Loop::addWriteWatcher($this->ctx->socket, $this->connectHandler->handle(...));
        $this->ctx->pollWatcherType = 'write';

        return $this->ctx->connectPromise;
    }

    /**
     * Executes a plain SQL query and returns the full buffered result.
     * Do NOT pass user-supplied values directly — use prepare() + executeStatement() instead.
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PostgresResult>
     */
    public function query(string $sql): PromiseInterface
    {
        /** @var PromiseInterface<\Hibla\Postgres\Interfaces\PostgresResult> $promise */
        $promise = $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);

        return $promise;
    }

    /**
     * Streams a plain SQL query row-by-row.
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PostgresRowStream>
     */
    public function streamQuery(string $sql, int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);

        /** @var Promise<RowStream> $outerPromise */
        $outerPromise = new Promise();

        $stream->setOuterPromiseCallbacks(
            onReady: static function () use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->resolve($stream);
                }
            },
            onError: static function (Throwable $e) use ($outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->reject($e);
                }
            },
        );

        $commandPromise = $this->enqueueCommand(CommandRequest::TYPE_STREAM, $sql, [], $stream);
        $stream->bindCommandPromise($commandPromise);

        $outerPromise->onCancel($stream->cancel(...));

        return $outerPromise;
    }

    /**
     * Prepares a SQL statement on the server and returns a PreparedStatement handle.
     *
     * Converts `?` and `:name` placeholders to the PostgreSQL `$1, $2, …` format automatically.
     *
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $name = 'stmt_' . (++$this->stmtCounter);
        [$parsedSql,, $paramNames] = ParamParser::parsePlaceholders($sql);

        $connection = $this;
        $factory = static fn () => new PreparedStatement($connection, $name, $paramNames);

        /** @var PromiseInterface<PreparedStatement> $promise */
        $promise = $this->enqueueCommand(CommandRequest::TYPE_PREPARE, $parsedSql, [], $factory);

        return $promise;
    }

    /**
     * @param array<string|int, mixed> $params
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PostgresResult>
     */
    public function executeStatement(PreparedStatement $stmt, array $params): PromiseInterface
    {
        /** @var PromiseInterface<\Hibla\Postgres\Interfaces\PostgresResult> $promise */
        $promise = $this->enqueueCommand(
            CommandRequest::TYPE_EXECUTE,
            $stmt->name,
            $this->resolveStatementParams($stmt, $params),
        );

        return $promise;
    }

    /**
     * @param array<string|int, mixed> $params
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PostgresRowStream>
     */
    public function executeStatementStream(PreparedStatement $stmt, array $params, int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);

        /** @var Promise<RowStream> $outerPromise */
        $outerPromise = new Promise();

        $stream->setOuterPromiseCallbacks(
            onReady: static function () use ($stream, $outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->resolve($stream);
                }
            },
            onError: static function (Throwable $e) use ($outerPromise): void {
                if ($outerPromise->isPending()) {
                    $outerPromise->reject($e);
                }
            },
        );

        $commandPromise = $this->enqueueCommand(
            CommandRequest::TYPE_EXECUTE_STREAM,
            $stmt->name,
            $this->resolveStatementParams($stmt, $params),
            $stream,
        );

        $stream->bindCommandPromise($commandPromise);
        $outerPromise->onCancel($stream->cancel(...));

        return $outerPromise;
    }

    /**
     * Deallocates a prepared statement on the server.
     *
     * @return PromiseInterface<void>
     */
    public function closeStatement(string $name): PromiseInterface
    {
        /** @var PromiseInterface<void> $promise */
        $promise = $this->enqueueCommand(CommandRequest::TYPE_QUERY, "DEALLOCATE {$name}");

        return $promise;
    }

    /**
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface
    {
        /** @var PromiseInterface<bool> $promise */
        $promise = $this->enqueueCommand(CommandRequest::TYPE_PING);

        return $promise;
    }

    /**
     * @return PromiseInterface<void>
     */
    public function reset(): PromiseInterface
    {
        $status = $this->getTransactionStatus();

        if (
            $status === PGSQL_TRANSACTION_ACTIVE
            || $status === PGSQL_TRANSACTION_INTRANS
            || $status === PGSQL_TRANSACTION_INERROR
        ) {
            /** @var PromiseInterface<void> */
            return $this->enqueueCommand(CommandRequest::TYPE_QUERY, 'ROLLBACK')
                ->then(fn () => $this->enqueueCommand(CommandRequest::TYPE_RESET, 'DISCARD ALL'))
            ;
        }

        /** @var PromiseInterface<void> */
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
        return $this->ctx->connection !== null ? pg_get_pid($this->assertConnection()) : 0;
    }

    public function getTransactionStatus(): int
    {
        return $this->ctx->connection !== null
            ? pg_transaction_status($this->assertConnection())
            : PGSQL_TRANSACTION_UNKNOWN;
    }

    /**
     * Returns true if a query was cancelled via pg_cancel_backend on this connection.
     *
     * When true, the connection pool MUST NOT return this connection to the
     * ready pool until finishCommand() has fired and state returns to READY.
     * Unlike MySQL's KILL QUERY, no scrub query is needed — the ErrorResponse
     * from the server is consumed automatically by QueryResultHandler.
     */
    public function wasQueryCancelled(): bool
    {
        return $this->wasQueryCancelled;
    }

    /**
     * Clears the cancelled flag once the pool has confirmed the connection
     * has returned to READY state after the cancel drain cycle.
     */
    public function clearCancelledFlag(): void
    {
        $this->wasQueryCancelled = false;
    }

    /**
     * Closes the connection and releases all resources.
     *
     * If the connection is actively querying and server-side cancellation is
     * enabled, a pg_cancel_backend is dispatched on a side-channel BEFORE
     * state is set to CLOSED. This prevents the server-side query from holding
     * row or table locks after the client disconnects.
     *
     * The snapshot of $pendingCancels is taken AFTER state is set to CLOSED
     * and AFTER the conditional dispatch above. At that point no new cancels
     * can be added:
     *   - handleCommandCancellation() guards with state !== CLOSED
     *   - close() itself only dispatches in the block above
     * The two code paths are therefore mutually exclusive at snapshot time.
     *
     * Teardown is always guaranteed to run: each pending cancel is wrapped in
     * a hard timeout via killTimeoutSeconds, so a slow or unreachable
     * side-channel can never block shutdown indefinitely.
     */
    public function close(): void
    {
        if ($this->ctx->state === ConnectionState::CLOSED) {
            return;
        }

        // Fail-safe: if closing a busy connection with cancellation enabled,
        // dispatch pg_cancel_backend so the server-side query is interrupted.
        // dispatchCancelBackend() is a no-op if a cancel is already in-flight
        // for this PID (e.g. from a prior promise cancellation), so there is
        // no double-send and no risk of overwriting an existing pendingCancels
        // entry and orphaning it.
        $pid = $this->getProcessId();
        if (
            $this->ctx->state === ConnectionState::QUERYING
            && $pid > 0
            && $this->config->enableServerSideCancellation
        ) {
            $this->dispatchCancelBackend($pid);
        }

        $this->ctx->state = ConnectionState::CLOSED;

        // Snapshot pendingCancels AFTER state is set to CLOSED and AFTER the
        // conditional dispatch above. No new cancels can be added from this
        // point forward. Both branches of then() call teardown() so that a
        // rejected or timed-out cancel never silently skips cleanup.
        if ($this->pendingCancels !== []) {
            $this->awaitPendingCancels()->then(
                $this->teardown(...),
                $this->teardown(...)
            );

            return;
        }

        $this->teardown();
    }

    public function __destruct()
    {
        $this->close();
    }

    public function onConnectReady(): void
    {
        $promise = $this->ctx->connectPromise;
        $this->ctx->connectPromise = null;
        $promise?->resolve($this);
        $this->processNextCommand();
    }

    public function onConnectFailed(string $errorMsg): void
    {
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

        if ($cmd === null) {
            $this->processNextCommand();

            return;
        }

        if ($error !== null) {
            $isStream = $cmd->type === CommandRequest::TYPE_STREAM
                || $cmd->type === CommandRequest::TYPE_EXECUTE_STREAM;

            if ($isStream && $cmd->context instanceof RowStream) {
                // Only forward the error to the stream if the stream itself was not
                // already cancelled. A cancelled stream has already set its own error
                // state; pushing the server's QueryException over it would corrupt
                // the CancelledException the iterator is about to throw.
                if (! $cmd->context->isCancelled()) {
                    $cmd->context->error($error);
                }
            }

            // If the promise was already cancelled (e.g. the user called cancel()
            // and pg_cancel_backend raced ahead), the QueryException arriving from
            // the ErrorResponse is the expected server-side acknowledgement of that
            // cancel attempt, not a new failure. Rejecting a cancelled promise would
            // overwrite CancelledException with QueryException, making await() throw
            // the wrong type. Guard here so the cancelled state always wins.
            if (! $cmd->promise->isCancelled()) {
                $cmd->promise->reject($error);
                $cmd->promise->catch(static function (): void {
                });
            }
        } else {
            if (! $cmd->promise->isCancelled()) {
                $cmd->promise->resolve($value);
            }
        }

        $this->processNextCommand();
    }

    public function processNextCommand(): void
    {
        if (
            $this->ctx->state !== ConnectionState::READY
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

        try {
            match ($cmd->type) {
                CommandRequest::TYPE_PING => $this->processPing(),
                CommandRequest::TYPE_PREPARE => $this->processPrepare($cmd),
                CommandRequest::TYPE_EXECUTE => $this->processExecute($cmd),
                CommandRequest::TYPE_EXECUTE_STREAM => $this->processExecuteStream($cmd),
                CommandRequest::TYPE_STREAM => $this->processStream($cmd),
                default => $this->processQuery($cmd),
            };
        } catch (Throwable $e) {
            // The pg_send_* helpers throw *before* returning, so no query
            // could have been dispatched at this point; nothing to cancel.
            $this->finishCommand($e);
        }
    }

    /**
     * @param array<int, mixed> $params
     *
     * @return PromiseInterface<mixed>
     */
    private function enqueueCommand(
        string $type,
        string $sql = '',
        array $params = [],
        mixed $context = null,
    ): PromiseInterface {
        if ($this->ctx->state === ConnectionState::CLOSED) {
            return Promise::rejected(new ConnectionException('Connection is closed'));
        }

        $promise = new Promise();
        $command = new CommandRequest($type, $promise, $sql, $params, $context);
        $this->ctx->commandQueue->enqueue($command);

        $promise->onCancel(function () use ($command): void {
            $this->handleCommandCancellation($command);
        });

        $this->processNextCommand();

        return $promise;
    }

    /**
     * Handles all edge cases when a command promise is cancelled.
     *
     * Case 1 — Command still in queue (not yet started):
     *   Just remove it. No server interaction needed.
     *
     * Case 2 — Command is currently executing, cancellation enabled:
     *   Dispatch pg_cancel_backend on a side-channel. The server will send an
     *   ErrorResponse on the main wire, which QueryResultHandler consumes and
     *   routes through finishCommand(), resetting state back to READY.
     *   The wasQueryCancelled flag is set so the pool can defer reuse until
     *   that drain cycle completes.
     *
     * Case 3 — Command is currently executing, cancellation disabled:
     *   PostgreSQL's wire protocol does not support interrupting a running
     *   query without a side-channel. Unlike MySQL, the connection cannot idle
     *   while the server finishes — the wire is blocked until the result
     *   arrives. Closing is the only safe option; the caller must reconnect.
     */
    private function handleCommandCancellation(CommandRequest $command): void
    {
        if ($this->removeFromQueue($command)) {
            return;
        }

        if ($this->ctx->currentCommand !== $command) {
            return;
        }

        $pid = $this->getProcessId();

        if ($this->config->enableServerSideCancellation && $pid > 0 && $this->ctx->state !== ConnectionState::CLOSED) {
            $this->wasQueryCancelled = true;
            $this->dispatchCancelBackend($pid);
        } elseif (! $this->config->enableServerSideCancellation && $this->ctx->state !== ConnectionState::CLOSED) {
            // Cannot cleanly interrupt the running query without a side-channel.
            // The only safe option is to close and let the caller reconnect.
            $this->close();
        }
    }

    /**
     * Opens a dedicated side-channel connection and sends pg_cancel_backend(<pid>).
     *
     * Key design properties:
     *
     *   1. IDEMPOTENT — if a cancel is already in-flight for this PID the
     *      method is a no-op. Sending a second one is redundant and would
     *      orphan the first promise, causing awaitPendingCancels() to never
     *      see it resolve. This also closes the double-dispatch race where
     *      both handleCommandCancellation() and close() independently detect
     *      a live query and call dispatchCancelBackend() for the same PID.
     *
     *   2. SYNCHRONOUS REGISTRATION — $cancelPromise is created and stored in
     *      $pendingCancels BEFORE nextTick() schedules the work. This closes
     *      the tick-boundary race where close() runs in the same tick as
     *      dispatchCancelBackend() but before the callback executes, making
     *      pendingCancels appear empty to close().
     *
     *   3. BOUNDED — the cancel work is wrapped in killTimeoutSeconds so a
     *      slow or unreachable side-channel never blocks teardown forever.
     *
     *   4. SAFE AFTER TEARDOWN — the callback captures only $this->config and
     *      $this->connector, both of which are readonly and never nulled by
     *      teardown(). The callback therefore remains safe to execute even if
     *      teardown() has already run on the parent connection.
     *
     *   5. NON-BLOCKING — nextTick() defers execution to the next event loop
     *      tick, preventing the current call stack (especially destructors or
     *      close()) from blocking while the side-channel connects and queries.
     */
    private function dispatchCancelBackend(int $pid): void
    {
        // Idempotency guard — a cancel is already in-flight for this PID.
        // Sending a second one is redundant and would orphan the first promise,
        // causing awaitPendingCancels() to never see it resolve.
        if (isset($this->pendingCancels[$pid])) {
            return;
        }

        // Registered synchronously so close() always sees a non-empty
        // pendingCancels map regardless of when it runs relative to the callback.
        $cancelPromise = new Promise();
        $this->pendingCancels[$pid] = $cancelPromise;

        Loop::nextTick(function () use ($pid, $cancelPromise): void {
            // Both resolve and reject paths must settle $cancelPromise and clean
            // up the pendingCancels entry and allSettled() in awaitPendingCancels()
            // depends on every entry eventually reaching a terminal state.
            $settle = function () use ($cancelPromise, $pid): void {
                $cancelPromise->resolve(null);
                unset($this->pendingCancels[$pid]);
            };

            Promise::timeout(
                Connection::create($this->config)
                    ->then(function (Connection $killConn) use ($pid): PromiseInterface {
                        return $killConn->query("SELECT pg_cancel_backend({$pid})")
                            ->finally(fn () => $killConn->close())
                        ;
                    }),
                $this->config->killTimeoutSeconds
            )->then($settle, $settle);
        });
    }

    /**
     * Returns a promise that resolves once every in-flight pg_cancel_backend
     * promise has reached a terminal state (fulfilled, rejected, or timed out).
     *
     * Uses Promise::allSettled() rather than Promise::all() so that a failed
     * or timed-out cancel never short-circuits teardown — we always want every
     * cancel to reach a terminal state before we proceed.
     *
     * The snapshot of $pendingCancels taken here is stable: by the time this
     * method is called from close(), state is already CLOSED and no new cancels
     * can be dispatched (handleCommandCancellation guards with state !== CLOSED,
     * and close() only dispatches before setting the state flag).
     *
     * @return PromiseInterface<void>
     */
    private function awaitPendingCancels(): PromiseInterface
    {
        if ($this->pendingCancels === []) {
            /** @var PromiseInterface<void> */
            return Promise::resolved();
        }

        /** @var PromiseInterface<void> */
        return Promise::allSettled($this->pendingCancels)
            ->then(function (): void {
                $this->pendingCancels = [];
            })
        ;
    }

    /**
     * Performs the actual resource release after all pending cancels have settled.
     *
     * Extracted from close() so it can be invoked either immediately (when no
     * cancels are in-flight) or deferred (after awaitPendingCancels() resolves).
     * Keeping teardown separate ensures the two code paths stay in sync and
     * cannot diverge over time.
     */
    private function teardown(): void
    {
        $this->clearAllWatchers();

        if ($this->ctx->connection !== null) {
            @pg_close($this->assertConnection());
            $this->ctx->connection = null;
        }

        $exception = new ConnectionException('Connection was closed');

        if ($this->ctx->connectPromise !== null) {
            $this->ctx->connectPromise->reject($exception);
            // Suppress unhandled rejection — the caller may have already dropped
            // the connect promise reference by the time teardown runs.
            $this->ctx->connectPromise->catch(static function (): void {
            });
            $this->ctx->connectPromise = null;
        }

        if ($this->ctx->currentCommand !== null) {
            $cmd = $this->ctx->currentCommand;
            $this->ctx->currentCommand = null;
            $cmd->promise->reject($exception);
            $cmd->promise->catch(static function (): void {
            });
        }

        while (! $this->ctx->commandQueue->isEmpty()) {
            $cmd = $this->ctx->commandQueue->dequeue();
            $cmd->promise->reject($exception);
            $cmd->promise->catch(static function (): void {
            });
        }
    }

    private function removeFromQueue(CommandRequest $command): bool
    {
        $found = false;
        /** @var SplQueue<CommandRequest> $temp */
        $temp = new SplQueue();
        while (! $this->ctx->commandQueue->isEmpty()) {
            $cmd = $this->ctx->commandQueue->dequeue();
            if ($cmd === $command) {
                $found = true;
            } else {
                $temp->enqueue($cmd);
            }
        }
        $this->ctx->commandQueue = $temp;

        return $found;
    }

    /**
     * @param array<string|int, mixed> $params
     *
     * @return array<int, mixed>
     */
    private function resolveStatementParams(PreparedStatement $stmt, array $params): array
    {
        if ($params !== [] && \is_string(array_key_first($params))) {
            /** @var array<string, mixed> $params */
            return ParamParser::resolveNamed($stmt->paramNames, $params);
        }

        return array_values($params);
    }

    private function processPing(): void
    {
        $healthy = pg_connection_status($this->assertConnection()) === PGSQL_CONNECTION_OK;
        $this->finishCommand(
            $healthy ? null : new ConnectionException('Ping failed. Connection is unhealthy.'),
            $healthy ? true : null,
        );
    }

    private function processPrepare(CommandRequest $cmd): void
    {
        $sent = @pg_send_prepare($this->assertConnection(), 'stmt_' . $this->stmtCounter, $cmd->sql);
        if ($sent === false) {
            throw new QueryException('Failed to send PREPARE: ' . pg_last_error($this->assertConnection()));
        }
        $this->addQueryReadWatcher();
    }

    private function processExecute(CommandRequest $cmd): void
    {
        $sent = @pg_send_execute($this->assertConnection(), $cmd->sql, $this->normalizeParams($cmd->params));
        if ($sent === false) {
            throw new QueryException('Failed to send EXECUTE: ' . pg_last_error($this->assertConnection()));
        }
        $this->addQueryReadWatcher();
    }

    private function processExecuteStream(CommandRequest $cmd): void
    {
        /** @var RowStream $stream */
        $stream = $cmd->context;
        $stream->setResumeCallback($this->streamHandler->resume(...));

        $sent = @pg_send_execute($this->assertConnection(), $cmd->sql, $this->normalizeParams($cmd->params));
        if ($sent === false) {
            throw new QueryException('Failed to send EXECUTE (stream): ' . pg_last_error($this->assertConnection()));
        }

        if (function_exists('pg_set_chunked_rows_size')) {
            @pg_set_chunked_rows_size($this->assertConnection(), $stream->bufferSize);
        }

        $this->addQueryReadWatcher();
    }

    private function processStream(CommandRequest $cmd): void
    {
        /** @var RowStream $stream */
        $stream = $cmd->context;
        $stream->setResumeCallback($this->streamHandler->resume(...));

        if (! function_exists('pg_set_chunked_rows_size')) {
            // Fallback to server-side cursors for PHP versions where chunked mode is unavailable.
            $this->cursorHandler->init($cmd->sql, []);

            return;
        }

        $sent = @pg_send_query($this->assertConnection(), $cmd->sql);

        if ($sent === false) {
            throw new QueryException('Failed to send query: ' . pg_last_error($this->assertConnection()));
        }

        @pg_set_chunked_rows_size($this->assertConnection(), $stream->bufferSize);
        $this->addQueryReadWatcher();
    }

    private function processQuery(CommandRequest $cmd): void
    {
        $sent = @pg_send_query($this->assertConnection(), $cmd->sql);

        if ($sent === false) {
            throw new QueryException('Failed to send query: ' . pg_last_error($this->assertConnection()));
        }

        $this->addQueryReadWatcher();
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

    /**
     * @param array<int, mixed> $params
     *
     * @return array<int, mixed>
     */
    private function normalizeParams(array $params): array
    {
        return array_map(
            static fn (mixed $p) => \is_bool($p) ? ($p ? '1' : '0') : $p,
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

    /**
     * Narrows the connection resource to the concrete PgSql\Connection type
     * that modern pg_* functions require.
     *
     * Must only be called when $this->ctx->connection is known to be non-null
     * (i.e. after a successful pg_connect and before teardown).
     */
    private function assertConnection(): \PgSql\Connection
    {
        assert($this->ctx->connection instanceof \PgSql\Connection);

        return $this->ctx->connection;
    }
}
