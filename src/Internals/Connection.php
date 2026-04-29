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
     * @var array<int, Promise<mixed>>
     */
    private array $pendingCancels = [];

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
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlResult>
     */
    public function query(string $sql): PromiseInterface
    {
        /** @var PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlResult> $promise */
        $promise = $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);

        return $promise;
    }

    /**
     * Streams a plain SQL query row-by-row.
     * Do NOT pass user-supplied values directly — use prepare() + executeStatementStream() instead.
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlRowStream>
     */
    public function streamQuery(string $sql, int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);
        $commandPromise = $this->enqueueCommand(CommandRequest::TYPE_STREAM, $sql, [], $stream);
        $stream->bindCommandPromise($commandPromise);

        return Promise::resolved($stream);
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
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlResult>
     */
    public function executeStatement(PreparedStatement $stmt, array $params): PromiseInterface
    {
        /** @var PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlResult> $promise */
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
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlRowStream>
     */
    public function executeStatementStream(PreparedStatement $stmt, array $params, int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);
        $commandPromise = $this->enqueueCommand(
            CommandRequest::TYPE_EXECUTE_STREAM,
            $stmt->name,
            $this->resolveStatementParams($stmt, $params),
            $stream,
        );
        $stream->bindCommandPromise($commandPromise);

        return Promise::resolved($stream);
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

    public function close(): void
    {
        if ($this->ctx->state === ConnectionState::CLOSED) {
            return;
        }

        $pid = $this->getProcessId();
        if (
            $this->ctx->state === ConnectionState::QUERYING
            && $pid > 0
            && $this->config->enableServerSideCancellation
        ) {
            $this->dispatchCancelBackend($pid);
        }

        $this->ctx->state = ConnectionState::CLOSED;

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
                $cmd->context->error($error);
            }

            $cmd->promise->reject($error);
        } else {
            $cmd->promise->resolve($value);
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

    private function handleCommandCancellation(CommandRequest $command): void
    {
        if ($this->removeFromQueue($command)) {
            return;
        }

        if ($this->ctx->currentCommand === $command) {
            $pid = $this->getProcessId();
            if ($this->config->enableServerSideCancellation && $pid > 0 && $this->ctx->state !== ConnectionState::CLOSED) {
                $this->dispatchCancelBackend($pid);
            }
        }
    }

    private function dispatchCancelBackend(int $pid): void
    {
        if (isset($this->pendingCancels[$pid])) {
            return;
        }

        $cancelPromise = new Promise();
        $this->pendingCancels[$pid] = $cancelPromise;

        Loop::nextTick(function () use ($pid, $cancelPromise): void {
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
            $this->ctx->connectPromise = null;
        }

        if ($this->ctx->currentCommand !== null) {
            $this->ctx->currentCommand->promise->reject($exception);
            $this->ctx->currentCommand = null;
        }

        while (! $this->ctx->commandQueue->isEmpty()) {
            $this->ctx->commandQueue->dequeue()->promise->reject($exception);
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
