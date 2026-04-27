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
 *
 *
 *
 * CANCELLATION BEHAVIOUR (ext-pgsql limitation)
 * -----------------------------------------------
 * Due to a known limitation in PHP's ext-pgsql extension, there is no true
 * client-side-only cancellation. When a query is in progress and the connection
 * is closed, PHP's internal _close_pgsql_link() calls PQgetResult() in a loop
 * before calling PQfinish(), which blocks until the server finishes the query.
 *
 * The only way to unblock this is to send a cancel signal to the server first,
 * which pg_cancel_query() does via a separate short-lived TCP connection.
 *
 * This flag controls what happens AFTER cancellation, not whether a server
 * signal is sent:
 *
 *   true  → Cancel signal is sent and the connection is kept alive for reuse.
 *            Use this in connection pool scenarios where you want to recycle
 *            the connection after cancellation.
 *
 *   false → Cancel signal is still sent (unavoidable), but the connection is
 *           closed and discarded afterwards. Use this for single-use connections
 *           or when you do not want to reuse the connection after cancellation.
 *
 * If you need true client-side cancellation with no server interaction, consider
 * using pecl-pq instead of ext-pgsql, or set a session-level statement_timeout
 * on connect to bound query execution time on the server side.
 *
 * Reference: https://bugs.php.net/bug.php?id=79134
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
     * Names are local to this connection, so a simple integer is sufficient.
     */
    private int $stmtCounter = 0;

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
     * @return PromiseInterface<self>
     */
    public static function create(PgSqlConfig|array|string $config): PromiseInterface
    {
        return new self($config)->connect();
    }

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

    /**
     * Executes a plain SQL query and returns the full buffered result.
     * Do NOT pass user-supplied values directly — use prepare() + executeStatement() instead.
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlResult>
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, $sql);
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
     * Converts `?` placeholders to the PostgreSQL `$1, $2, …` format automatically.
     *
     * @return PromiseInterface<PreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $name = 'stmt_' . (++$this->stmtCounter);

        [$parsedSql] = ParamParser::parsePlaceholders($sql);

        // Pass a factory closure so QueryResultHandler can construct the
        // PreparedStatement after the server acknowledges the PREPARE without
        // needing a direct dependency on this class.
        $connection = $this;
        $factory = static fn () => new PreparedStatement($connection, $name);

        return $this->enqueueCommand(CommandRequest::TYPE_PREPARE, $parsedSql, [], $factory);
    }

    /**
     * Executes a prepared statement and returns the full buffered result.
     *
     * @param array<int, mixed> $params
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlResult>
     */
    public function executeStatement(PreparedStatement $stmt, array $params): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_EXECUTE, $stmt->name, $params);
    }

    /**
     * Executes a prepared statement and streams the result row-by-row.
     *
     * @param array<int, mixed> $params
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PgSqlRowStream>
     */
    public function executeStatementStream(PreparedStatement $stmt, array $params, int $bufferSize = 100): PromiseInterface
    {
        $stream = new RowStream($bufferSize);
        $commandPromise = $this->enqueueCommand(CommandRequest::TYPE_EXECUTE_STREAM, $stmt->name, $params, $stream);
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
        // Statement names are generated by us (alphanumeric + underscore) so
        // embedding directly is safe. Avoid pg_escape_identifier here
        // because it requires a live connection resource.
        return $this->enqueueCommand(CommandRequest::TYPE_QUERY, "DEALLOCATE {$name}");
    }

    /**
     * @return PromiseInterface<bool>
     */
    public function ping(): PromiseInterface
    {
        return $this->enqueueCommand(CommandRequest::TYPE_PING);
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

    public function onConnectReady(): void
    {
        $promise = $this->ctx->connectPromise;
        $this->ctx->connectPromise = null;
        $promise?->resolve($this);
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
            $isStream = $cmd?->type === CommandRequest::TYPE_STREAM
                || $cmd?->type === CommandRequest::TYPE_EXECUTE_STREAM;

            if ($isStream && $cmd->context !== null) {
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
        $querySent = false;

        try {
            match ($cmd->type) {
                CommandRequest::TYPE_PING => $this->processPing(),
                CommandRequest::TYPE_PREPARE => $querySent = $this->processPrepare($cmd),
                CommandRequest::TYPE_EXECUTE => $querySent = $this->processExecute($cmd),
                CommandRequest::TYPE_EXECUTE_STREAM => $querySent = $this->processExecuteStream($cmd),
                CommandRequest::TYPE_STREAM => $querySent = $this->processStream($cmd),
                default => $querySent = $this->processQuery($cmd),
            };
        } catch (Throwable $e) {
            if ($querySent && $this->ctx->connection !== null) {
                @pg_cancel_query($this->ctx->connection);
            }
            $this->finishCommand($e);
        }
    }

    private function processPing(): void
    {
        $healthy = pg_connection_status($this->ctx->connection) === PGSQL_CONNECTION_OK;
        $this->finishCommand(
            $healthy ? null : new ConnectionException('Ping failed. Connection is unhealthy.'),
            $healthy ? true : null,
        );
    }

    private function processPrepare(CommandRequest $cmd): bool
    {
        $sent = @pg_send_prepare($this->ctx->connection, 'stmt_' . $this->stmtCounter, $cmd->sql);

        if (! $sent) {
            throw new QueryException('Failed to send PREPARE: ' . pg_last_error($this->ctx->connection));
        }

        $this->addQueryReadWatcher();

        return true;
    }

    private function processExecute(CommandRequest $cmd): bool
    {
        $sent = @pg_send_execute($this->ctx->connection, $cmd->sql, $this->normalizeParams($cmd->params));

        if (! $sent) {
            throw new QueryException('Failed to send EXECUTE: ' . pg_last_error($this->ctx->connection));
        }

        $this->addQueryReadWatcher();

        return true;
    }

    private function processExecuteStream(CommandRequest $cmd): bool
    {
        /** @var RowStream $stream */
        $stream = $cmd->context;
        $stream->setResumeCallback($this->streamHandler->resume(...));

        $sent = @pg_send_execute($this->ctx->connection, $cmd->sql, $this->normalizeParams($cmd->params));

        if (! $sent) {
            throw new QueryException('Failed to send EXECUTE (stream): ' . pg_last_error($this->ctx->connection));
        }

        if (function_exists('pg_set_chunked_rows_size')) {
            @pg_set_chunked_rows_size($this->ctx->connection, $stream->bufferSize);
        }

        $this->addQueryReadWatcher();

        return true;
    }

    private function processStream(CommandRequest $cmd): bool
    {
        /** @var RowStream $stream */
        $stream = $cmd->context;
        $stream->setResumeCallback($this->streamHandler->resume(...));

        if (! function_exists('pg_set_chunked_rows_size')) {
            $this->cursorHandler->init($cmd->sql, []);

            return false;
        }

        $sent = @pg_send_query($this->ctx->connection, $cmd->sql);

        if (! $sent) {
            throw new QueryException('Failed to send query: ' . pg_last_error($this->ctx->connection));
        }

        @pg_set_chunked_rows_size($this->ctx->connection, $stream->bufferSize);
        $this->addQueryReadWatcher();

        return true;
    }

    private function processQuery(CommandRequest $cmd): bool
    {
        $sent = @pg_send_query($this->ctx->connection, $cmd->sql);

        if (! $sent) {
            throw new QueryException('Failed to send query: ' . pg_last_error($this->ctx->connection));
        }

        $this->addQueryReadWatcher();

        return true;
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
            if ($this->ctx->currentCommand === $command) {
                if ($this->config->enableServerSideCancellation && $this->ctx->connection !== null) {
                    @pg_cancel_query($this->ctx->connection);
                } else {
                    $this->close();
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
}
