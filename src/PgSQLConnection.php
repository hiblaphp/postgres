<?php

namespace Hibla\Postgres;

use Hibla\Async\Timer;
use Hibla\Postgres\Exception\ConfigurationException;
use Hibla\Postgres\Exception\NotInitializedException;
use Hibla\Postgres\Exception\NotInTransactionException;
use Hibla\Postgres\Exception\QueryException;
use Hibla\Postgres\Exception\TransactionException;
use Hibla\Postgres\Exception\TransactionFailedException;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use PgSql\Connection;
use PgSql\Result;
use Throwable;
use WeakMap;

use function Hibla\async;
use function Hibla\await;

/**
 * Instance-based Asynchronous PostgreSQL API for independent database connections.
 *
 * This class provides non-static methods for managing a single connection pool.
 * Each instance is completely independent, allowing true multi-database support
 * without global state.
 */
final class PgSQLConnection
{
    /** @var PoolManager|null Connection pool instance for this connection */
    private ?PoolManager $pool = null;

    /** @var bool Tracks initialization state of this instance */
    private bool $isInitialized = false;

    /** @var WeakMap<Connection, array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null}>|null Transaction callbacks using WeakMap */
    private ?WeakMap $transactionCallbacks = null;

    /**
     * Creates a new independent PgSQLConnection instance.
     *
     * Each instance manages its own connection pool and is completely
     * independent from other instances, allowing true multi-database support.
     *
     * @param  array<string, mixed>  $dbConfig  Database configuration array containing:
     *                                          - host: Database host (e.g., 'localhost')
     *                                          - port: Database port (default: 5432)
     *                                          - dbname: Database name
     *                                          - user: Database username
     *                                          - password: Database password
     *                                          - options: Additional connection options (optional)
     * @param  int  $poolSize  Maximum number of connections in the pool
     *
     * @throws ConfigurationException If configuration is invalid
     */
    public function __construct(array $dbConfig, int $poolSize = 10)
    {
        try {
            $this->pool = new PoolManager($dbConfig, $poolSize);
            $this->transactionCallbacks = new WeakMap();
            $this->isInitialized = true;
        } catch (\InvalidArgumentException $e) {
            throw new ConfigurationException(
                'Invalid database configuration: ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * Resets this instance, closing all connections and clearing state.
     * After reset, this instance cannot be used until recreated.
     *
     * @return void
     */
    public function reset(): void
    {
        if ($this->pool !== null) {
            $this->pool->close();
        }
        $this->pool = null;
        $this->isInitialized = false;
        $this->transactionCallbacks = null;
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * This method can only be called from within an active transaction.
     * The callback will be executed after the transaction successfully commits
     * but before the transaction() method returns.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     *
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public function onCommit(callable $callback): void
    {
        $connection = $this->getCurrentTransactionConnection();

        if ($connection === null) {
            throw new NotInTransactionException(
                'onCommit() can only be called within a transaction.'
            );
        }

        $this->ensureTransactionCallbacksInitialized();

        if (!isset($this->transactionCallbacks[$connection])) {
            throw new TransactionException('Transaction state not found.');
        }

        $transactionData = $this->transactionCallbacks[$connection];
        $commitCallbacks = $transactionData['commit'];
        $commitCallbacks[] = $callback;

        $this->transactionCallbacks[$connection] = [
            'commit' => $commitCallbacks,
            'rollback' => $transactionData['rollback'],
            'fiber' => $transactionData['fiber'],
        ];
    }

    /**
     * Registers a callback to execute when the current transaction rolls back.
     *
     * This method can only be called from within an active transaction.
     * The callback will be executed after the transaction is rolled back
     * but before the exception is re-thrown.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     *
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public function onRollback(callable $callback): void
    {
        $connection = $this->getCurrentTransactionConnection();

        if ($connection === null) {
            throw new NotInTransactionException(
                'onRollback() can only be called within a transaction.'
            );
        }

        $this->ensureTransactionCallbacksInitialized();

        if (!isset($this->transactionCallbacks[$connection])) {
            throw new TransactionException('Transaction state not found.');
        }

        $transactionData = $this->transactionCallbacks[$connection];
        $rollbackCallbacks = $transactionData['rollback'];
        $rollbackCallbacks[] = $callback;

        $this->transactionCallbacks[$connection] = [
            'commit' => $transactionData['commit'],
            'rollback' => $rollbackCallbacks,
            'fiber' => $transactionData['fiber'],
        ];
    }

    /**
     * Executes a callback with a connection from this instance's pool.
     *
     * Automatically handles connection acquisition and release. The callback
     * receives a Connection instance and can perform any database operations.
     * The connection is guaranteed to be released back to the pool even if
     * the callback throws an exception.
     *
     * @template TResult
     *
     * @param  callable(Connection): TResult  $callback  Function that receives Connection instance
     * @return PromiseInterface<TResult> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function run(callable $callback): PromiseInterface
    {
        return async(function () use ($callback): mixed {
            $connection = null;

            try {
                $connection = await($this->getPool()->get());
                return $callback($connection);
            } finally {
                if ($connection !== null) {
                    $this->getPool()->release($connection);
                }
            }
        });
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * The query is executed asynchronously using PostgreSQL's non-blocking API.
     * Parameters are safely bound using prepared statements to prevent SQL injection.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>> Promise resolving to array of associative arrays
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<array<int, array<string, mixed>>> */
        return $this->executeAsyncQuery($sql, $params, 'fetchAll');
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * The query is executed asynchronously using PostgreSQL's non-blocking API.
     * Returns null if no rows match the query.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<string, mixed>|null> Promise resolving to associative array or null if no rows
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<array<string, mixed>|null> */
        return $this->executeAsyncQuery($sql, $params, 'fetchOne');
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * The statement is executed asynchronously using PostgreSQL's non-blocking API.
     * Returns the number of rows affected by the operation.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<int> Promise resolving to number of affected rows
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If statement execution fails
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<int> */
        return $this->executeAsyncQuery($sql, $params, 'execute');
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * Useful for queries that return a single scalar value like COUNT, MAX, etc.
     * Returns null if the query returns no rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<mixed> Promise resolving to scalar value or null if no rows
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    public function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return $this->executeAsyncQuery($sql, $params, 'fetchValue');
    }

    /**
     * Executes multiple operations within a database transaction.
     *
     * Automatically handles transaction begin/commit/rollback. If the callback
     * throws an exception, the transaction is rolled back and retried based on
     * the specified number of attempts. All retry attempts are made with exponential
     * backoff between attempts.
     *
     * Registered onCommit() callbacks are executed after successful commit.
     * Registered onRollback() callbacks are executed after rollback.
     *
     * @param  callable(Connection): mixed  $callback  Transaction callback receiving Connection instance
     * @param  int  $attempts  Number of times to attempt the transaction (default: 1)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws TransactionFailedException If transaction fails after all attempts
     * @throws \InvalidArgumentException If attempts is less than 1
     */
    public function transaction(callable $callback, int $attempts = 1): PromiseInterface
    {
        return async(function () use ($callback, $attempts) {
            if ($attempts < 1) {
                throw new \InvalidArgumentException('Transaction attempts must be at least 1.');
            }

            /** @var Throwable|null $lastException */
            $lastException = null;

            for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
                try {
                    return await($this->run(function (Connection $connection) use ($callback) {
                        $currentFiber = \Fiber::getCurrent();

                        $this->ensureTransactionCallbacksInitialized();

                        /** @var array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null} $initialState */
                        $initialState = [
                            'commit' => [],
                            'rollback' => [],
                            'fiber' => $currentFiber,
                        ];

                        if ($this->transactionCallbacks !== null) {
                            $this->transactionCallbacks[$connection] = $initialState;
                        }

                        $beginResult = @pg_query($connection, 'BEGIN');
                        if ($beginResult === false) {
                            throw new TransactionException(
                                'Failed to begin transaction: ' . pg_last_error($connection)
                            );
                        }

                        try {
                            $result = $callback($connection);

                            $commitResult = @pg_query($connection, 'COMMIT');
                            if ($commitResult === false) {
                                throw new TransactionException(
                                    'Failed to commit transaction: ' . pg_last_error($connection)
                                );
                            }

                            $this->executeCallbacks($connection, 'commit');

                            return $result;
                        } catch (Throwable $e) {
                            @pg_query($connection, 'ROLLBACK');

                            $this->executeCallbacks($connection, 'rollback');

                            throw $e;
                        } finally {
                            if ($this->transactionCallbacks !== null && isset($this->transactionCallbacks[$connection])) {
                                unset($this->transactionCallbacks[$connection]);
                            }
                        }
                    }));
                } catch (Throwable $e) {
                    $lastException = $e;

                    if ($currentAttempt < $attempts) {
                        continue;
                    }

                    throw new TransactionFailedException(
                        sprintf(
                            'Transaction failed after %d attempt(s): %s',
                            $attempts,
                            $e->getMessage()
                        ),
                        $attempts,
                        $e
                    );
                }
            }

            if ($lastException !== null) {
                throw new TransactionFailedException(
                    sprintf('Transaction failed after %d attempt(s)', $attempts),
                    $attempts,
                    $lastException
                );
            }

            throw new TransactionException('Transaction failed without exception.');
        });
    }

    /**
     * Gets statistics about this instance's connection pool.
     *
     * Returns information about the current state of the connection pool,
     * including total connections, available connections, and connections in use.
     *
     * @return array<string, int|bool> Pool statistics including:
     *                                  - total: Total number of connections in pool
     *                                  - available: Number of available connections
     *                                  - inUse: Number of connections currently in use
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getStats(): array
    {
        /** @var array<string, int|bool> */
        return $this->getPool()->getStats();
    }

    /**
     * Gets the most recently used connection from this pool.
     *
     * This is primarily useful for debugging and testing purposes.
     * Returns null if no connection has been used yet.
     *
     * @return Connection|null The last connection or null if none used yet
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getLastConnection(): ?Connection
    {
        return $this->getPool()->getLastConnection();
    }

    /**
     * Executes an async query with the specified result processing type.
     *
     * This method handles the complete lifecycle of query execution including
     * connection acquisition, query sending, result waiting, and connection release.
     *
     * @param  string  $sql  SQL query/statement
     * @param  array<int, mixed>  $params  Query parameters
     * @param  string  $resultType  Type of result processing ('fetchAll', 'fetchOne', 'execute', 'fetchValue')
     * @return PromiseInterface<mixed> Promise resolving to processed result
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     *
     * @internal This method is for internal use only
     */
    private function executeAsyncQuery(string $sql, array $params, string $resultType): PromiseInterface
    {
        return async(function () use ($sql, $params, $resultType) {
            $connection = await($this->getPool()->get());

            try {
                if (count($params) > 0) {
                    $sendResult = @pg_send_query_params($connection, $sql, $params);
                    if ($sendResult === false) {
                        throw new QueryException(
                            'Failed to send parameterized query: ' . pg_last_error($connection),
                            $sql,
                            $params
                        );
                    }
                } else {
                    $sendResult = @pg_send_query($connection, $sql);
                    if ($sendResult === false) {
                        throw new QueryException(
                            'Failed to send query: ' . pg_last_error($connection),
                            $sql,
                            $params
                        );
                    }
                }

                $result = await($this->waitForAsyncCompletion($connection));

                return $this->processResult($result, $resultType, $connection, $sql, $params);
            } finally {
                $this->getPool()->release($connection);
            }
        });
    }

    /**
     * Waits for an async query to complete using non-blocking polling.
     *
     * This method polls the connection status at increasing intervals
     * (exponential backoff) until the query completes. This provides
     * efficient non-blocking behavior without busy-waiting.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @return PromiseInterface<Result|false> Promise resolving to query result
     *
     * @internal This method is for internal use only
     */
    private function waitForAsyncCompletion(Connection $connection): PromiseInterface
    {
        return async(function () use ($connection) {
            $pollInterval = 100; // microseconds
            $maxInterval = 1000;

            while (pg_connection_busy($connection)) {
                await(Timer::delay($pollInterval / 1000000));
                $pollInterval = (int) min($pollInterval * 1.2, $maxInterval);
            }

            return pg_get_result($connection);
        });
    }

    /**
     * Processes a query result based on the specified result type.
     *
     * This method converts the raw PostgreSQL result into the appropriate
     * PHP data structure based on the requested result type.
     *
     * @param  Result|false  $result  PostgreSQL query result
     * @param  string  $resultType  Type of result processing
     * @param  Connection  $connection  PostgreSQL connection for error reporting
     * @param  string  $sql  The SQL query for error context
     * @param  array<int, mixed>  $params  The query parameters for error context
     * @return mixed Processed result based on result type
     *
     * @throws QueryException If result is false or processing fails
     *
     * @internal This method is for internal use only
     */
    private function processResult(
        Result|false $result,
        string $resultType,
        Connection $connection,
        string $sql,
        array $params
    ): mixed {
        if ($result === false) {
            $error = pg_last_error($connection);
            throw new QueryException(
                'Query execution failed: ' . ($error !== '' ? $error : 'Unknown error'),
                $sql,
                $params
            );
        }

        return match ($resultType) {
            'fetchAll' => $this->handleFetchAll($result),
            'fetchOne' => $this->handleFetchOne($result),
            'fetchValue' => $this->handleFetchValue($result),
            'execute' => $this->handleExecute($result),
            default => $result,
        };
    }

    /**
     * Fetches all rows from a query result.
     *
     * Converts the PostgreSQL result into an array of associative arrays,
     * where each array represents a row with column names as keys.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return array<int, array<string, mixed>> Array of associative arrays
     *
     * @internal This method is for internal use only
     */
    private function handleFetchAll(Result $result): array
    {
        $rows = pg_fetch_all($result);

        /** @var array<int, array<string, mixed>> $rows */
        return $rows;
    }

    /**
     * Fetches the first row from a query result.
     *
     * Converts the first row of the PostgreSQL result into an associative array
     * with column names as keys. Returns null if no rows exist.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return array<int|string, string|null> Associative array or null if no rows
     *
     * @internal This method is for internal use only
     */
    private function handleFetchOne(Result $result): ?array
    {
        $row = pg_fetch_assoc($result);

        if ($row === false) {
            return null;
        }

        return $row;
    }

    /**
     * Fetches a single column value from the first row.
     *
     * Extracts the first column of the first row from the result set.
     * Useful for aggregate queries like COUNT, SUM, MAX, etc.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return mixed Scalar value or null if no rows
     *
     * @internal This method is for internal use only
     */
    private function handleFetchValue(Result $result): mixed
    {
        $row = pg_fetch_row($result);

        if ($row === false) {
            return null;
        }

        return $row[0];
    }

    /**
     * Gets the number of affected rows from a query result.
     *
     * Returns the count of rows affected by an INSERT, UPDATE, or DELETE statement.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return int Number of affected rows
     *
     * @internal This method is for internal use only
     */
    private function handleExecute(Result $result): int
    {
        return pg_affected_rows($result);
    }

    /**
     * Gets the current transaction's Connection instance if in a transaction within the current fiber.
     *
     * This method checks if the current fiber is executing within a transaction context
     * and returns the associated connection if found.
     *
     * @return Connection|null Connection instance or null if not in transaction
     *
     * @internal This method is for internal use only
     */
    private function getCurrentTransactionConnection(): ?Connection
    {
        if ($this->transactionCallbacks === null) {
            return null;
        }

        $currentFiber = \Fiber::getCurrent();

        foreach ($this->transactionCallbacks as $connection => $data) {
            if ($data['fiber'] === $currentFiber) {
                return $connection;
            }
        }

        return null;
    }

    /**
     * Executes registered callbacks for commit or rollback.
     *
     * Runs all callbacks registered for the specified transaction event.
     * If any callback throws an exception, execution stops and the first
     * exception is re-thrown after all callbacks have been attempted.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @param  string  $type  'commit' or 'rollback'
     * @return void
     *
     * @throws TransactionException If any callback throws an exception
     *
     * @internal This method is for internal use only
     */
    private function executeCallbacks(Connection $connection, string $type): void
    {
        if ($this->transactionCallbacks === null || !isset($this->transactionCallbacks[$connection])) {
            return;
        }

        $transactionData = $this->transactionCallbacks[$connection];

        if ($type !== 'commit' && $type !== 'rollback') {
            return;
        }

        $callbacks = $transactionData[$type];

        /** @var list<Throwable> $exceptions */
        $exceptions = [];

        foreach ($callbacks as $callback) {
            try {
                $callback();
            } catch (Throwable $e) {
                $exceptions[] = $e;
            }
        }

        if (count($exceptions) > 0) {
            throw new TransactionException(
                sprintf(
                    'Transaction %s callback failed: %s',
                    $type,
                    $exceptions[0]->getMessage()
                ),
                0,
                $exceptions[0]
            );
        }
    }

    /**
     * Ensures the transaction callbacks WeakMap is initialized.
     *
     * This is a safety check to ensure the WeakMap exists before use.
     * In normal operation, it should always be initialized in the constructor.
     *
     * @return void
     *
     * @internal This method is for internal use only
     */
    private function ensureTransactionCallbacksInitialized(): void
    {
        if ($this->transactionCallbacks === null) {
            $this->transactionCallbacks = new WeakMap();
        }
    }

    /**
     * Gets the connection pool instance.
     *
     * @return PoolManager The initialized connection pool
     *
     * @throws NotInitializedException If this instance is not initialized
     *
     * @internal This method is for internal use only
     */
    private function getPool(): PoolManager
    {
        if (!$this->isInitialized || $this->pool === null) {
            throw new NotInitializedException(
                'PgSQLConnection instance has not been initialized or has been reset.'
            );
        }

        return $this->pool;
    }
}