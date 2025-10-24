<?php

namespace Hibla\Postgres;

use Hibla\Postgres\Exception\ConfigurationException;
use Hibla\Postgres\Exception\NotInitializedException;
use Hibla\Postgres\Exception\NotInTransactionException;
use Hibla\Postgres\Exception\QueryException;
use Hibla\Postgres\Exception\TransactionException;
use Hibla\Postgres\Exception\TransactionFailedException;
use Hibla\Promise\Interfaces\PromiseInterface;
use PgSql\Connection;

/**
 * Asynchronous PostgreSQL API providing fiber-based database operations.
 *
 * This class serves as a singleton facade over PgSQLConnection,
 * providing convenient static methods for common database tasks in
 * single-database applications.
 */
final class PgSQL
{
    /** @var PgSQLConnection|null Underlying connection instance */
    private static ?PgSQLConnection $instance = null;

    /** @var bool Tracks initialization state */
    private static bool $isInitialized = false;

    /**
     * Initializes the async PostgreSQL database system.
     *
     * This is the single point of configuration and must be called before
     * using any other PgSQL methods. Multiple calls are ignored.
     *
     * @param  array<string, mixed>  $dbConfig  Database configuration array containing:
     *                                          - host: Database host (e.g., 'localhost')
     *                                          - port: Database port (default: 5432)
     *                                          - dbname: Database name
     *                                          - user: Database username
     *                                          - password: Database password
     *                                          - options: Additional connection options (optional)
     * @param  int  $poolSize  Maximum number of connections in the pool
     * @return void
     *
     * @throws ConfigurationException If the provided configuration is invalid
     */
    public static function init(array $dbConfig, int $poolSize = 10): void
    {
        if (self::$isInitialized) {
            return;
        }

        self::$instance = new PgSQLConnection($dbConfig, $poolSize);
        self::$isInitialized = true;
    }

    /**
     * Resets the singleton instance for clean testing.
     *
     * Closes all database connections and clears the pool. Primarily used
     * in testing scenarios to ensure clean state between tests.
     *
     * @return void
     */
    public static function reset(): void
    {
        if (self::$instance !== null) {
            self::$instance->reset();
        }
        self::$instance = null;
        self::$isInitialized = false;
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public static function onCommit(callable $callback): void
    {
        self::getInstance()->onCommit($callback);
    }

    /**
     * Registers a callback to execute when the current transaction rolls back.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public static function onRollback(callable $callback): void
    {
        self::getInstance()->onRollback($callback);
    }

    /**
     * Executes a callback with an async PostgreSQL connection from the pool.
     *
     * Automatically handles connection acquisition and release. The callback
     * receives a Connection instance and can perform any database operations.
     *
     * @template TResult
     *
     * @param  callable(Connection): TResult  $callback  Function that receives Connection instance
     * @return PromiseInterface<TResult> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     */
    public static function run(callable $callback): PromiseInterface
    {
        return self::getInstance()->run($callback);
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>> Promise resolving to array of associative arrays
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws QueryException If query execution fails
     */
    public static function query(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->query($sql, $params);
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<string, mixed>|null> Promise resolving to associative array or null if no rows
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws QueryException If query execution fails
     */
    public static function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->fetchOne($sql, $params);
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<int> Promise resolving to number of affected rows
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws QueryException If statement execution fails
     */
    public static function execute(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->execute($sql, $params);
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * Useful for queries that return a single scalar value like COUNT, MAX, etc.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<mixed> Promise resolving to scalar value or null if no rows
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws QueryException If query execution fails
     */
    public static function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->fetchValue($sql, $params);
    }

    /**
     * Executes multiple operations within a database transaction.
     *
     * Automatically handles transaction begin/commit/rollback. If the callback
     * throws an exception, the transaction is rolled back automatically and
     * retried up to the specified number of attempts.
     *
     * @param  callable(Connection): mixed  $callback  Transaction callback receiving Connection instance
     * @param  int  $attempts  Number of times to attempt the transaction (default: 1)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     * @throws TransactionFailedException If transaction fails after all attempts
     * @throws \InvalidArgumentException If attempts is less than 1
     */
    public static function transaction(callable $callback, int $attempts = 1): PromiseInterface
    {
        return self::getInstance()->transaction($callback, $attempts);
    }

    /**
     * Gets statistics about the connection pool.
     *
     * @return array<string, int|bool> Pool statistics including:
     *                                  - total: Total number of connections in pool
     *                                  - available: Number of available connections
     *                                  - inUse: Number of connections currently in use
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     */
    public static function getStats(): array
    {
        return self::getInstance()->getStats();
    }

    /**
     * Gets the most recently used connection from the pool.
     *
     * @return Connection|null The last connection or null if none used yet
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     */
    public static function getLastConnection(): ?Connection
    {
        return self::getInstance()->getLastConnection();
    }

    /**
     * Gets the underlying PgSQLConnection instance.
     *
     * @return PgSQLConnection The initialized connection instance
     *
     * @throws NotInitializedException If PgSQL has not been initialized
     *
     * @internal This method is for internal use only
     */
    public static function getInstance(): PgSQLConnection
    {
        if (!self::$isInitialized || self::$instance === null) {
            throw new NotInitializedException(
                'PgSQL has not been initialized. Please call PgSQL::init() at application startup.'
            );
        }

        return self::$instance;
    }
}