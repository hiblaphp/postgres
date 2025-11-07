<?php

declare(strict_types=1);

namespace Hibla\Postgres\Utilities;

use Hibla\Postgres\Manager\TransactionManager;
use PgSql\Connection;

/**
 * Represents an active database transaction with scoped query methods.
 *
 * This class provides a clean API for executing queries within a transaction context.
 * All queries executed through this object are automatically part of the transaction.
 * All query methods return resolved values directly instead of promises.
 */
final class Transaction
{
    /**
     * Creates a new Transaction instance.
     *
     * @param Connection $connection The PostgreSQL connection for this transaction
     * @param QueryExecutor $queryExecutor The query executor instance
     * @param TransactionManager $transactionManager The transaction manager instance
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly QueryExecutor $queryExecutor,
        private readonly TransactionManager $transactionManager
    ) {
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return array<int, array<string, mixed>> Array of associative arrays
     */
    public function query(string $sql, array $params = []): array
    {
        /** @var array<int, array<string, mixed>> */
        return await($this->queryExecutor->executeQuery(
            $this->connection,
            $sql,
            $params,
            'fetchAll'
        ));
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return array<string, mixed>|null Associative array or null if no rows
     */
    public function fetchOne(string $sql, array $params = []): ?array
    {
        /** @var array<string, mixed>|null */
        return await($this->queryExecutor->executeQuery(
            $this->connection,
            $sql,
            $params,
            'fetchOne'
        ));
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return int Number of affected rows
     */
    public function execute(string $sql, array $params = []): int
    {
        /** @var int */
        return await($this->queryExecutor->executeQuery(
            $this->connection,
            $sql,
            $params,
            'execute'
        ));
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders ($1, $2, etc.)
     * @param  array<int, mixed>  $params  Parameter values for prepared statement
     * @return mixed Scalar value or null if no rows
     */
    public function fetchValue(string $sql, array $params = []): mixed
    {
        return await($this->queryExecutor->executeQuery(
            $this->connection,
            $sql,
            $params,
            'fetchValue'
        ));
    }

    /**
     * Registers a callback to execute when this transaction commits.
     *
     * The callback will be executed after the transaction successfully commits
     * but before the transaction() method returns.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     */
    public function onCommit(callable $callback): void
    {
        $this->transactionManager->onCommit($callback);
    }

    /**
     * Registers a callback to execute when this transaction rolls back.
     *
     * The callback will be executed after the transaction is rolled back
     * but before the exception is re-thrown.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     */
    public function onRollback(callable $callback): void
    {
        $this->transactionManager->onRollback($callback);
    }

    /**
     * Gets the underlying PostgreSQL connection.
     *
     * Useful for advanced operations or raw connection access within the transaction.
     *
     * @return Connection The PostgreSQL connection
     */
    public function getConnection(): Connection
    {
        return $this->connection;
    }
}