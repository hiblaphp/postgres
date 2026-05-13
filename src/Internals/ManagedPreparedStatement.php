<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\RowStream as SqlRowStream;

/**
 * A wrapper around PreparedStatement that manages connection lifecycle.
 *
 * This class automatically releases the connection back to the pool when
 * the statement is closed or goes out of scope.
 *
 * This must not be instantiated directly.
 */
final class ManagedPreparedStatement implements PreparedStatementInterface
{
    private bool $isReleased = false;

    /**
     * @param PreparedStatementInterface $statement The underlying prepared statement
     * @param Connection $connection The connection this statement belongs to
     * @param PoolManager $pool The pool to release the connection back to
     */
    public function __construct(
        private readonly PreparedStatementInterface $statement,
        private readonly Connection $connection,
        private readonly PoolManager $pool
    ) {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresResult>
     */
    public function execute(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<PostgresResult> $promise */
        $promise = $this->statement->execute($params);

        return Promise::propagateCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<SqlRowStream>
     */
    public function executeStream(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<SqlRowStream> $promise */
        $promise = $this->statement->executeStream($params);

        return Promise::propagateCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * Closes the underlying prepared statement and releases the connection
     * back to the pool.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        return $this->statement->close()
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * Releases the connection back to the pool.
     */
    private function releaseConnection(): void
    {
        if ($this->isReleased) {
            return;
        }

        $this->isReleased = true;
        $this->pool->release($this->connection);
    }

    public function __destruct()
    {
        if (! $this->isReleased && ! $this->connection->isClosed()) {
            $this->connection->close();
        }

        $this->releaseConnection();
    }
}
