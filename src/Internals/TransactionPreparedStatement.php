<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Interfaces\PostgresRowStream;
use Hibla\Postgres\Traits\CancellationHelperTrait;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;

/**
 * A wrapper around PreparedStatement used strictly inside Transactions.
 *
 * This class automatically sends a close command to the server when the
 * statement is closed or goes out of scope (garbage collected), preventing
 * server-side memory leaks.
 *
 * Crucially, unlike ManagedPreparedStatement, this DOES NOT release the
 * underlying TCP connection back to the pool, as the Transaction still owns it.
 *
 * @internal
 */
class TransactionPreparedStatement implements PreparedStatementInterface
{
    use CancellationHelperTrait;

    private bool $isClosed = false;

    public function __construct(
        private readonly PreparedStatementInterface $statement,
        private readonly Connection $connection
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

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresRowStream>
     */
    public function executeStream(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<PostgresRowStream> $promise */
        $promise = $this->statement->executeStream($params);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::resolved();
        }

        $this->isClosed = true;

        return $this->statement->close();
    }

    /**
     * Destructor ensures the server-side statement is closed when the object
     * goes out of scope.
     */
    public function __destruct()
    {
        if (! $this->isClosed && ! $this->connection->isClosed()) {
            $this->close();
        }
    }
}
