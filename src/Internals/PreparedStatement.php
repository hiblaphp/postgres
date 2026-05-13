<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\PreparedException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\RowStream as SqlRowStream;

/**
 * @internal Represents a server-side prepared statement on one PostgreSQL connection.
 *
 * Instances are created exclusively by Connection::prepare() and must never
 * be constructed directly. The statement is automatically deallocated from the
 * server when close() is called or the object is garbage-collected.
 */
final class PreparedStatement implements PreparedStatementInterface
{
    private bool $isClosed = false;

    /**
     * @param Connection $connection The connection that owns this statement.
     * @param string $name The server-side statement name (e.g. "stmt_3").
     * @param list<string> $paramNames Ordered parameter names from the original SQL,
     *                                 empty when positional (?) placeholders were used.
     * @param string $parsedSql The parsed SQL statement, with placeholders replaced by actual values.
     */
    public function __construct(
        private readonly Connection $connection,
        public readonly string $name,
        public readonly array $paramNames = [],
        public readonly string $parsedSql = '',
    ) {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<\Hibla\Postgres\Interfaces\PostgresResult>
     */
    public function execute(array $params = []): PromiseInterface
    {
        if ($this->isClosed) {
            throw new PreparedException('Cannot execute a closed prepared statement.');
        }

        return $this->connection->executeStatement($this, $params);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<SqlRowStream>
     */
    public function executeStream(array $params = [], int $bufferSize = 100): PromiseInterface
    {
        if ($this->isClosed) {
            throw new PreparedException('Cannot execute a closed prepared statement.');
        }

        return $this->connection->executeStatementStream($this, $params, $bufferSize);
    }

    /**
     * {@inheritdoc}
     *
     * Sends DEALLOCATE to the server so it can free the statement handle.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::resolved();
        }

        $this->isClosed = true;

        return $this->connection->closeStatement($this->name);
    }

    public function __destruct()
    {
        if (! $this->isClosed && ! $this->connection->isClosed()) {
            $this->close();
        }
    }
}
