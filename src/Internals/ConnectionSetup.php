<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\ConnectionSetup as ConnectionSetupInterface;
use Hibla\Postgres\Interfaces\PgSqlResult;
use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * @internal
 *
 * Wraps a raw Connection to expose only the query surface needed by
 * onConnect hooks. Prevents the internal Connection from leaking into
 * public API.
 */
final class ConnectionSetup implements ConnectionSetupInterface
{
    public function __construct(private readonly Connection $connection)
    {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PgSqlResult>
     */
    public function query(string $sql): PromiseInterface
    {
        return $this->connection->query($sql);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<int>
     */
    public function execute(string $sql): PromiseInterface
    {
        return $this->connection->query($sql)
            ->then(fn (PgSqlResult $result) => $result->affectedRows)
        ;
    }
}
