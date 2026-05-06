<?php

declare(strict_types=1);

namespace Hibla\Postgres\Traits;

use PgSql\Connection as PgSqlConnection;

/**
 * @internal
 *
 * @property-read \Hibla\Postgres\Internals\ConnectionContext $ctx
 */
trait HandlerHelperTrait
{
    /**
     * Narrows $this->ctx->connection from PgSql\Connection|resource to PgSql\Connection.
     * Safe to call whenever connection is known to be non-null.
     */
    private function getTypedConnection(): PgSqlConnection
    {
        assert($this->ctx->connection instanceof PgSqlConnection);

        return $this->ctx->connection;
    }

    /**
     * Narrows pg_fetch_assoc() keys from int|string to string.
     * PostgreSQL column names are always strings; the int possibility
     * is a PHPStan stub artifact.
     *
     * @param array<int|string, string|null> $row
     *
     * @return array<string, string|null>
     */
    private function normalizeRow(array $row): array
    {
        /** @var array<string, string|null> */
        return array_combine(
            array_map(\strval(...), array_keys($row)),
            array_values($row)
        );
    }
}
