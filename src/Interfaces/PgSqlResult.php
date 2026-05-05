<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

use Hibla\Sql\MultiResult;

/**
 * Extends the core Result interface with PostgreSQL-specific metadata.
 */
interface PgSqlResult extends MultiResult
{
    /**
     * The connection process ID (PID) associated with the executed query.
     */
    public int $connectionId { get; }

    /**
     * Returns the OID of the inserted row (if applicable and using OIDs).
     */
    public ?int $insertedOid { get; }

    /**
     * Returns the next result in the multi-result set or .
     */
    public function nextResult(): ?self;
}
