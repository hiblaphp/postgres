<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

use Hibla\Sql\Result;

/**
 * Extends the core Result interface with PostgreSQL-specific metadata.
 */
interface PgSqlResult extends Result
{
    /**
     * The connection process ID (PID) associated with the executed query.
     */
    public int $connectionId { get; }

    /**
     * Returns the OID of the inserted row (if applicable and using OIDs).
     */
    public ?int $insertedOid { get; }
}