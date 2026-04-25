<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

use Hibla\Sql\CancellableStreamInterface;
use Hibla\Sql\RowStream;

/**
 * Provides an asynchronous stream of rows for PostgreSQL.
 * Uses pg_set_single_row_mode() internally.
 */
interface PgSqlRowStream extends RowStream, CancellableStreamInterface
{
    /**
     * Cancels the stream and releases resources.
     * Triggers pg_cancel_backend() if the query is still actively streaming from the server.
     */
    public function cancel(): void;

    /**
     * Returns whether this stream has been cancelled.
     */
    public function isCancelled(): bool;
}
