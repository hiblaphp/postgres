<?php

declare(strict_types=1);

namespace Hibla\Postgres\Exceptions;

/**
 * Base exception for all PostgreSQL-related errors.
 *
 * This is the parent exception for all exceptions thrown by the
 * Hibla PostgreSQL library. Catch this to handle any database error.
 */
class PgSQLException extends \RuntimeException
{
}
