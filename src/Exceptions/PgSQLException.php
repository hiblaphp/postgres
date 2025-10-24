<?php

namespace Hibla\Postgres\Exception;

/**
 * Base exception for all PostgreSQL-related errors.
 *
 * This is the parent exception for all exceptions thrown by the
 * Hibla PostgreSQL library. Catch this to handle any database error.
 */
class PgSQLException extends \RuntimeException
{
}
