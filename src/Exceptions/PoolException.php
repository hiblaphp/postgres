<?php

declare(strict_types=1);

namespace Hibla\Postgres\Exceptions;

/**
 * Base exception for connection pool-related errors.
 *
 * This exception is thrown when connection pool operations fail,
 * such as acquiring connections or managing the pool state.
 */
class PoolException extends PgSQLException
{
}
