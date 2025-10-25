<?php

declare(strict_types=1);

namespace Hibla\Postgres\Exceptions;

/**
 * Thrown when unable to establish a database connection.
 *
 * This exception indicates that the connection to the PostgreSQL
 * database could not be established or has been lost.
 */
class ConnectionException extends PoolException
{
}
