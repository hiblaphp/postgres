<?php

namespace Hibla\Postgres\Exception;

/**
 * Thrown when unable to establish a database connection.
 *
 * This exception indicates that the connection to the PostgreSQL
 * database could not be established or has been lost.
 */
class ConnectionException extends PoolException
{
}
