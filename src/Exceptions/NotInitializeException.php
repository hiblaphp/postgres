<?php

namespace Hibla\Postgres\Exception;

/**
 * Thrown when a PgSQLConnection instance is used before initialization.
 *
 * This exception indicates that operations were attempted on a connection
 * instance that has been reset or was never properly initialized.
 */
class NotInitializedException extends PgSQLException
{
}
