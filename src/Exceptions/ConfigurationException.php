<?php

namespace Hibla\Postgres\Exceptions;

/**
 * Thrown when database configuration is invalid.
 *
 * This exception is thrown during initialization when the provided
 * configuration parameters are invalid or incomplete.
 */
class ConfigurationException extends PgSQLException
{
}
