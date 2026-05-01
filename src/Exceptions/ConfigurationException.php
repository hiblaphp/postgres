<?php

declare(strict_types=1);

namespace Hibla\Postgres\Exceptions;

use InvalidArgumentException;

/**
 * Exception thrown when database configuration is invalid.
 */
class ConfigurationException extends InvalidArgumentException
{
}
