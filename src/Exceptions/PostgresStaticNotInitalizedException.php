<?php

namespace Hibla\Exceptions;

use Exception;
use RuntimeException;

class PostgresNotInitalizedException extends RuntimeException
{
    public function __construct()
    {
        parent::__construct('PgSQL has not been initialized. Please call PgSQL::init() at application startup.');
    }
}
