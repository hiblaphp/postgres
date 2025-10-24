<?php

namespace Hibla\Postgres\Exceptions;

use RuntimeException;

class PostgreNotInitializedException extends RuntimeException
{
    public function __construct()
    {
        parent::__construct('PgSQLConnection instance has not been initialized.');
    }
}
