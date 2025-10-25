<?php

namespace Tests\Helpers;

class TestHelper
{
    public static function getTestConfig(): array
    {
        return [
            'host' => $_ENV['PGSQL_HOST'],
            'username' => $_ENV['PGSQL_USERNAME'],
            'database' => $_ENV['PGSQL_DATABASE'],
            'password' => $_ENV['PGSQL_PASSWORD'],
            'port' => (int) $_ENV['PGSQL_PORT'],
        ];
    }
}