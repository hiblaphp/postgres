<?php

declare(strict_types=1);

use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\ValueObjects\PgSqlConfig;

use function Hibla\await;

function pgConfig(?PgSqlConfig $config = null): PgSqlConfig
{
    return $config ?? new PgSqlConfig(
        host: getenv('PG_HOST') ?: '127.0.0.1',
        port: (int) (getenv('PG_PORT') ?: 5443),
        username: getenv('PG_USER') ?: 'postgres',
        password: getenv('PG_PASSWORD') ?: 'postgres',
        database: getenv('PG_DB') ?: 'postgres',
    );
}

function pgConn(?PgSqlConfig $config = null): Connection
{
    return await(
        Connection::create($config ?? pgConfig())
    );
}
