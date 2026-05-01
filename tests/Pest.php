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

function twentyRowPgSql(): string
{
    return 'SELECT n FROM generate_series(1, 20) AS n ORDER BY n';
}

function twentyRowPgPreparedSql(): string
{
    return 'SELECT n FROM generate_series(1, $1) AS n ORDER BY n';
}

function pgConnWith(bool $enableServerSideCancellation = true): Connection
{
    return await(Connection::create(
        pgConfig()->withQueryCancellation($enableServerSideCancellation)
    ));
}

function awaitCancelDrain(Connection $conn): void
{
    await($conn->ping());
    $conn->clearCancelledFlag();
}
