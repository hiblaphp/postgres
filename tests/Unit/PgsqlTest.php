<?php

declare(strict_types=1);

use Hibla\Postgres\ValueObjects\PgSqlConfig;

it('throws exception if killTimeoutSeconds is zero or negative', function () {
    expect(fn () => new PgSqlConfig(host: 'localhost', killTimeoutSeconds: 0.0))
        ->toThrow(InvalidArgumentException::class, 'killTimeoutSeconds must be greater than zero')
    ;

    expect(fn () => new PgSqlConfig(host: 'localhost', killTimeoutSeconds: -1.5))
        ->toThrow(InvalidArgumentException::class, 'killTimeoutSeconds must be greater than zero')
    ;
});

it('parses a complete configuration array correctly', function () {
    $config = PgSqlConfig::fromArray([
        'host' => 'db.example.com',
        'port' => '5433',
        'username' => 'admin',
        'password' => 'secret',
        'database' => 'mydb',
        'sslmode' => 'require',
        'ssl_ca' => '/path/ca.crt',
        'ssl_cert' => '/path/client.crt',
        'ssl_key' => '/path/client.key',
        'connect_timeout' => '15',
        'application_name' => 'myapp',
        'kill_timeout_seconds' => '5.5',
        'enable_server_side_cancellation' => 1,
        'reset_connection' => true,
        'cast_prepared_types' => false,
    ]);

    expect($config->host)->toBe('db.example.com')
        ->and($config->port)->toBe(5433)
        ->and($config->username)->toBe('admin')
        ->and($config->password)->toBe('secret')
        ->and($config->database)->toBe('mydb')
        ->and($config->sslmode)->toBe('require')
        ->and($config->sslCa)->toBe('/path/ca.crt')
        ->and($config->sslCert)->toBe('/path/client.crt')
        ->and($config->sslKey)->toBe('/path/client.key')
        ->and($config->connectTimeout)->toBe(15)
        ->and($config->applicationName)->toBe('myapp')
        ->and($config->killTimeoutSeconds)->toBe(5.5)
        ->and($config->enableServerSideCancellation)->toBeTrue()
        ->and($config->resetConnection)->toBeTrue()
        ->and($config->castPreparedTypes)->toBeFalse()
    ;
});

it('throws exception if host is missing or invalid in array', function () {
    expect(fn () => PgSqlConfig::fromArray([]))
        ->toThrow(InvalidArgumentException::class, 'Host is required')
    ;

    expect(fn () => PgSqlConfig::fromArray(['host' => 12345]))
        ->toThrow(InvalidArgumentException::class, 'Host must be a string')
    ;
});

it('applies defaults for missing array keys', function () {
    $config = PgSqlConfig::fromArray(['host' => 'localhost']);

    expect($config->port)->toBe(5432)
        ->and($config->username)->toBe('postgres')
        ->and($config->password)->toBe('')
        ->and($config->database)->toBe('')
        ->and($config->sslmode)->toBe('prefer')
        ->and($config->castPreparedTypes)->toBeTrue()
        ->and($config->sslCa)->toBeNull()
    ;
});

it('parses a standard PostgreSQL URI correctly', function () {
    $uri = 'postgresql://myuser:mypass@127.0.0.1:5433/mydb';
    $config = PgSqlConfig::fromUri($uri);

    expect($config->host)->toBe('127.0.0.1')
        ->and($config->port)->toBe(5433)
        ->and($config->username)->toBe('myuser')
        ->and($config->password)->toBe('mypass')
        ->and($config->database)->toBe('mydb')
    ;
});

it('prepends postgresql:// scheme automatically if missing', function () {
    $uri = '127.0.0.1:5432/testdb';
    $config = PgSqlConfig::fromUri($uri);

    expect($config->host)->toBe('127.0.0.1')
        ->and($config->port)->toBe(5432)
        ->and($config->database)->toBe('testdb')
    ;
});

it('throws exception for unsupported URI schemes', function () {
    expect(fn () => PgSqlConfig::fromUri('mysql://127.0.0.1/db'))
        ->toThrow(InvalidArgumentException::class, 'Invalid URI scheme "mysql", expected "postgresql"')
    ;
});

it('throws exception for completely malformed URIs', function () {
    expect(fn () => PgSqlConfig::fromUri('postgresql:///'))
        ->toThrow(InvalidArgumentException::class, 'Invalid PostgreSQL URI')
    ;
});

it('decodes URL-encoded credentials and database names', function () {
    $uri = 'postgresql://user%40domain.com:p%40ss%3Aword@localhost/my%20db';
    $config = PgSqlConfig::fromUri($uri);

    expect($config->username)->toBe('user@domain.com')
        ->and($config->password)->toBe('p@ss:word')
        ->and($config->database)->toBe('my db')
    ;
});

it('parses query string parameters including boolean strings correctly', function () {
    $uri = 'postgresql://localhost/db?sslmode=verify-full&connect_timeout=30&reset_connection=true&cast_prepared_types=false&ssl_ca=/ca.pem';
    $config = PgSqlConfig::fromUri($uri);

    expect($config->sslmode)->toBe('verify-full')
        ->and($config->connectTimeout)->toBe(30)
        ->and($config->resetConnection)->toBeTrue()
        ->and($config->castPreparedTypes)->toBeFalse()
        ->and($config->sslCa)->toBe('/ca.pem')
    ;
});

it('creates a new instance when changing query cancellation', function () {
    $original = PgSqlConfig::fromArray(['host' => 'localhost', 'enable_server_side_cancellation' => false]);
    $modified = $original->withQueryCancellation(true);

    expect($original->enableServerSideCancellation)->toBeFalse()
        ->and($modified->enableServerSideCancellation)->toBeTrue()
        ->and($original)->not->toBe($modified)
    ;
});

it('formats connection string for pg_connect correctly', function () {
    $config = PgSqlConfig::fromArray([
        'host' => '127.0.0.1',
        'port' => 5432,
        'database' => 'mydb',
        'username' => 'postgres',
        'application_name' => 'test_app',
    ]);

    $str = $config->toConnectionString();

    expect($str)->toContain("host='127.0.0.1'")
        ->and($str)->toContain("port='5432'")
        ->and($str)->toContain("dbname='mydb'")
        ->and($str)->toContain("user='postgres'")
        ->and($str)->toContain("application_name='test_app'")
        ->and($str)->not->toContain('password=')
    ;
});

it('properly escapes single quotes in passwords for connection string', function () {
    $config = PgSqlConfig::fromArray([
        'host' => 'localhost',
        'password' => "my'super'secret\\pass",
    ]);

    $str = $config->toConnectionString();

    expect($str)->toContain("password='my\\'super\\'secret\\pass'");
});

it('includes SSL parameters only when they are set', function () {
    $configWithSSL = PgSqlConfig::fromArray([
        'host' => 'localhost',
        'ssl_ca' => '/path/to/ca',
        'ssl_cert' => '/path/to/cert',
        'ssl_key' => '/path/to/key',
    ]);

    $str = $configWithSSL->toConnectionString();

    expect($str)->toContain("sslrootcert='/path/to/ca'")
        ->and($str)->toContain("sslcert='/path/to/cert'")
        ->and($str)->toContain("sslkey='/path/to/key'");
});
