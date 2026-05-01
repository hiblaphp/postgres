<?php

declare(strict_types=1);

use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Sql\Exceptions\ConnectionException;

use function Hibla\await;

describe('PostgreSQL SSL/TLS Connection', function (): void {

    it('connects successfully via SSL with verification disabled (sslmode=require)', function (): void {
        $config = PgSqlConfig::fromArray([
            'host' => $_ENV['POSTGRES_SSL_HOST'] ?? '127.0.0.1',
            'port' => (int) ($_ENV['POSTGRES_SSL_PORT'] ?? 5444),
            'database' => $_ENV['POSTGRES_SSL_DATABASE'] ?? 'postgres',
            'username' => $_ENV['POSTGRES_SSL_USERNAME'] ?? 'postgres',
            'password' => $_ENV['POSTGRES_SSL_PASSWORD'] ?? 'postgres',
            'sslmode' => 'require',
        ]);

        $conn = await(Connection::create($config));

        $result = await($conn->query('SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()'));
        $isSsl = $result->fetchOne()['ssl'];

        expect($conn->isReady())->toBeTrue()
            ->and($isSsl)->toBe('t')
        ;

        $conn->close();
    });

    it('connects successfully with strict CA verification (sslmode=verify-full)', function (): void {
        $caPath = realpath(__DIR__ . '/../Fixtures/ssl/ca.pem');

        $config = PgSqlConfig::fromArray([
            'host' => $_ENV['POSTGRES_SSL_HOST'] ?? '127.0.0.1',
            'port' => (int) ($_ENV['POSTGRES_SSL_PORT'] ?? 5444),
            'database' => $_ENV['POSTGRES_SSL_DATABASE'] ?? 'postgres',
            'username' => $_ENV['POSTGRES_SSL_USERNAME'] ?? 'postgres',
            'password' => $_ENV['POSTGRES_SSL_PASSWORD'] ?? 'postgres',
            'sslmode' => 'verify-full',
            'ssl_ca' => $caPath,
        ]);

        $conn = await(Connection::create($config));

        $result = await($conn->query('SELECT version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()'));
        $sslInfo = $result->fetchOne();

        expect($conn->isReady())->toBeTrue()
            ->and($sslInfo['version'])->not->toBeEmpty()
            ->and($sslInfo['cipher'])->not->toBeEmpty()
        ;

        $conn->close();
    });

    it('fails to connect with invalid CA (sslmode=verify-ca)', function (): void {
        $fakeCaPath = realpath(__DIR__ . '/../Fixtures/ssl/client-cert.pem');

        $config = PgSqlConfig::fromArray([
            'host' => $_ENV['POSTGRES_SSL_HOST'] ?? '127.0.0.1',
            'port' => (int) ($_ENV['POSTGRES_SSL_PORT'] ?? 5444),
            'database' => 'postgres',
            'username' => 'postgres',
            'password' => 'postgres',
            'sslmode' => 'verify-ca',
            'ssl_ca' => $fakeCaPath,
        ]);

        $exception = null;

        try {
            await(Connection::create($config));
        } catch (ConnectionException $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class)
            ->and($exception->getMessage())->toMatch('/certificate/i')
        ;
    });

    it('connects successfully providing Client Certificates (mTLS)', function (): void {
        $fixtureDir = __DIR__ . '/../Fixtures/ssl';

        $caPath = realpath($fixtureDir . '/ca.pem');
        $certPath = realpath($fixtureDir . '/client-cert.pem');
        $keyPath = realpath($fixtureDir . '/client-key.pem');

        if ($caPath === false || $certPath === false || $keyPath === false) {
            $this->fail("Missing SSL fixtures. Run 'php tests/Fixtures/ssl/generate-certs.php' first.");
        }

        $config = PgSqlConfig::fromArray([
            'host' => $_ENV['POSTGRES_SSL_HOST'] ?? '127.0.0.1',
            'port' => (int) ($_ENV['POSTGRES_SSL_PORT'] ?? 5444),
            'database' => $_ENV['POSTGRES_SSL_DATABASE'] ?? 'postgres',
            'username' => $_ENV['POSTGRES_SSL_USERNAME'] ?? 'postgres',
            'password' => $_ENV['POSTGRES_SSL_PASSWORD'] ?? 'postgres',
            'sslmode' => 'require',
            'ssl_ca' => $caPath,
            'ssl_cert' => $certPath,
            'ssl_key' => $keyPath,
        ]);

        $conn = await(Connection::create($config));

        expect($conn->isReady())->toBeTrue();

        $conn->close();
    });
})->skipOnWindows();
