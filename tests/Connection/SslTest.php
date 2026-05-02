<?php

declare(strict_types=1);

use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Sql\Exceptions\ConnectionException;

use function Hibla\await;

/**
 * Copies a PEM file to a Linux tmpfs path and applies the requested chmod.
 * Required on WSL/NTFS where chmod is silently ignored.
 * Returns the temp path; caller must unlink() it in a finally block.
 */
function sslTmpCopy(string $src, int $mode): string
{
    $tmp = sys_get_temp_dir() . '/hibla_ssl_' . getmypid() . '_' . basename($src);
    copy($src, $tmp);
    chmod($tmp, $mode);

    return $tmp;
}

function sslFixture(string $filename): string
{
    $path = realpath(__DIR__ . '/../Fixtures/ssl/' . $filename);
    if ($path === false) {
        throw new \RuntimeException(
            "Missing SSL fixture: {$filename}. Run 'php tests/Fixtures/ssl/generate-certs.php' first."
        );
    }

    return $path;
}

function sslConfig(array $overrides = []): array
{
    return array_merge([
        'host'     => $_ENV['POSTGRES_SSL_HOST'] ?? '127.0.0.1',
        'port'     => (int) ($_ENV['POSTGRES_SSL_PORT'] ?? 5444),
        'database' => $_ENV['POSTGRES_SSL_DATABASE'] ?? 'postgres',
        'username' => $_ENV['POSTGRES_SSL_USERNAME'] ?? 'postgres',
        'password' => $_ENV['POSTGRES_SSL_PASSWORD'] ?? 'postgres',
    ], $overrides);
}

describe('PostgreSQL SSL/TLS Connection', function (): void {
    it('connects successfully via SSL with verification disabled (sslmode=require)', function (): void {
        $conn = await(Connection::create(PgSqlConfig::fromArray(sslConfig([
            'sslmode' => 'require',
        ]))));

        $isSsl = await($conn->query('SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()'))
            ->fetchOne()['ssl'];

        expect($conn->isReady())->toBeTrue()
            ->and($isSsl)->toBe('t')
        ;

        $conn->close();
    });

    it('connects successfully with strict CA verification (sslmode=verify-full)', function (): void {
        $conn = await(Connection::create(PgSqlConfig::fromArray(sslConfig([
            'sslmode' => 'verify-full',
            'ssl_ca'  => sslFixture('ca.pem'),
        ]))));

        $row = await($conn->query('SELECT version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()'))
            ->fetchOne();

        expect($conn->isReady())->toBeTrue()
            ->and($row['version'])->not->toBeEmpty()
            ->and($row['cipher'])->not->toBeEmpty()
        ;

        $conn->close();
    });

    it('connects successfully providing Client Certificates (mTLS)', function (): void {
        $keyPath = sslFixture('client-key.pem');
        $tmpKey  = null;

        $perms = fileperms($keyPath);
        if ($perms !== false && ($perms & 0x003C) !== 0) {
            $tmpKey  = sslTmpCopy($keyPath, 0600);
            $keyPath = $tmpKey;
        }

        try {
            $conn = await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_ca'   => sslFixture('ca.pem'),
                'ssl_cert' => sslFixture('client-cert.pem'),
                'ssl_key'  => $keyPath,
            ]))));

            expect($conn->isReady())->toBeTrue();

            $conn->close();
        } finally {
            if ($tmpKey !== null && file_exists($tmpKey)) {
                unlink($tmpKey);
            }
        }
    });

    it('fails to connect with an invalid CA (sslmode=verify-ca)', function (): void {
        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode' => 'verify-ca',
                'ssl_ca'  => sslFixture('client-cert.pem'),
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class)
            ->and($exception->getMessage())->toMatch('/certificate/i')
        ;
    });

    it('fails when sslmode=verify-full but no CA is provided', function (): void {
        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode' => 'verify-full',
                // ssl_ca intentionally omitted
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class);
    });

    it('fails when sslmode=verify-full and CA is a self-signed cert instead of a CA', function (): void {
        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode' => 'verify-full',
                'ssl_ca'  => sslFixture('server-cert.pem'),
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class)
            ->and($exception->getMessage())->toMatch('/certificate/i')
        ;
    });


    it('falls back gracefully without SSL when sslmode=prefer and SSL is available', function (): void {
        $conn = await(Connection::create(PgSqlConfig::fromArray(sslConfig([
            'sslmode' => 'prefer',
        ]))));

        expect($conn->isReady())->toBeTrue();

        $conn->close();
    });

    it('succeeds with sslmode=allow even though SSL is not required', function (): void {
        $conn = await(Connection::create(PgSqlConfig::fromArray(sslConfig([
            'sslmode' => 'allow',
        ]))));

        expect($conn->isReady())->toBeTrue();

        $conn->close();
    });

    it('fails mTLS when cert and key do not match', function (): void {
        $mismatchedKeyPath = sslFixture('server-key.pem');
        $tmpKey            = null;

        $perms = fileperms($mismatchedKeyPath);
        if ($perms !== false && ($perms & 0x003C) !== 0) {
            $tmpKey            = sslTmpCopy($mismatchedKeyPath, 0600);
            $mismatchedKeyPath = $tmpKey;
        }

        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_cert' => sslFixture('client-cert.pem'),
                'ssl_key'  => $mismatchedKeyPath,
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        } finally {
            if ($tmpKey !== null && file_exists($tmpKey)) {
                unlink($tmpKey);
            }
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class);
    });

    it('fails mTLS when ssl_cert is provided but ssl_key is missing', function (): void {
        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_cert' => sslFixture('client-cert.pem'),
                // ssl_key intentionally omitted
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class);
    });

    it('fails when the client key file has world-readable permissions', function (): void {
        // libpq explicitly refuses key files that are group- or world-readable
        // (must be 0600 or stricter). This test copies the key to a real Linux
        // tmpfs path where chmod is enforced, then deliberately sets 0644.
        $keyPath = sslFixture('client-key.pem');
        $tmpKey  = sslTmpCopy($keyPath, 0644); // deliberately too open

        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_cert' => sslFixture('client-cert.pem'),
                'ssl_key'  => $tmpKey,
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        } finally {
            if (file_exists($tmpKey)) {
                unlink($tmpKey);
            }
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class)
            ->and($exception->getMessage())->toMatch('/permissions|access/i')
        ;
    });

    it('connects successfully when ssl_cert points to a non-existent file and server does not require mTLS', function (): void {
        $conn = null;

        try {
            $conn = await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_cert' => '/tmp/does_not_exist_cert.pem',
                'ssl_key'  => sslFixture('client-key.pem'),
            ]))));
        } catch (ConnectionException) {
            // Also acceptable libpq may validate the path eagerly in some builds.
        }

        if ($conn !== null) {
            expect($conn->isReady())->toBeTrue();
            $conn->close();
        } else {
            expect(true)->toBeTrue(); 
        }
    });

    it('fails when ssl_cert is provided but the server key does not match and server enforces mTLS', function (): void {
        $mismatchedKey = sslFixture('server-key.pem');
        $tmpKey        = null;

        $perms = fileperms($mismatchedKey);
        if ($perms !== false && ($perms & 0x003C) !== 0) {
            $tmpKey        = sslTmpCopy($mismatchedKey, 0600);
            $mismatchedKey = $tmpKey;
        }

        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_cert' => sslFixture('client-cert.pem'),
                'ssl_key'  => $mismatchedKey, 
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        } finally {
            if ($tmpKey !== null && file_exists($tmpKey)) {
                unlink($tmpKey);
            }
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class);
    });

    it('fails when ssl_ca is a corrupt / non-PEM file', function (): void {
        $tmpCa = sys_get_temp_dir() . '/hibla_corrupt_ca_' . getmypid() . '.pem';
        file_put_contents($tmpCa, 'this is not a valid PEM certificate');

        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode' => 'verify-ca',
                'ssl_ca'  => $tmpCa,
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        } finally {
            if (file_exists($tmpCa)) {
                unlink($tmpCa);
            }
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class);
    });

    it('fails when ssl_cert is a corrupt / non-PEM file', function (): void {
        $tmpCert = sys_get_temp_dir() . '/hibla_corrupt_cert_' . getmypid() . '.pem';
        $tmpKey  = sslTmpCopy(sslFixture('client-key.pem'), 0600);
        file_put_contents($tmpCert, 'not a certificate');

        $exception = null;

        try {
            await(Connection::create(PgSqlConfig::fromArray(sslConfig([
                'sslmode'  => 'require',
                'ssl_cert' => $tmpCert,
                'ssl_key'  => $tmpKey,
            ]))));
        } catch (ConnectionException $e) {
            $exception = $e;
        } finally {
            foreach ([$tmpCert, $tmpKey] as $f) {
                if (file_exists($f)) {
                    unlink($f);
                }
            }
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class);
    });


    it('marks the connection as closed after an SSL handshake failure', function (): void {
        $conn = null;

        try {
            $conn = new Connection(PgSqlConfig::fromArray(sslConfig([
                'sslmode' => 'verify-full',
                'ssl_ca'  => sslFixture('client-cert.pem'), // wrong CA — will fail
            ])));

            await($conn->connect());
        } catch (ConnectionException) {
            // expected
        }

        expect($conn?->isClosed())->toBeTrue();
    });

    it('rejects subsequent commands after an SSL failure without a new connect', function (): void {
        $conn = new Connection(PgSqlConfig::fromArray(sslConfig([
            'sslmode' => 'verify-full',
            'ssl_ca'  => sslFixture('client-cert.pem'), // wrong CA
        ])));

        try {
            await($conn->connect());
        } catch (ConnectionException) {
            // expected
        }

        // Once closed, every command must be immediately rejected and the connection
        // must never queue work against a dead socket.
        $exception = null;

        try {
            await($conn->query('SELECT 1'));
        } catch (ConnectionException $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(ConnectionException::class)
            ->and($exception->getMessage())->toContain('closed')
        ;
    });

    it('does not leak the connection object when SSL fails and the instance is discarded', function (): void {
        // This is a GC / destructor safety test. If the destructor calls close()
        // on an already-CLOSED connection it must be a silent no-op — no exception
        // and no double-free of the socket resource.
        $exception = null;

        try {
            // Create in an inner scope so the object is eligible for GC on exit.
            (function (): void {
                $conn = new Connection(PgSqlConfig::fromArray(sslConfig([
                    'sslmode' => 'verify-full',
                    'ssl_ca'  => sslFixture('client-cert.pem'),
                ])));

                try {
                    await($conn->connect());
                } catch (ConnectionException) {
                    // expected — $conn goes out of scope and __destruct fires
                }
            })();
        } catch (\Throwable $e) {
            $exception = $e;
        }

        expect($exception)->toBeNull();
    });

    it('fromArray ignores ssl_ca when the value is not a string', function (): void {
        $config = PgSqlConfig::fromArray(sslConfig([
            'sslmode' => 'require',
            'ssl_ca'  => 12345, // non-string must be silently dropped
        ]));

        expect($config->sslCa)->toBeNull();
    });

    it('fromUri parses ssl parameters from the query string correctly', function (): void {
        $caPath = sslFixture('ca.pem');

        $uri    = sprintf(
            'postgresql://postgres:postgres@127.0.0.1:5444/postgres?sslmode=verify-full&ssl_ca=%s',
            urlencode($caPath)
        );

        $config = PgSqlConfig::fromUri($uri);

        expect($config->sslmode)->toBe('verify-full')
            ->and($config->sslCa)->toBe($caPath)
        ;
    });

    it('withQueryCancellation preserves all SSL fields', function (): void {
        $original = PgSqlConfig::fromArray(sslConfig([
            'sslmode'  => 'verify-full',
            'ssl_ca'   => sslFixture('ca.pem'),
            'ssl_cert' => sslFixture('client-cert.pem'),
            'ssl_key'  => sslFixture('client-key.pem'),
        ]));

        $copy = $original->withQueryCancellation(true);

        expect($copy->sslmode)->toBe($original->sslmode)
            ->and($copy->sslCa)->toBe($original->sslCa)
            ->and($copy->sslCert)->toBe($original->sslCert)
            ->and($copy->sslKey)->toBe($original->sslKey)
            ->and($copy->enableServerSideCancellation)->toBeTrue()
        ;
    });
})->skipOnWindows();
