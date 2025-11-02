<?php

declare(strict_types=1);

use Hibla\Postgres\AsyncPgSQLConnection;
use Hibla\Promise\Promise;

function createPersistentPgConnection(int $poolSize = 10): AsyncPgSQLConnection
{
    return new AsyncPgSQLConnection([
        'host' => getenv('PGSQL_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('PGSQL_PORT') ?: 5432),
        'database' => getenv('PGSQL_DATABASE') ?: 'postgres',
        'username' => getenv('PGSQL_USERNAME') ?: 'postgres',
        'password' => getenv('PGSQL_PASSWORD') ?: 'postgres',
        'persistent' => true,
    ], $poolSize);
}

function createRegularPgConnection(int $poolSize = 10): AsyncPgSQLConnection
{
    return new AsyncPgSQLConnection([
        'host' => getenv('PGSQL_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('PGSQL_PORT') ?: 5432),
        'database' => getenv('PGSQL_DATABASE') ?: 'postgres',
        'username' => getenv('PGSQL_USERNAME') ?: 'postgres',
        'password' => getenv('PGSQL_PASSWORD') ?: 'postgres',
        'persistent' => false,
    ], $poolSize);
}

describe('Connection ID Persistence', function () {
    it('reuses connection IDs within the same persistent client instance', function () {
        $client = createPersistentPgConnection();

        $connectionIds = [];
        for ($i = 0; $i < 5; $i++) {
            $id = $client->fetchValue('SELECT pg_backend_pid()')->await();
            $connectionIds[] = $id;
        }

        $uniqueIds = array_unique($connectionIds);
        expect($uniqueIds)->toHaveCount(1)
            ->and((int) $connectionIds[0])->toBeInt()->toBeGreaterThan(0)
        ;
    });

    it('reuses connection IDs within the same regular client instance', function () {
        $client = createRegularPgConnection();

        $connectionIds = [];
        for ($i = 0; $i < 5; $i++) {
            $id = $client->fetchValue('SELECT pg_backend_pid()')->await();
            $connectionIds[] = $id;
        }

        $uniqueIds = array_unique($connectionIds);
        expect($uniqueIds)->toHaveCount(1)
            ->and((int) $connectionIds[0])->toBeInt()->toBeGreaterThan(0)
        ;
    });
});

describe('Pool Statistics', function () {
    it('shows correct statistics for persistent connection pool', function () {
        $client = createPersistentPgConnection(10);

        $client->fetchValue('SELECT 1')->await();

        $stats = $client->getStats();

        expect($stats)
            ->toHaveKey('active_connections')
            ->toHaveKey('pooled_connections')
            ->toHaveKey('waiting_requests')
            ->toHaveKey('max_size')
            ->toHaveKey('config_validated')
            ->toHaveKey('persistent')
            ->and($stats['persistent'])->toBeTrue()
            ->and($stats['max_size'])->toBe(10)
            ->and($stats['config_validated'])->toBeTrue()
        ;
    });

    it('shows correct statistics for regular connection pool', function () {
        $client = createRegularPgConnection(10);

        $client->fetchValue('SELECT 1')->await();

        $stats = $client->getStats();

        expect($stats)
            ->toHaveKey('persistent')
            ->and($stats['persistent'])->toBeFalse()
            ->and($stats['max_size'])->toBe(10)
        ;
    });
});

describe('Backend PID Analysis', function () {
    it('returns valid backend PIDs for persistent connections', function () {
        $client = createPersistentPgConnection();

        $pid = $client->run(function ($connection) {
            return pg_get_pid($connection);
        })->await();

        expect($pid)->toBeInt()->toBeGreaterThan(0);
    });

    it('returns valid backend PIDs for regular connections', function () {
        $client = createRegularPgConnection();

        $pid = $client->run(function ($connection) {
            return pg_get_pid($connection);
        })->await();

        expect($pid)->toBeInt()->toBeGreaterThan(0);
    });
});

describe('Session Variable Persistence', function () {
    it('persists session variables within persistent client instance', function () {
        $client = createPersistentPgConnection();

        $client->execute("SET SESSION test.var = 'persistent_value'")->await();
        $value1 = $client->fetchValue("SELECT current_setting('test.var')")->await();
        $value2 = $client->fetchValue("SELECT current_setting('test.var')")->await();

        expect($value1)->toBe('persistent_value')
            ->and($value2)->toBe('persistent_value')
        ;
    });

    it('persists session variables within regular client instance', function () {
        $client = createRegularPgConnection();

        $client->execute("SET SESSION test.var = 'regular_value'")->await();
        $value1 = $client->fetchValue("SELECT current_setting('test.var')")->await();
        $value2 = $client->fetchValue("SELECT current_setting('test.var')")->await();

        expect($value1)->toBe('regular_value')
            ->and($value2)->toBe('regular_value')
        ;
    });
});

describe('Transaction Handling', function () {
    it('can start and rollback transactions on persistent connections', function () {
        $client = createPersistentPgConnection();

        $result = $client->transaction(function ($connection) {
            $result = await($connection->fetchValue('SELECT 1 as test'));

            return $result;
        })->await();

        expect($result)->toBe('1');
    });

    it('rolls back on exceptions', function () {
        $client = createPersistentPgConnection();

        try {
            $client->transaction(function ($connection) {
                await($connection->execute('CREATE TEMPORARY TABLE IF NOT EXISTS test_rollback (id INT)'));
                await($connection->execute('INSERT INTO test_rollback VALUES (1)'));

                throw new Exception('Intentional error');
            })->await();
        } catch (Exception $e) {
            expect($e->getMessage())->toContain('Intentional error');
        }
    });
});

describe('Parallel Execution Performance', function () {
    it('executes queries in parallel for persistent connections', function () {
        $client = createPersistentPgConnection();

        $startTime = microtime(true);
        Promise::all([
            $client->query('SELECT pg_sleep(1)'),
            $client->query('SELECT pg_sleep(1)'),
        ])->await();
        $executionTime = microtime(true) - $startTime;

        expect($executionTime)->toBeLessThan(1.5)
            ->toBeGreaterThan(0.9)
        ;
    });

    it('executes queries in parallel for regular connections', function () {
        $client = createRegularPgConnection();

        $startTime = microtime(true);
        Promise::all([
            $client->query('SELECT pg_sleep(1)'),
            $client->query('SELECT pg_sleep(1)'),
        ])->await();
        $executionTime = microtime(true) - $startTime;

        expect($executionTime)->toBeLessThan(1.5)
            ->toBeGreaterThan(0.9)
        ;
    });
});

describe('Connection Reuse with Small Pool', function () {
    it('reuses the same connection with pool size of 1', function () {
        $client = createPersistentPgConnection(1);

        $pids = [];
        for ($i = 0; $i < 5; $i++) {
            $pid = $client->run(function ($connection) {
                return pg_get_pid($connection);
            })->await();
            $pids[] = $pid;
        }

        $uniquePids = array_unique($pids);

        expect($uniquePids)->toHaveCount(1)
            ->and($pids)->toHaveCount(5)
        ;
    });
});

describe('TRUE Persistent Connection Test', function () {
    it('reuses connections across client instance recreation for persistent connections', function () {
        $client1 = createPersistentPgConnection(1);

        $pid1 = $client1->run(function ($connection) {
            return pg_get_pid($connection);
        })->await();

        $client1->execute("SET SESSION test.persistent_test = 'client1'")->await();
        $testVar1 = $client1->fetchValue("SELECT current_setting('test.persistent_test')")->await();

        expect($testVar1)->toBe('client1')
            ->and($pid1)->toBeInt()->toBeGreaterThan(0)
        ;

        $client1->reset();
        unset($client1);

        $client2 = createPersistentPgConnection(1);

        $pid2 = $client2->run(function ($connection) {
            return pg_get_pid($connection);
        })->await();

        try {
            $testVar2 = $client2->fetchValue("SELECT current_setting('test.persistent_test')")->await();
        } catch (Exception $e) {
            $testVar2 = null;
        }

        expect($pid2)->toBe($pid1)
            ->and($testVar2)->toBeNull()
        ;
    })->skip('PostgreSQL persistent connection test is not supported in this environment and only work on web context');

    it('does NOT reuse connections across client instance recreation for regular connections', function () {
        $client1 = createRegularPgConnection(1);

        $pid1 = $client1->run(function ($connection) {
            return pg_get_pid($connection);
        })->await();

        expect($pid1)->toBeInt()->toBeGreaterThan(0);

        $client1->reset();
        unset($client1);

        $client2 = createRegularPgConnection(1);

        $pid2 = $client2->run(function ($connection) {
            return pg_get_pid($connection);
        })->await();

        expect($pid2)->toBeInt()->toBeGreaterThan(0)
            ->not->toBe($pid1)
        ;
    });
});

describe('Server-Side Connection Analysis', function () {
    it('can query the PostgreSQL activity list', function () {
        $client = createPersistentPgConnection();

        $result = $client->query('SELECT pid, usename, datname, state FROM pg_stat_activity WHERE pid = pg_backend_pid()')->await();

        expect($result)->toBeArray()
            ->not->toBeEmpty()
            ->and($result[0])->toHaveKeys(['pid', 'usename', 'datname', 'state'])
        ;
    });
});

describe('Connection Properties', function () {
    it('provides connection metadata for persistent connections', function () {
        $client = createPersistentPgConnection();

        $metadata = $client->run(function ($connection) {
            return [
                'pid' => pg_get_pid($connection),
                'host' => pg_host($connection),
                'dbname' => pg_dbname($connection),
                'version' => pg_version($connection),
            ];
        })->await();

        expect($metadata['pid'])->toBeInt()->toBeGreaterThan(0)
            ->and($metadata['host'])->toBeString()
            ->and($metadata['dbname'])->toBeString()
            ->and($metadata['version'])->toBeArray()
        ;
    });
});

describe('Edge Cases', function () {
    it('handles multiple parallel operations correctly', function () {
        $client = createPersistentPgConnection(5);

        $promises = [];
        for ($i = 0; $i < 10; $i++) {
            $promises[] = $client->fetchValue("SELECT {$i}");
        }

        $results = Promise::all($promises)->await();

        expect($results)->toHaveCount(10);

        $intResults = array_map('intval', $results);

        expect($intResults[0])->toBe(0)
            ->and($intResults[9])->toBe(9)
        ;
    });

    it('handles connection pool exhaustion gracefully', function () {
        $client = createPersistentPgConnection(2);

        $promises = [];
        for ($i = 0; $i < 5; $i++) {
            $promises[] = $client->query('SELECT pg_sleep(0.1)');
        }

        $startTime = microtime(true);
        Promise::all($promises)->await();
        $duration = microtime(true) - $startTime;

        expect($duration)->toBeGreaterThan(0.2);
    });
});
