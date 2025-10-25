<?php

declare(strict_types=1);

use Hibla\Postgres\Manager\PoolManager;
use Tests\Helpers\TestHelper;

describe('PoolManager Constructor', function () {
    it('creates a pool with valid configuration', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);
        $stats = $pool->getStats();

        expect($stats['max_size'])->toBe(5)
            ->and($stats['active_connections'])->toBe(0)
            ->and($stats['pooled_connections'])->toBe(0)
            ->and($stats['waiting_requests'])->toBe(0)
            ->and($stats['config_validated'])->toBeTrue()
        ;
    });

    it('uses default max size of 10', function () {
        $pool = new PoolManager(TestHelper::getTestConfig());
        $stats = $pool->getStats();

        expect($stats['max_size'])->toBe(10);
    });

    it('throws exception for empty configuration', function () {
        expect(fn () => new PoolManager([]))
            ->toThrow(InvalidArgumentException::class, 'Database configuration cannot be empty')
        ;
    });

    it('throws exception for missing host', function () {
        expect(fn () => new PoolManager([
            'username' => 'postgres',
            'database' => 'test_db',
        ]))->toThrow(InvalidArgumentException::class, "Missing required database configuration field: 'host'");
    });

    it('throws exception for missing username', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'database' => 'test_db',
        ]))->toThrow(InvalidArgumentException::class, "Missing required database configuration field: 'username'");
    });

    it('throws exception for missing database', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'username' => 'postgres',
        ]))->toThrow(InvalidArgumentException::class, "Missing required database configuration field: 'database'");
    });

    it('throws exception for empty host', function () {
        expect(fn () => new PoolManager([
            'host' => '',
            'username' => 'postgres',
            'database' => 'test_db',
        ]))->toThrow(InvalidArgumentException::class, "Database configuration field 'host' cannot be empty");
    });

    it('throws exception for empty database', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => '',
        ]))->toThrow(InvalidArgumentException::class, "Database configuration field 'database' cannot be empty");
    });

    it('throws exception for null host', function () {
        expect(fn () => new PoolManager([
            'host' => null,
            'username' => 'postgres',
            'database' => 'test_db',
        ]))->toThrow(InvalidArgumentException::class, "Database configuration field 'host' cannot be empty");
    });

    it('throws exception for invalid port type', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => 'test_db',
            'port' => 'invalid',
        ]))->toThrow(InvalidArgumentException::class, 'Database port must be a positive integer');
    });

    it('throws exception for negative port', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => 'test_db',
            'port' => -1,
        ]))->toThrow(InvalidArgumentException::class, 'Database port must be a positive integer');
    });

    it('throws exception for zero port', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => 'test_db',
            'port' => 0,
        ]))->toThrow(InvalidArgumentException::class, 'Database port must be a positive integer');
    });

    it('throws exception for non-string host', function () {
        expect(fn () => new PoolManager([
            'host' => 12345,
            'username' => 'postgres',
            'database' => 'test_db',
        ]))->toThrow(InvalidArgumentException::class, 'Database host must be a string');
    });

    it('throws exception for invalid sslmode', function () {
        expect(fn () => new PoolManager([
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => 'test_db',
            'sslmode' => 'invalid',
        ]))->toThrow(InvalidArgumentException::class, 'Invalid sslmode value');
    });

    it('accepts valid sslmode values', function () {
        $validModes = ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'];

        foreach ($validModes as $mode) {
            $pool = new PoolManager([
                'host' => 'localhost',
                'username' => 'postgres',
                'database' => 'test_db',
                'sslmode' => $mode,
            ], 5);

            expect($pool->getStats()['config_validated'])->toBeTrue();
        }
    });
});
