<?php

use Hibla\Postgres\Manager\PoolManager;
use PgSql\Connection;
use Tests\Helpers\TestHelper;

describe('PoolManager Edge Cases', function () {
    it('handles max size of 1', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 1);

        $connection1 = $pool->get()->await();

        $promise = $pool->get();
        expect($pool->getStats()['waiting_requests'])->toBe(1);

        $pool->release($connection1);

        $connection2 = $promise->await();
        expect($connection2)->toBeInstanceOf(Connection::class);
        expect($pool->getStats()['waiting_requests'])->toBe(0);

        $pool->release($connection2);
        $pool->close();
    });

    it('handles releasing same connection multiple times', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);

        $connection = $pool->get()->await();

        $pool->release($connection);
        expect($pool->getStats()['pooled_connections'])->toBe(1);

        $pool->release($connection);
        expect($pool->getStats()['pooled_connections'])->toBe(2);

        $pool->close();
    });

    it('works with actual database operations', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);

        $connection = $pool->get()->await();

        pg_query($connection, 'CREATE TEMP TABLE test (id SERIAL PRIMARY KEY, name TEXT)');
        pg_query($connection, "INSERT INTO test (name) VALUES ('Alice')");

        $pool->release($connection);

        $connection2 = $pool->get()->await();

        $result = pg_query($connection2, 'SELECT * FROM test');
        $row = pg_fetch_assoc($result);
        expect($row['name'])->toBe('Alice');

        $pool->release($connection2);
        $pool->close();
    });

    it('maintains connection state across pool cycles', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 3);

        $conn1 = $pool->get()->await();
        pg_query($conn1, 'CREATE TEMP TABLE users (id SERIAL PRIMARY KEY, name TEXT)');
        pg_query($conn1, "INSERT INTO users (name) VALUES ('John')");

        $pool->release($conn1);

        $conn2 = $pool->get()->await();

        expect($conn2)->toBe($conn1);
        $result = pg_query($conn2, 'SELECT COUNT(*) as count FROM users');
        $row = pg_fetch_assoc($result);
        expect((int)$row['count'])->toBe(1);

        $pool->release($conn2);
        $pool->close();
    });

    it('handles concurrent requests properly', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 2);

        $conn1 = $pool->get()->await();
        $conn2 = $pool->get()->await();

        $promise1 = $pool->get();
        $promise2 = $pool->get();

        expect($pool->getStats()['waiting_requests'])->toBe(2);

        $pool->release($conn1);
        $pool->release($conn2);

        $conn3 = $promise1->await();
        $conn4 = $promise2->await();

        expect($conn3)->toBeInstanceOf(Connection::class);
        expect($conn4)->toBeInstanceOf(Connection::class);
        expect($pool->getStats()['waiting_requests'])->toBe(0);

        $pool->release($conn3);
        $pool->release($conn4);
        $pool->close();
    });

    it('handles connection with empty password', function () {
        $config = [
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => 'test_db',
            'password' => '',
        ];

        $pool = new PoolManager($config, 5);

        expect($pool->getStats()['config_validated'])->toBeTrue();

        $pool->close();
    });

    it('handles connection without password field', function () {
        $config = [
            'host' => 'localhost',
            'username' => 'postgres',
            'database' => 'test_db',
        ];

        $pool = new PoolManager($config, 5);

        expect($pool->getStats()['config_validated'])->toBeTrue();

        $pool->close();
    });

    it('rejects waiting promise on connection creation failure', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 1);

        $conn1 = $pool->get()->await();

        $badPool = new PoolManager([
            'host' => 'invalid-host-12345.example.com',
            'username' => 'postgres',
            'database' => 'test_db',
        ], 1);

        $promise = $badPool->get();

        try {
            $promise->await();
            expect(false)->toBeTrue('Should have thrown exception');
        } catch (RuntimeException $e) {
            expect($e->getMessage())->toContain('PostgreSQL Connection failed');
        }

        $pool->release($conn1);
        $pool->close();
        $badPool->close();
    })->skip('Skipped to avoid DNS lookup warnings');
});
