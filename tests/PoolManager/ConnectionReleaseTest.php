<?php

use Hibla\Postgres\Manager\PoolManager;
use PgSql\Connection;
use Tests\Helpers\TestHelper;

describe('PoolManager Connection Release', function () {
    it('returns connection to pool', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);

        $connection = $pool->get()->await();
        $pool->release($connection);

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(1)
            ->and($stats['active_connections'])->toBe(1)
        ;

        $pool->close();
    });

    it('passes connection to waiting request', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 1);

        $connection1 = $pool->get()->await();

        $promise = $pool->get();
        expect($promise->isPending())->toBeTrue();

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(1);

        $pool->release($connection1);

        $connection2 = $promise->await();
        expect($connection2)->toBe($connection1);

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(0)
            ->and($stats['pooled_connections'])->toBe(0)
        ;

        $pool->release($connection2);
        $pool->close();
    });

    it('handles dead connection by removing from pool', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 2);

        $connection = $pool->get()->await();
        $initialActive = $pool->getStats()['active_connections'];

        // Close the connection to make it "dead"
        pg_close($connection);

        $pool->release($connection);

        $stats = $pool->getStats();
        expect($stats['active_connections'])->toBeLessThan($initialActive);

        $pool->close();
    });

    it('creates new connection for waiter when released connection is dead', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 2);

        $connection1 = $pool->get()->await();
        $connection2 = $pool->get()->await();

        $promise = $pool->get();
        expect($promise->isPending())->toBeTrue();

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(1);

        // Close connection1 to make it dead
        pg_close($connection1);
        $pool->release($connection1);

        $connection3 = $promise->await();
        expect($connection3)->toBeInstanceOf(Connection::class);
        expect($connection3)->not->toBe($connection1);

        $pool->release($connection2);
        $pool->release($connection3);
        $pool->close();
    });

    it('rolls back active transaction before pooling', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);

        $connection = $pool->get()->await();

        pg_query($connection, 'CREATE TEMP TABLE test_table (id SERIAL PRIMARY KEY)');
        pg_query($connection, 'BEGIN');
        pg_query($connection, 'INSERT INTO test_table DEFAULT VALUES');
        
        expect(pg_transaction_status($connection))->not->toBe(PGSQL_TRANSACTION_IDLE);

        $pool->release($connection);

        $connection2 = $pool->get()->await();

        expect(pg_transaction_status($connection2))->toBe(PGSQL_TRANSACTION_IDLE);

        $pool->release($connection2);
        $pool->close();
    });

    it('updates last connection on release to waiter', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 1);

        $connection1 = $pool->get()->await();
        expect($pool->getLastConnection())->toBe($connection1);

        $promise = $pool->get();
        $pool->release($connection1);

        $connection2 = $promise->await();
        expect($pool->getLastConnection())->toBe($connection2);

        $pool->release($connection2);
        $pool->close();
    });
});