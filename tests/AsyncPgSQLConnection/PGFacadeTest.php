<?php

declare(strict_types=1);

use Hibla\Postgres\Exceptions\NotInitializedException;
use Hibla\Postgres\PG;
use Tests\Helpers\TestHelper;

describe('PG Facade', function () {
    beforeEach(function () {
        PG::reset();
    });

    afterEach(function () {
        PG::reset();
    });

    it('requires initialization before use', function () {
        expect(fn () => PG::query('SELECT 1')->await())
            ->toThrow(NotInitializedException::class)
        ;
    });

    it('initializes with configuration', function () {
        PG::init(TestHelper::getTestConfig(), 5);

        $stats = PG::getStats();
        expect($stats['max_size'])->toBe(5);
    });

    it('ignores multiple init calls', function () {
        PG::init(TestHelper::getTestConfig(), 5);
        PG::init(TestHelper::getTestConfig(), 10);

        $stats = PG::getStats();
        expect($stats['max_size'])->toBe(5);
    });

    it('delegates query to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        $result = PG::query('SELECT 1 as num')->await();

        expect($result)->toBeArray()
            ->and($result[0]['num'])->toBe('1')
        ;
    });

    it('delegates fetchOne to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        $result = PG::fetchOne('SELECT 1 as num')->await();

        expect($result)->toBeArray()
            ->and($result['num'])->toBe('1')
        ;
    });

    it('delegates fetchValue to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        $result = PG::fetchValue('SELECT 42')->await();

        expect($result)->toBe('42');
    });

    it('delegates execute to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        PG::execute('CREATE TEMPORARY TABLE test_pg (id INT)')->await();
        $affected = PG::execute('INSERT INTO test_pg VALUES (1)')->await();

        expect($affected)->toBe(1);
    });

    it('delegates transaction to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        PG::execute('CREATE TEMPORARY TABLE test_pg (value INT)')->await();

        $result = PG::transaction(function ($conn) {
            pg_query($conn, 'INSERT INTO test_pg VALUES (100)');

            return 'done';
        })->await();

        expect($result)->toBe('done');

        $count = PG::fetchValue('SELECT COUNT(*) FROM test_pg')->await();
        expect($count)->toBe('1');
    });

    it('delegates onCommit to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        PG::execute('CREATE TEMPORARY TABLE test_pg (value INT)')->await();

        $tracker = new class () {
            public bool $called = false;
        };

        PG::transaction(function ($conn) use ($tracker) {
            pg_query($conn, 'INSERT INTO test_pg VALUES (1)');
            PG::onCommit(function () use ($tracker) {
                $tracker->called = true;
            });
        })->await();

        expect($tracker->called)->toBeTrue();
    });

    it('delegates onRollback to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        PG::execute('CREATE TEMPORARY TABLE test_pg (value INT)')->await();

        $tracker = new class () {
            public bool $called = false;
        };

        try {
            PG::transaction(function ($conn) use ($tracker) {
                pg_query($conn, 'INSERT INTO test_pg VALUES (1)');
                PG::onRollback(function () use ($tracker) {
                    $tracker->called = true;
                });

                throw new Exception('Force rollback');
            })->await();
        } catch (Hibla\Postgres\Exceptions\TransactionFailedException $e) {
            // Expected
        }

        expect($tracker->called)->toBeTrue();
    });

    it('delegates run to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        $result = PG::run(function ($conn) {
            return pg_parameter_status($conn, 'server_version');
        })->await();

        expect($result)->toBeString();
    });

    it('delegates getLastConnection to underlying instance', function () {
        PG::init(TestHelper::getTestConfig());

        expect(PG::getLastConnection())->toBeNull();

        PG::query('SELECT 1')->await();

        expect(PG::getLastConnection())->toBeInstanceOf(PgSql\Connection::class);
    });

    it('resets properly and becomes unusable', function () {
        PG::init(TestHelper::getTestConfig());

        PG::query('SELECT 1')->await();

        PG::reset();

        expect(fn () => PG::query('SELECT 1')->await())
            ->toThrow(NotInitializedException::class)
        ;
    });
});
