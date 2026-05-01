<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Internals\Connection;
use Hibla\Promise\Exceptions\CancelledException;

use function Hibla\await;

describe('Active Query Cancellation (Server-Side)', function () {

    it('aborts a running query using a side-channel and preserves the connection', function () {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        try {
            $slowPromise = $conn->query('SELECT pg_sleep(5)');

            Loop::addTimer(0.1, function () use ($slowPromise) {
                $slowPromise->cancel();
            });

            $fastPromise = $conn->query('SELECT 42 AS val');

            $start = microtime(true);
            $result = await($fastPromise);
            $duration = microtime(true) - $start;

            expect((int) $result->fetchOne()['val'])->toBe(42);
            expect($duration)->toBeLessThan(1.0);
            expect($slowPromise->isCancelled())->toBeTrue();
            expect($conn->isReady())->toBeTrue();
        } finally {
            $conn->close();
        }
    });

    it('handles multiple cancellations on the same connection sequentially', function () {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        try {
            for ($i = 0; $i < 2; $i++) {
                $slow = $conn->query('SELECT pg_sleep(2)');
                Loop::addTimer(0.05, fn() => $slow->cancel());

                $check = await($conn->query('SELECT 1 AS ok'));
                expect((int) $check->fetchOne()['ok'])->toBe(1);
            }
        } finally {
            $conn->close();
        }
    });
});

describe('Queued Command Cancellation', function () {

    it('removes a command from the local queue if cancelled before it starts', function () {
        $conn = pgConn();

        try {
            $slow = $conn->query('SELECT pg_sleep(0.5)');
            
            $queued = $conn->query('SELECT 99 AS val');
            
            $queued->cancel();

            await($slow);

            expect($queued->isCancelled())->toBeTrue();

            $next = await($conn->query('SELECT 1 AS ok'));
            expect((int) $next->fetchOne()['ok'])->toBe(1);
        } finally {
            $conn->close();
        }
    });
});

describe('Cancellation Strategy (Opt-out)', function () {

    it('closes the connection entirely when server-side cancellation is disabled', function () {
        $config = pgConfig()->withQueryCancellation(false);
        $conn = await(Connection::create($config));

        try {
            $promise = $conn->query('SELECT pg_sleep(2)');
            
            $promise->cancel();

            expect($conn->isClosed())->toBeTrue();
        } finally {
            $conn->close();
        }
    });
});

describe('Destructor and Teardown Safety', function () {

    it('waits for pending side-channel cancels during close()', function () {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        $slow = $conn->query('SELECT pg_sleep(2)');
        
        Hibla\EventLoop\Loop::addTimer(0.05, function() use ($slow) {
            $slow->cancel();
        });

        $conn->close();

        expect($conn->isClosed())->toBeTrue();
    });
});