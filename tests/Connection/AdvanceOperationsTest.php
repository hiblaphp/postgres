<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Internals\Connection;

use function Hibla\await;

describe('Transactions', function () {
    it('successfully commits a transaction', function () {
        $conn = pgConn();

        try {
            await($conn->query('BEGIN'));
            await($conn->query('CREATE TEMP TABLE tx_test (id INT)'));
            await($conn->query('INSERT INTO tx_test VALUES (1)'));
            await($conn->query('COMMIT'));

            // query() returns a Promise resolving to a Result object.
            // We await the result, THEN call fetchOne() on that result.
            $result = await($conn->query('SELECT count(*) AS total FROM tx_test'));
            $row = $result->fetchOne();

            expect((int) $row['total'])->toBe(1);
        } finally {
            $conn->close();
        }
    });

    it('successfully rolls back a transaction', function () {
        $conn = pgConn();

        try {
            await($conn->query('CREATE TEMP TABLE tx_test_rb (id INT)'));

            await($conn->query('BEGIN'));
            await($conn->query('INSERT INTO tx_test_rb VALUES (1)'));
            await($conn->query('ROLLBACK'));

            $result = await($conn->query('SELECT count(*) AS total FROM tx_test_rb'));
            $row = $result->fetchOne();

            expect((int) $row['total'])->toBe(0);
        } finally {
            $conn->close();
        }
    });
});

describe('Stream Backpressure', function () {
    it('handles backpressure correctly by pausing and resuming the stream', function () {
        $conn = pgConn();

        try {
            $bufferSize = 10;
            $totalRows = 50;

            $stream = await($conn->streamQuery(
                "SELECT n FROM generate_series(1, {$totalRows}) AS n",
                $bufferSize
            ));

            $count = 0;
            foreach ($stream as $row) {
                $count++;
                if ($count === 15) {
                    usleep(10000);
                }
            }

            expect($count)->toBe($totalRows);
        } finally {
            $conn->close();
        }
    });
});

describe('Cancellation', function () {

    it('cancels a running query using the internal async side-channel and executes the next queue item', function () {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        try {
            $slowPromise = $conn->query('SELECT pg_sleep(2)');

            Loop::addTimer(0.1, function () use ($slowPromise) {
                $slowPromise->cancel();
            });

            $fastPromise = $conn->query('SELECT 1 AS alive');

            $start = microtime(true);
            $result = await($fastPromise); 
            $duration = microtime(true) - $start;

            $row = $result->fetchOne();

            expect((int) $row['alive'])->toBe(1);
            expect($duration)->toBeLessThan(1.0);
            expect($slowPromise->isCancelled())->toBeTrue();

        } finally {
            $conn->close();
        }
    });

});
