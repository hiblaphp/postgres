<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\Internals\RowStream;
use Hibla\Promise\Exceptions\CancelledException;

use function Hibla\async;
use function Hibla\await;
use function Hibla\delay;


describe('Query Cancellation', function (): void {

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

            Hibla\EventLoop\Loop::addTimer(0.05, function () use ($slow) {
                $slow->cancel();
            });

            $conn->close();

            expect($conn->isClosed())->toBeTrue();
        });
    });

    it('cancels a long-running query and throws CancelledException', function (): void {
        $conn = pgConnWith();
        $startTime = microtime(true);

        $queryPromise = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn() => await($queryPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(1.5);

        $conn->close();
    });

    it('marks the connection as cancelled after a query is cancelled', function (): void {
        $conn = pgConnWith();

        $queryPromise = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn() => await($queryPromise))
            ->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $conn->close();
    });

    it('can clear the cancelled flag after cancellation', function (): void {
        $conn = pgConnWith();

        $queryPromise = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn() => await($queryPromise))
            ->toThrow(CancelledException::class);

        $conn->clearCancelledFlag();

        expect($conn->wasQueryCancelled())->toBeFalse();

        $conn->close();
    });

    it('recovers and reuses the connection after a query is cancelled', function (): void {
        $conn = pgConnWith();

        $queryPromise = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($queryPromise): void {
            $queryPromise->cancel();
        });

        expect(fn() => await($queryPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });

    it('handles multiple sequential cancellations on the same connection', function (): void {
        $conn = pgConnWith();

        for ($i = 0; $i < 3; $i++) {
            $slow = $conn->query('SELECT pg_sleep(2)');

            Loop::addTimer(0.1, fn() => $slow->cancel());

            expect(fn() => await($slow))->toThrow(CancelledException::class);

            if ($conn->wasQueryCancelled()) {
                awaitCancelDrain($conn);
            }
        }

        $result = await($conn->query('SELECT 42 AS val'));

        expect((int) $result->fetchOne()['val'])->toBe(42)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

describe('Prepared Statement Cancellation', function (): void {

    it('cancels a long-running prepared statement execution', function (): void {
        $conn = pgConnWith();
        $startTime = microtime(true);

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $execPromise = $conn->executeStatement($stmt, [2]);

        Loop::addTimer(0.3, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn() => await($execPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(1.5);

        $conn->close();
    });

    it('marks the connection as cancelled after a prepared statement is cancelled', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $execPromise = $conn->executeStatement($stmt, [2]);

        Loop::addTimer(0.3, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn() => await($execPromise))
            ->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $conn->close();
    });

    it('recovers and reuses the connection after a prepared statement is cancelled', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $execPromise = $conn->executeStatement($stmt, [2]);

        Loop::addTimer(0.3, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn() => await($execPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
        ;

        $conn->close();
    });

    it('can reuse a prepared statement after its execution is cancelled', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $execPromise = $conn->executeStatement($stmt, [2]);

        Loop::addTimer(0.3, function () use ($execPromise): void {
            $execPromise->cancel();
        });

        expect(fn() => await($execPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        // Reuse the same prepared statement handle — DEALLOCATE was not sent.
        $result = await($conn->executeStatement($stmt, [0]));

        expect($result)->not->toBeNull()
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

// ── Stream Query Cancellation ─────────────────────────────────────────────────

describe('Stream Query Cancellation', function (): void {

    it('cancels a streaming query before the first row arrives and throws CancelledException', function (): void {
        $conn = pgConnWith();
        $startTime = microtime(true);

        $streamPromise = $conn->streamQuery('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(1.5);

        $conn->close();
    });

    it('marks the connection as cancelled after a stream is cancelled before the first row', function (): void {
        $conn = pgConnWith();

        $streamPromise = $conn->streamQuery('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $conn->close();
    });

    it('recovers and reuses the connection after a stream is cancelled before the first row', function (): void {
        $conn = pgConnWith();

        $streamPromise = $conn->streamQuery('SELECT pg_sleep(2)');

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
        ;

        $conn->close();
    });

    it('cancels mid-iteration via break and stream reports as cancelled', function (): void {
        $conn = pgConnWith();

        $stream = await($conn->streamQuery(twentyRowPgSql()));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;

        foreach ($stream as $row) {
            $rowsReceived++;

            if ($rowsReceived >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($rowsReceived)->toBe(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        $conn->close();
    });

    test('wasQueryCancelled is false when cancelling a fully-buffered stream mid-iteration', function (): void {
        $conn = pgConnWith();

        // generate_series(1,20) returns all rows in one result set — the entire
        // result is buffered before iteration starts, so cancelling mid-loop
        // never triggers a server-side cancel.
        $stream = await($conn->streamQuery(twentyRowPgSql()));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        expect($conn->wasQueryCancelled())->toBeFalse();

        $conn->close();
    });

    test('connection remains healthy after cancelling a fully-buffered stream mid-iteration', function (): void {
        $conn = pgConnWith();

        $stream = await($conn->streamQuery(twentyRowPgSql()));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $conn->close();
    });

    it('cancels mid-iteration via async timer between rows and throws CancelledException', function (): void {
        $conn = pgConnWith();

        $stream = await($conn->streamQuery(twentyRowPgSql()));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;
        $cancelFired = false;

        expect(function () use ($stream, &$rowsReceived, &$cancelFired): void {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        })->toThrow(CancelledException::class);

        expect($rowsReceived)->toBeGreaterThanOrEqual(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        $conn->close();
    });

    test('connection remains healthy after cancelling a stream via async timer between rows', function (): void {
        $conn = pgConnWith();

        $stream = await($conn->streamQuery(twentyRowPgSql()));

        $rowsReceived = 0;
        $cancelFired = false;

        try {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        } catch (CancelledException) {
            // expected — fall through to health check
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $conn->close();
    });
});

// ── Execute Stream Cancellation ───────────────────────────────────────────────

describe('Execute Stream Cancellation', function (): void {

    it('cancels executeStatementStream before the first row arrives and throws CancelledException', function (): void {
        $conn = pgConnWith();
        $startTime = microtime(true);

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $streamPromise = $conn->executeStatementStream($stmt, [2]);

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect(round(microtime(true) - $startTime, 2))->toBeLessThan(1.5);

        $conn->close();
    });

    it('marks the connection as cancelled after executeStatementStream is cancelled before first row', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $streamPromise = $conn->executeStatementStream($stmt, [2]);

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $conn->close();
    });

    it('recovers and reuses the connection after executeStatementStream is cancelled before first row', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $streamPromise = $conn->executeStatementStream($stmt, [2]);

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive')
            ->and($conn->wasQueryCancelled())->toBeFalse()
        ;

        $conn->close();
    });

    it('cancels executeStatementStream mid-iteration via break and stream reports as cancelled', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare(twentyRowPgPreparedSql()));
        $stream = await($conn->executeStatementStream($stmt, [20]));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;

        foreach ($stream as $row) {
            $rowsReceived++;

            if ($rowsReceived >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($rowsReceived)->toBe(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        $conn->close();
    });

    test('wasQueryCancelled is false when cancelling a fully-buffered executeStatementStream mid-iteration', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare(twentyRowPgPreparedSql()));
        $stream = await($conn->executeStatementStream($stmt, [20]));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        expect($conn->wasQueryCancelled())->toBeFalse();

        $conn->close();
    });

    test('connection remains healthy after cancelling a fully-buffered executeStatementStream mid-iteration', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare(twentyRowPgPreparedSql()));
        $stream = await($conn->executeStatementStream($stmt, [20]));

        foreach ($stream as $row) {
            $stream->cancel();

            break;
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $conn->close();
    });

    it('cancels executeStatementStream mid-iteration via async timer between rows and throws CancelledException', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare(twentyRowPgPreparedSql()));
        $stream = await($conn->executeStatementStream($stmt, [20]));

        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowsReceived = 0;
        $cancelFired = false;

        expect(function () use ($stream, &$rowsReceived, &$cancelFired): void {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        })->toThrow(CancelledException::class);

        expect($rowsReceived)->toBeGreaterThanOrEqual(3)
            ->and($stream->isCancelled())->toBeTrue()
        ;

        $conn->close();
    });

    test('connection remains healthy after cancelling executeStatementStream via async timer between rows', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare(twentyRowPgPreparedSql()));
        $stream = await($conn->executeStatementStream($stmt, [20]));

        $rowsReceived = 0;
        $cancelFired = false;

        try {
            foreach ($stream as $row) {
                $rowsReceived++;

                if ($rowsReceived === 3 && ! $cancelFired) {
                    $cancelFired = true;
                    async(function () use ($stream): void {
                        $stream->cancel();
                    });
                }

                await(delay(0));
            }
        } catch (CancelledException) {
            // expected — fall through to health check
        }

        $result = await($conn->query("SELECT 'Alive' AS status"));

        expect($result->fetchOne()['status'])->toBe('Alive');

        $conn->close();
    });

    it('can reuse a prepared statement after executeStatementStream is cancelled', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $streamPromise = $conn->executeStatementStream($stmt, [2]);

        Loop::addTimer(0.3, function () use ($streamPromise): void {
            $streamPromise->cancel();
        });

        expect(fn() => await($streamPromise))
            ->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $verifyStmt = await($conn->prepare('SELECT $1::int AS result'));
        $stream = await($conn->executeStatementStream($verifyStmt, [42]));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(1)
            ->and((int) $rows[0]['result'])->toBe(42)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });

    test('connection is fully healthy after all executeStatementStream cancellation scenarios', function (): void {
        $conn = pgConnWith();

        $result = await($conn->query("SELECT 'AllGreen' AS status"));

        expect($result->fetchOne()['status'])->toBe('AllGreen');

        $conn->close();
    });
});

describe('Cancellation Strategy (Opt-out)', function (): void {

    it('closes the connection entirely when server-side cancellation is disabled', function (): void {
        $conn = pgConnWith(enableServerSideCancellation: false);

        try {
            $promise = $conn->query('SELECT pg_sleep(2)');
            $promise->cancel();

            expect($conn->isClosed())->toBeTrue();
        } finally {
            $conn->close();
        }
    });

    it('does not set wasQueryCancelled when server-side cancellation is disabled', function (): void {
        $conn = pgConnWith(enableServerSideCancellation: false);

        try {
            $promise = $conn->query('SELECT pg_sleep(2)');
            $promise->cancel();

            expect($conn->wasQueryCancelled())->toBeFalse();
        } finally {
            $conn->close();
        }
    });
});

describe('No-Drain Recovery (Postgres-specific)', function (): void {

    it('does not need a scrub query — ping alone is sufficient as a drain barrier', function (): void {
        $conn = pgConnWith();

        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn() => $slow->cancel());

        expect(fn() => await($slow))->toThrow(CancelledException::class);

        await($conn->ping());
        $conn->clearCancelledFlag();

        $result = await($conn->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });

    it('enqueuing a query immediately after cancel resolves once the drain completes', function (): void {
        $conn = pgConnWith();

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn() => $slow->cancel());

        $follow = $conn->query('SELECT 99 AS val');

        expect(fn() => await($slow))->toThrow(CancelledException::class);

        $result = await($follow);

        expect((int) $result->fetchOne()['val'])->toBe(99)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});
