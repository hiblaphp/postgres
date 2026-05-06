<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Internals\Connection;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Sql\Exceptions\QueryException;

use function Hibla\await;
use function Hibla\delay;

describe('Prepare Command Cancellation', function (): void {

    it('cancels a queued PREPARE before it starts executing', function (): void {
        $conn = pgConnWith();

        $slow = $conn->query('SELECT pg_sleep(0.5)');
        $preparePromise = $conn->prepare('SELECT $1::int AS val');

        $preparePromise->cancel();

        await($slow);

        expect($preparePromise->isCancelled())->toBeTrue()
            ->and($conn->isReady())->toBeTrue()
        ;

        $result = await($conn->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $conn->close();
    });

    it('leaves the connection healthy when a running PREPARE is cancelled with server-side cancellation', function (): void {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        $slow = $conn->query('SELECT pg_sleep(1)');

        $preparePromise = $conn->prepare('SELECT pg_sleep($1)');

        Loop::addTimer(0.05, fn () => $slow->cancel());

        $preparePromise->cancel();

        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

describe('Cancel Idempotency', function (): void {

    it('calling cancel() twice on the same promise is a no-op', function (): void {
        $conn = pgConnWith();

        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, function () use ($slow): void {
            $slow->cancel();
            $slow->cancel();
        });

        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $conn->close();
    });

    it('calling cancel() on an already-resolved promise is a no-op', function (): void {
        $conn = pgConnWith();

        $promise = $conn->query('SELECT 1 AS ok');
        await($promise);

        $promise->cancel();

        expect($promise->isCancelled())->toBeFalse()
            ->and($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $result2 = await($conn->query('SELECT 2 AS ok'));
        expect((int) $result2->fetchOne()['ok'])->toBe(2);

        $conn->close();
    });

    it('calling cancel() on an already-rejected promise is a no-op', function (): void {
        $conn = pgConnWith();

        $promise = $conn->query('SELECT 1/0');
        $promise->catch(static function (): void {
        });

        expect(fn () => await($promise))->toThrow(QueryException::class);
        $promise->cancel();

        expect($promise->isCancelled())->toBeFalse()
            ->and($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

// ── Middle-of-Queue Cancellation ─────────────────────────────────────────────

describe('Middle-of-Queue Cancellation', function (): void {

    it('removes only the middle command from a three-command queue', function (): void {
        $conn = pgConnWith();

        $q1 = $conn->query('SELECT 1 AS val');
        $q2 = $conn->query('SELECT 2 AS val'); // will be cancelled
        $q3 = $conn->query('SELECT 3 AS val');

        $q2->cancel();

        $r1 = await($q1);
        $r3 = await($q3);

        expect((int) $r1->fetchOne()['val'])->toBe(1)
            ->and($q2->isCancelled())->toBeTrue()
            ->and((int) $r3->fetchOne()['val'])->toBe(3)
            ->and($conn->wasQueryCancelled())->toBeFalse() // never hit the wire
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });

    it('cancelling all queued commands while one is active leaves the connection clean', function (): void {
        $conn = pgConnWith();

        $active = $conn->query('SELECT pg_sleep(0.3)');
        $q1 = $conn->query('SELECT 1 AS val');
        $q2 = $conn->query('SELECT 2 AS val');
        $q3 = $conn->query('SELECT 3 AS val');

        $q1->cancel();
        $q2->cancel();
        $q3->cancel();

        await($active);

        expect($q1->isCancelled())->toBeTrue()
            ->and($q2->isCancelled())->toBeTrue()
            ->and($q3->isCancelled())->toBeTrue()
            ->and($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $result = await($conn->query('SELECT 42 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(42);

        $conn->close();
    });
});

// ── Transaction State After Cancellation ─────────────────────────────────────

describe('Transaction State After Cancellation', function (): void {

    it('leaves no open transaction after a query inside BEGIN is cancelled', function (): void {
        $conn = pgConnWith();

        await($conn->query('BEGIN'));

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        // PostgreSQL cancels the statement but keeps the transaction open in
        // error state.  A ROLLBACK is required before reuse.
        await($conn->query('ROLLBACK'));

        expect($conn->getTransactionStatus())->toBe(PGSQL_TRANSACTION_IDLE)
            ->and($conn->isReady())->toBeTrue()
        ;

        $result = await($conn->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $conn->close();
    });

    it('getTransactionStatus() does not return INERROR after cancel drain and rollback', function (): void {
        $conn = pgConnWith();

        await($conn->query('BEGIN'));
        await($conn->query("SET LOCAL application_name = 'tx_test'"));

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        // After a cancel inside a transaction the status must be INERROR.
        expect($conn->getTransactionStatus())->toBe(PGSQL_TRANSACTION_INERROR);

        await($conn->query('ROLLBACK'));

        expect($conn->getTransactionStatus())->toBe(PGSQL_TRANSACTION_IDLE);

        $conn->close();
    });

    it('reset() rolls back an open transaction automatically after cancellation', function (): void {
        $conn = pgConnWith();

        await($conn->query('BEGIN'));

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        // Connection::reset() issues ROLLBACK automatically when INERROR, then
        // DISCARD ALL — no manual ROLLBACK needed from the caller.
        await($conn->reset());

        expect($conn->getTransactionStatus())->toBe(PGSQL_TRANSACTION_IDLE)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

describe('Cancel Timer After Close', function (): void {

    it('does not crash when the cancel timer fires after the connection is already closed', function (): void {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        $slow = $conn->query('SELECT pg_sleep(5)');

        Loop::addTimer(0.05, fn () => $conn->close());
        Loop::addTimer(0.20, fn () => $slow->cancel());

        $threw = false;

        try {
            await($slow);
        } catch (Throwable) {
            $threw = true;
        }

        expect($threw)->toBeTrue()
            ->and($conn->isClosed())->toBeTrue()
        ;

        await(delay(0.3));
    });

    it('does not crash when close() is called while pg_cancel_backend is in-flight', function (): void {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        $slow = $conn->query('SELECT pg_sleep(5)');

        Loop::addTimer(0.05, function () use ($slow, $conn): void {
            $slow->cancel();
            Loop::addTimer(0.001, fn () => $conn->close());
        });

        $threw = false;

        try {
            await($slow);
        } catch (Throwable) {
            $threw = true;
        }

        expect($threw)->toBeTrue();

        await(delay(0.3));

        expect($conn->isClosed())->toBeTrue();
    });
});

describe('Statement Handle Integrity After Cancellation', function (): void {

    it('a prepared statement prepared before a cancel can still be executed after drain', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT $1::int AS val'));

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->executeStatement($stmt, [99]));
        expect((int) $result->fetchOne()['val'])->toBe(99);

        $conn->close();
    });

    it('closeStatement() after cancel drain deallocates the handle cleanly', function (): void {
        $conn = pgConnWith();

        $stmt = await($conn->prepare('SELECT $1::int AS val'));

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        await($conn->closeStatement($stmt->name));

        expect($conn->isReady())->toBeTrue();

        expect(fn () => await($conn->executeStatement($stmt, [1])))
            ->toThrow(QueryException::class)
        ;

        $conn->close();
    });
});

describe('Large Result Set Cancellation', function (): void {

    it('cancels a query that returns many rows mid-transfer and recovers', function (): void {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));

        $sql = 'SELECT pg_sleep(0.001) FROM generate_series(1, 500)';

        $promise = $conn->query($sql);
        Loop::addTimer(0.05, fn () => $promise->cancel());

        $threw = false;

        try {
            await($promise);
        } catch (CancelledException) {
            $threw = true;
        }

        expect($threw)->toBeTrue();

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });

    it('cancels a streaming query that returns many rows mid-transfer and recovers', function (): void {
        $config = pgConfig()->withQueryCancellation(true);
        $conn = await(Connection::create($config));
        $sql = 'SELECT pg_sleep(0.001) FROM generate_series(1, 500)';

        $stream = await($conn->streamQuery($sql));

        Loop::addTimer(0.05, fn () => $stream->cancel());

        $threw = false;

        try {
            foreach ($stream as $row) {
                // Do nothing, just consume rows from the socket
            }
        } catch (CancelledException) {
            $threw = true;
        }

        expect($threw)->toBeTrue();

        if ($conn->wasQueryCancelled()) {
            awaitCancelDrain($conn);
        }

        $result = await($conn->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

describe('wasQueryCancelled Flag Lifecycle', function (): void {

    it('wasQueryCancelled remains true while the drain ping is queued but not yet resolved', function (): void {
        $conn = pgConnWith();

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $follow = $conn->query('SELECT 1 AS ok');

        expect($conn->wasQueryCancelled())->toBeTrue();

        await($follow);

        expect($conn->isReady())->toBeTrue();

        $conn->close();
    });

    it('clearCancelledFlag() has no effect on a connection that was never cancelled', function (): void {
        $conn = pgConnWith();

        await($conn->query('SELECT 1'));

        $conn->clearCancelledFlag();

        expect($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });

    it('wasQueryCancelled is false when a slow query errors naturally without cancellation', function (): void {
        $conn = pgConnWith();

        $promise = $conn->query('SELECT 1/0');
        $promise->catch(static function (): void {
        });

        expect(fn () => await($promise))->toThrow(QueryException::class);

        expect($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $conn->close();
    });
});

describe('Ping Cancellation', function (): void {

    it('cancels a queued ping before it executes without corrupting the connection', function (): void {
        $conn = pgConnWith();

        $slow = $conn->query('SELECT pg_sleep(0.3)');

        $pingPromise = $conn->ping();
        $pingPromise->cancel();

        await($slow);

        expect($pingPromise->isCancelled())->toBeTrue()
            ->and($conn->wasQueryCancelled())->toBeFalse()
            ->and($conn->isReady())->toBeTrue()
        ;

        $ok = await($conn->ping());
        expect($ok)->toBeTrue();

        $conn->close();
    });
});
