<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Exceptions\PoolException;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Sql\Exceptions\TimeoutException;

use function Hibla\await;
use function Hibla\delay;

describe('Waiter Lifecycle During Drain', function (): void {

    it('does not resolve a waiter that was cancelled before the drain completed', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $waiter = $pool->get();
        $waiter->cancel();

        expect($waiter->isCancelled())->toBeTrue();

        await(delay(0.5));

        expect($pool->stats['draining_connections'])->toBe(0)
            ->and($pool->stats['pooled_connections'])->toBe(1)
        ;

        $pool->close();
    });

    it('serves the second waiter when the first waiter times out before drain completes', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true, acquireTimeout: 0.05);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $timedOut = $pool->get();

        try {
            await($timedOut);
        } catch (Throwable $e) {
            expect($e)->toBeInstanceOf(TimeoutException::class);
        }

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->getProcessId())->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('queues multiple waiters and satisfies them all from successive releases of the same drained connection', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $w1 = $pool->get();
        $w2 = $pool->get();
        $w3 = $pool->get();

        $c1 = await($w1);
        $pool->release($c1);

        $c2 = await($w2);
        $pool->release($c2);

        $c3 = await($w3);
        $pool->release($c3);

        expect($c1->getProcessId())->toBe($conn->getProcessId())
            ->and($c2->getProcessId())->toBe($conn->getProcessId())
            ->and($c3->getProcessId())->toBe($conn->getProcessId())
        ;

        $pool->close();
    });
});

describe('Double Release / Post-Close Release', function (): void {

    it('does not corrupt pool state when the same connection is released twice', function (): void {
        $pool = makePool(maxSize: 2, enableServerSideCancellation: true);

        $conn = await($pool->get());
        await($conn->query('SELECT 1'));

        $pool->release($conn);

        $pool->release($conn);

        $conn2 = await($pool->get());
        $result = await($conn2->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $pool->release($conn2);
        $pool->close();
    });

    it('does not throw when releasing a connection after the pool is force-closed', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $pool->close();

        $pool->release($conn);
        expect(true)->toBeTrue();
    });
});

describe('Shutdown Races', function (): void {

    it('closeAsync() with a timeout falls back to force close and resolves the promise', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());

        $resolved = false;
        $shutdown = $pool->closeAsync(timeout: 0.1)->then(function () use (&$resolved): void {
            $resolved = true;
        });

        await($shutdown);

        expect($resolved)->toBeTrue()
            ->and($pool->stats['active_connections'])->toBe(0)
        ;
    });

    it('close() called while closeAsync() is pending resolves the shutdown promise immediately', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());

        $resolved = false;

        $pool->closeAsync()->then(function () use (&$resolved): void {
            $resolved = true;
        });

        await(delay(0));
        expect($resolved)->toBeFalse();

        $pool->close();

        await(delay(0));

        expect($resolved)->toBeTrue();
    });

    it('get() is rejected immediately after closeAsync() even if connections are still draining', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);
        $pool->closeAsync();

        expect(fn () => await($pool->get()))->toThrow(PoolException::class);
    });

    it('pending waiters are rejected when closeAsync() is called', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());

        $waiter = $pool->get();

        $pool->closeAsync();

        expect(fn () => await($waiter))->toThrow(PoolException::class);

        $pool->release($conn);
        $pool->close();
    });

    it('maxWaiters limit is respected while a drain is in progress', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true, maxWaiters: 1);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $w1 = $pool->get();
        expect($w1->isPending())->toBeTrue();

        expect(fn () => await($pool->get()))->toThrow(PoolException::class);

        $conn2 = await($w1);
        $pool->release($conn2);
        $pool->close();
    });
});

describe('Cancellation of Non-Streaming Prepared Statement Execution', function (): void {

    it('routes a cancelled executeStatement through drainAndRelease and reuses the connection', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);
        $conn = await($pool->get());

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $exec = $conn->executeStatement($stmt, [2]);

        Loop::addTimer(0.1, fn () => $exec->cancel());
        expect(fn () => await($exec))->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());
        $result = await($conn2->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn2->getProcessId())->toBe($conn->getProcessId())
            ->and($conn2->wasQueryCancelled())->toBeFalse()
        ;

        $pool->release($conn2);
        $pool->close();
    });
});

describe('onConnect Hook Interaction with Drain/Reset', function (): void {

    it('re-runs the onConnect hook after a drain+reset cycle', function (): void {
        $hookRunCount = 0;

        $pool = makePool(
            maxSize: 1,
            enableServerSideCancellation: true,
            resetConnection: true,
            onConnect: function () use (&$hookRunCount): void {
                $hookRunCount++;
            },
        );

        $conn = await($pool->get());
        expect($hookRunCount)->toBe(1);

        $slow = $conn->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $conn2 = await($pool->get());
        expect($hookRunCount)->toBe(2);

        $pool->release($conn2);
        $pool->close();
    });

    it('drops the connection and satisfies the next waiter when the onConnect hook fails after a reset', function (): void {
        $callCount = 0;

        $pool = makePool(
            maxSize: 1,
            enableServerSideCancellation: true,
            resetConnection: true,
            onConnect: function () use (&$callCount): void {
                $callCount++;

                if ($callCount === 2) {
                    throw new RuntimeException('hook failed');
                }
            },
        );

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->getProcessId())->not->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Health Check Interaction with Drain', function (): void {

    it('healthCheck() does not include a draining connection in its results', function (): void {
        $pool = makePool(maxSize: 2, enableServerSideCancellation: true);

        $idle = await($pool->get());
        $active = await($pool->get());

        await($idle->query('SELECT 1'));
        $pool->release($idle);

        $slow = $active->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($active);

        expect($pool->stats['draining_connections'])->toBe(1);

        $stats = await($pool->healthCheck());

        expect($stats['total_checked'])->toBe(1)
            ->and($stats['healthy'])->toBe(1)
        ;

        await($pool->get());
        $pool->close();
    });
});

describe('minSize Replenishment After Cancellation', function (): void {

    it('respawns a connection to satisfy minSize after a cancelled connection is dropped by opt-out', function (): void {
        $pool = makePool(maxSize: 2, minSize: 1, enableServerSideCancellation: false);

        await(delay(0.1));

        expect($pool->stats['pooled_connections'])->toBe(1);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        $slow->cancel();
        expect($conn->isClosed())->toBeTrue();

        $pool->release($conn);

        await(delay(0.2));

        expect($pool->stats['active_connections'])->toBeGreaterThanOrEqual(1);

        $pool->close();
    });
});
