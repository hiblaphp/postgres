<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Promise\Exceptions\CancelledException;

use function Hibla\await;
use function Hibla\delay;

describe('Query Cancellation', function (): void {

    it('routes a cancelled connection through drainAndRelease before reuse', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());

        expect(fn () => await($slow))->toThrow(CancelledException::class);

        expect($conn->wasQueryCancelled())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->wasQueryCancelled())->toBeFalse()
            ->and($conn2->getProcessId())->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('clears the wasQueryCancelled flag after the drain cycle', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->wasQueryCancelled())->toBeFalse();

        $pool->release($conn2);
        $pool->close();
    });

    it('tracks the draining connection in stats during the drain cycle', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        expect($pool->stats['draining_connections'])->toBe(1);

        $conn2 = await($pool->get());

        expect($pool->stats['draining_connections'])->toBe(0);

        $pool->release($conn2);
        $pool->close();
    });

    it('handles multiple sequential query cancellations on the same pooled connection', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        for ($i = 0; $i < 3; $i++) {
            $conn = await($pool->get());
            $slow = $conn->query('SELECT pg_sleep(2)');

            Loop::addTimer(0.1, fn () => $slow->cancel());
            expect(fn () => await($slow))->toThrow(CancelledException::class);

            $pool->release($conn);
        }

        $conn = await($pool->get());
        $result = await($conn->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn->isReady())->toBeTrue()
        ;

        $pool->release($conn);
        $pool->close();
    });

    it('serves a waiter from the draining connection once the drain completes', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);
        $waiter = $pool->get();

        expect($waiter->isPending())->toBeTrue();

        $conn2 = await($waiter);

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->getProcessId())->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('handles concurrent cancellations across multiple connections', function (): void {
        $pool = makePool(maxSize: 3, enableServerSideCancellation: true);
        $conns = [await($pool->get()), await($pool->get()), await($pool->get())];
        $slows = array_map(fn ($c) => $c->query('SELECT pg_sleep(2)'), $conns);

        Loop::addTimer(0.1, function () use ($slows): void {
            foreach ($slows as $s) {
                $s->cancel();
            }
        });

        foreach ($slows as $slow) {
            try {
                await($slow);
            } catch (CancelledException) {
            }
        }

        foreach ($conns as $conn) {
            $pool->release($conn);
        }

        $recovered = [];
        for ($i = 0; $i < 3; $i++) {
            $c = await($pool->get());
            $recovered[] = $c;
            expect($c->isReady())->toBeTrue()
                ->and($c->wasQueryCancelled())->toBeFalse()
            ;
        }

        foreach ($recovered as $c) {
            $pool->release($c);
        }

        $pool->close();
    });
});

describe('Cancellation Opt-out', function (): void {

    it('removes a closed connection and spawns a fresh one on next borrow', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: false);

        $conn = await($pool->get());
        $pid = $conn->getProcessId();
        $slow = $conn->query('SELECT pg_sleep(2)');

        $slow->cancel();

        expect($conn->isClosed())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->getProcessId())->not->toBe($pid)
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('does not set the draining flag when opt-out closes the connection', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: false);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');
        $slow->cancel();

        $pool->release($conn);

        expect($pool->stats['draining_connections'])->toBe(0);

        $pool->close();
    });

    it('serves a queued waiter with a fresh connection after opt-out close', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: false);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');
        $waiter = $pool->get();

        $slow->cancel();
        $pool->release($conn);

        $conn2 = await($waiter);

        expect($conn2->isReady())->toBeTrue();

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Stream Query Cancellation', function (): void {

    it('routes a cancelled stream through drainAndRelease and reuses the connection', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $streamPromise = $conn->streamQuery('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $streamPromise->cancel());

        expect(fn () => await($streamPromise))->toThrow(CancelledException::class);
        expect($conn->wasQueryCancelled())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->wasQueryCancelled())->toBeFalse()
            ->and($conn2->getProcessId())->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('reuses the connection after a stream is cancelled mid-iteration', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);
        $conn = await($pool->get());
        $stream = await($conn->streamQuery(twentyRowPgSql()));

        $rowsRead = 0;
        foreach ($stream as $row) {
            $rowsRead++;
            if ($rowsRead >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($stream->isCancelled())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue();

        $result = await($conn2->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $pool->release($conn2);
        $pool->close();
    });

    it('pool remains healthy after a stream completes naturally and cancel is called on it', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);
        $conn = await($pool->get());
        $stream = await($conn->streamQuery(twentyRowPgSql()));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }
        $stream->cancel();

        expect($conn->wasQueryCancelled())->toBeFalse();

        $pool->release($conn);

        expect($pool->stats['draining_connections'])->toBe(0);

        $conn2 = await($pool->get());
        $result = await($conn2->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Execute Stream Cancellation', function (): void {

    it('routes a cancelled executeStatementStream through drainAndRelease', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);
        $conn = await($pool->get());

        $stmt = await($conn->prepare('SELECT pg_sleep($1)'));
        $streamPromise = $conn->executeStatementStream($stmt, [2]);

        Loop::addTimer(0.1, fn () => $streamPromise->cancel());

        expect(fn () => await($streamPromise))->toThrow(CancelledException::class);
        expect($conn->wasQueryCancelled())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->wasQueryCancelled())->toBeFalse()
            ->and($conn2->getProcessId())->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('reuses the connection after executeStatementStream is cancelled mid-iteration', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);
        $conn = await($pool->get());
        $stmt = await($conn->prepare(twentyRowPgPreparedSql()));
        $stream = await($conn->executeStatementStream($stmt, [20]));

        $rowsRead = 0;
        foreach ($stream as $row) {
            $rowsRead++;
            if ($rowsRead >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($stream->isCancelled())->toBeTrue();

        $pool->release($conn);

        $conn2 = await($pool->get());
        $result = await($conn2->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1)
            ->and($conn2->isReady())->toBeTrue()
        ;

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Connection Reset after Cancellation', function (): void {

    it('drains first then resets when both wasQueryCancelled and resetConnection are true', function (): void {
        $pool = makePool(
            maxSize: 1,
            enableServerSideCancellation: true,
            resetConnection: true,
        );

        $conn = await($pool->get());
        await($conn->query("SET application_name = 'pre_cancel'"));

        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $conn2 = await($pool->get());
        $result = await($conn2->query('SHOW application_name'));
        $name = $result->fetchOne()['application_name'];

        expect($name)->not->toBe('pre_cancel')
            ->and($conn2->wasQueryCancelled())->toBeFalse()
        ;

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Shutdown During Drain', function (): void {

    it('close() during an active drain does not leak the draining connection', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        expect($pool->stats['draining_connections'])->toBe(1);

        $pool->close();

        expect($pool->stats['active_connections'])->toBe(0)
            ->and($pool->stats['draining_connections'])->toBe(0)
        ;
    });

    it('closeAsync() waits for the draining connection to finish before resolving', function (): void {
        $pool = makePool(maxSize: 1, enableServerSideCancellation: true);

        $conn = await($pool->get());
        $slow = $conn->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $slow->cancel());
        expect(fn () => await($slow))->toThrow(CancelledException::class);

        $pool->release($conn);

        $resolved = false;
        $shutdown = $pool->closeAsync()->then(function () use (&$resolved): void {
            $resolved = true;
        });

        await(delay(0));
        expect($resolved)->toBeFalse();

        await($shutdown);

        expect($resolved)->toBeTrue()
            ->and($pool->stats['active_connections'])->toBe(0)
        ;
    });
});
