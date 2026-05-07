<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Promise\Exceptions\CancelledException;

use function Hibla\await;
use function Hibla\delay;

describe('query() Cancellation', function (): void {

    it('throws CancelledException when the promise is cancelled mid-execution', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        expect(fn () => await($promise))->toThrow(CancelledException::class);

        $client->close();
    });

    it('connection is reusable after a plain query is cancelled and drained', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('active_connections returns to 0 after the drain cycle completes', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        await($client->query('SELECT 1'));

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('cancelling a parameterized query (statement cache hit) drains cleanly', function (): void {
        $client = makeClient([
            'maxConnections' => 1,
            'enableStatementCache' => true,
            'enableServerSideCancellation' => true,
        ]);

        await($client->query('SELECT pg_sleep($1)', [0]));

        $promise = $client->query('SELECT pg_sleep($1)', [2]);
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('cancelling a parameterized query (no statement cache) drains cleanly', function (): void {
        $client = makeClient([
            'maxConnections' => 1,
            'enableStatementCache' => false,
            'enableServerSideCancellation' => true,
        ]);

        $promise = $client->query('SELECT pg_sleep($1)', [2]);
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('multiple sequential cancellations do not leak connections', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        for ($i = 0; $i < 3; $i++) {
            $promise = $client->query('SELECT pg_sleep(2)');
            Loop::addTimer(0.1, fn () => $promise->cancel());

            try {
                await($promise);
            } catch (CancelledException) {
            }
        }

        $result = await($client->query('SELECT 42 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(42);
        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('draining_connections stat is 1 immediately after cancel and 0 after drain', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        expect($client->stats['draining_connections'])->toBe(1);

        await($client->query('SELECT 1'));

        expect($client->stats['draining_connections'])->toBe(0);

        $client->close();
    });
});

describe('Derived method Cancellation', function (): void {

    it('execute() throws CancelledException and leaves the pool healthy', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->execute('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        expect(fn () => await($promise))->toThrow(CancelledException::class);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('fetchOne() throws CancelledException and leaves the pool healthy', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->fetchOne('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        expect(fn () => await($promise))->toThrow(CancelledException::class);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('fetchValue() throws CancelledException and leaves the pool healthy', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->fetchValue('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        expect(fn () => await($promise))->toThrow(CancelledException::class);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });
});

describe('stream() Cancellation', function (): void {

    it('throws CancelledException when the outer stream promise is cancelled before resolving', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->stream('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        expect(fn () => await($promise))->toThrow(CancelledException::class);

        $client->close();
    });

    it('connection is reusable after the outer stream promise is cancelled', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $promise = $client->stream('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('connection is reusable after a plain stream is cancelled mid-iteration', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $stream = await($client->stream(twentyRowPgSql()));

        $rowsRead = 0;
        foreach ($stream as $row) {
            $rowsRead++;
            if ($rowsRead >= 3) {
                $stream->cancel();

                break;
            }
        }

        expect($stream->isCancelled())->toBeTrue();

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('active_connections returns to 0 after a stream is cancelled mid-iteration', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $stream = await($client->stream(twentyRowPgSql()));

        $rowsRead = 0;
        foreach ($stream as $row) {
            $rowsRead++;
            if ($rowsRead >= 3) {
                $stream->cancel();

                break;
            }
        }

        await($client->query('SELECT 1'));

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('connection is reusable after a parameterized stream is cancelled mid-iteration', function (): void {
        $client = makeClient([
            'maxConnections' => 1,
            'enableStatementCache' => true,
            'enableServerSideCancellation' => true,
        ]);

        $stream = await($client->stream(twentyRowPgPreparedSql(), [20]));

        $rowsRead = 0;
        foreach ($stream as $row) {
            $rowsRead++;
            if ($rowsRead >= 5) {
                $stream->cancel();

                break;
            }
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('cancelling a fully-consumed stream does not set wasQueryCancelled', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $stream = await($client->stream(twentyRowPgSql()));
        foreach ($stream as $row) {
        }
        $stream->cancel();

        expect($client->stats['draining_connections'])->toBe(0);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });
});

describe('prepare() Cancellation', function (): void {

    it('cancelling a prepare() waiter does not corrupt the pool', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $slow = $client->query('SELECT pg_sleep(0.5)');
        $slow->catch(fn () => null);

        $prepare = $client->prepare('SELECT $1::int AS val');
        $prepare->cancel();

        expect($prepare->isCancelled())->toBeTrue();

        await($slow);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('a valid prepare() after a cancelled prepare() waiter succeeds', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $slow = $client->query('SELECT pg_sleep(0.3)');
        $slow->catch(fn () => null);

        $cancelled = $client->prepare('SELECT $1::int AS a');
        $cancelled->cancel();

        await($slow);

        $stmt = await($client->prepare('SELECT $1::int AS val'));
        $result = await($stmt->execute([99]));

        expect((int) $result->fetchOne()['val'])->toBe(99);

        await($stmt->close());
        $client->close();
    });
});

describe('Waiter Cancellation', function (): void {

    it('cancelling a waiter does not prevent remaining waiters from being served', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $slow = $client->query('SELECT pg_sleep(0.2)');

        $w1 = $client->query('SELECT 1 AS a');
        $w2 = $client->query('SELECT 2 AS b');
        $w3 = $client->query('SELECT 3 AS c');

        $w2->cancel();

        await($slow);

        $r1 = await($w1);
        $r3 = await($w3);

        expect((int) $r1->fetchOne()['a'])->toBe(1)
            ->and((int) $r3->fetchOne()['c'])->toBe(3)
            ->and($w2->isCancelled())->toBeTrue()
        ;

        $client->close();
    });

    it('cancelling all waiters returns the pool to idle once the held query finishes', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $slow = $client->query('SELECT pg_sleep(0.2)');

        $w1 = $client->query('SELECT 1');
        $w2 = $client->query('SELECT 2');

        $w1->cancel();
        $w2->cancel();

        await($slow);
        await(delay(0.05));

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('cancelling a waiter while the pool is draining does not cause a double-release', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $slow = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());

        try {
            await($slow);
        } catch (CancelledException) {
        }

        $waiter = $client->query('SELECT 1');
        $waiter->cancel();

        expect($waiter->isCancelled())->toBeTrue();

        await(delay(0.4));

        $result = await($client->query('SELECT 77 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(77);
        expect($client->stats['draining_connections'])->toBe(0);

        $client->close();
    });
});

describe('Cancellation Opt-out', function (): void {

    it('throws CancelledException and closes the connection when opt-out is active', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => false]);

        $promise = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        expect(fn () => await($promise))->toThrow(CancelledException::class);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('draining_connections stays at 0 during opt-out cancel', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => false]);

        $promise = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $promise->cancel());

        try {
            await($promise);
        } catch (CancelledException) {
        }

        expect($client->stats['draining_connections'])->toBe(0);

        $client->close();
    });

    it('pool stays within maxConnections after opt-out cancel replaces closed connections', function (): void {
        $client = makeClient(['maxConnections' => 2, 'enableServerSideCancellation' => false]);

        $p1 = $client->query('SELECT pg_sleep(2)');
        $p2 = $client->query('SELECT pg_sleep(2)');

        Loop::addTimer(0.1, fn () => $p1->cancel());
        Loop::addTimer(0.1, fn () => $p2->cancel());

        try {
            await($p1);
        } catch (CancelledException) {
        }

        try {
            await($p2);
        } catch (CancelledException) {
        }

        await(delay(0.2));

        expect($client->stats['total_connections'])->toBeLessThanOrEqual(2);

        $client->close();
    });

    it('a waiter is served with a fresh connection after an opt-out cancel', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => false]);

        $slow = $client->query('SELECT pg_sleep(2)');
        $waiter = $client->query('SELECT 42 AS val');

        Loop::addTimer(0.1, fn () => $slow->cancel());

        try {
            await($slow);
        } catch (CancelledException) {
        }

        $result = await($waiter);
        expect((int) $result->fetchOne()['val'])->toBe(42);

        $client->close();
    });
});

describe('Cancellation + resetConnection', function (): void {

    it('drains first then resets when both cancellation and resetConnection are active', function (): void {
        $client = makeClient([
            'maxConnections' => 1,
            'enableServerSideCancellation' => true,
            'resetConnection' => true,
        ]);

        await($client->query("SET application_name = 'pre_cancel'"));

        $slow = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());

        try {
            await($slow);
        } catch (CancelledException) {
        }

        $result = await($client->query('SHOW application_name'));
        $name = $result->fetchOne()['application_name'];

        expect($name)->not->toBe('pre_cancel');
        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('onConnect hook re-runs after a drain+reset cycle triggered by cancellation', function (): void {
        $hookCount = 0;

        $client = makeClient([
            'maxConnections' => 1,
            'enableServerSideCancellation' => true,
            'resetConnection' => true,
            'onConnect' => function () use (&$hookCount): void {
                $hookCount++;
            },
        ]);

        await($client->query('SELECT 1'));
        expect($hookCount)->toBe(1);

        $slow = $client->query('SELECT pg_sleep(2)');
        Loop::addTimer(0.1, fn () => $slow->cancel());

        try {
            await($slow);
        } catch (CancelledException) {
        }

        await($client->query('SELECT 1'));

        // The hook fires on initial connect, after the DISCARD ALL reset,
        // and potentially on the side-channel kill connection depending on how
        // the client wires the hook in the test suite.
        expect($hookCount)->toBeGreaterThanOrEqual(2);

        $client->close();
    });

    it('cancels a long-running query rapidly via server-side cancellation and connection can be reused', function (): void {
        $client = makeClient(['maxConnections' => 1, 'enableServerSideCancellation' => true]);

        $start = microtime(true);

        $promise = $client->query('SELECT pg_sleep(5)');

        Loop::addTimer(0.1, fn () => $promise->cancel());

        $threw = false;

        try {
            await($promise);
        } catch (CancelledException) {
            $threw = true;
        }

        $duration = microtime(true) - $start;

        expect($threw)->toBeTrue()
            ->and($duration)->toBeLessThan(1.0)
        ;

        $result = await($client->query('SELECT 1 AS ok'));

        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });
});
