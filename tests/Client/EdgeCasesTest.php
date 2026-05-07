<?php

declare(strict_types=1);

use Hibla\Postgres\Exceptions\PoolException;
use Hibla\Postgres\Interfaces\ConnectionSetup;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\QueryException;

use function Hibla\await;
use function Hibla\delay;

describe('Statement Cache - edge cases', function (): void {

    it('does not share cached statements across different physical connections', function (): void {
        $client = makeClient(['enableStatementCache' => true, 'maxConnections' => 2]);

        $sql = 'SELECT $1::int AS val';

        [$r1, $r2] = await(Promise::all([
            $client->query($sql, [7]),
            $client->query($sql, [8]),
        ]));

        expect((int) $r1->fetchOne()['val'])->toBe(7)
            ->and((int) $r2->fetchOne()['val'])->toBe(8)
        ;

        $client->close();
    });

    it('cache entry is evicted and statement is deallocated when cache is full', function (): void {
        $client = makeClient([
            'enableStatementCache' => true,
            'statementCacheSize' => 1,
            'maxConnections' => 1,
        ]);

        await($client->query('SELECT $1::int AS a', [1]));
        await($client->query('SELECT $1::int AS b', [2]));
        $result = await($client->query('SELECT $1::int AS a', [99]));

        expect((int) $result->fetchOne()['a'])->toBe(99);

        $client->close();
    });

    it('cache is rebuilt per-connection after resetConnection clears session state', function (): void {
        $client = makeClient([
            'enableStatementCache' => true,
            'resetConnection' => true,
            'maxConnections' => 1,
        ]);

        $sql = 'SELECT $1::int AS val';

        await($client->query($sql, [1]));
        await($client->query($sql, [2]));

        $result = await($client->query($sql, [3]));
        expect((int) $result->fetchOne()['val'])->toBe(3);

        $client->close();
    });

    it('clearStatementCache() does not break subsequent parameterized queries', function (): void {
        $client = makeClient(['enableStatementCache' => true, 'maxConnections' => 1]);

        $sql = 'SELECT $1::int AS val';

        await($client->query($sql, [10]));
        $client->clearStatementCache();

        $result = await($client->query($sql, [20]));

        expect((int) $result->fetchOne()['val'])->toBe(20);

        $client->close();
    });
});

describe('Connection lifecycle - edge cases', function (): void {

    it('discards a connection that has exceeded idleTimeout on next borrow', function (): void {
        $client = makeClient([
            'maxConnections' => 1,
            'idleTimeout' => 1,
        ]);

        await($client->query('SELECT 1'));

        await(delay(1.2));

        $result = await($client->query('SELECT 42 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(42);

        $client->close();
    });

    it('discards a connection that has exceeded maxLifetime on next borrow', function (): void {
        $client = makeClient([
            'maxConnections' => 1,
            'maxLifetime' => 1,
            'idleTimeout' => 300,
        ]);

        await($client->query('SELECT 1'));

        await(delay(1.2));

        $result = await($client->query('SELECT 99 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(99);

        $client->close();
    });

    it('pool stays within maxConnections even under a burst of concurrent requests', function (): void {
        $maxConns = 3;
        $client = makeClient(['maxConnections' => $maxConns]);

        await(Promise::all(array_map(
            fn () => $client->query('SELECT pg_sleep(0.05)'),
            range(1, 6)
        )));

        expect($client->stats['total_connections'])->toBeLessThanOrEqual($maxConns);

        $client->close();
    });

    it('minConnections connections are replenished after forced eviction', function (): void {
        $client = makeClient(['minConnections' => 2, 'maxConnections' => 5]);

        await(delay(0.2));

        $before = $client->stats['total_connections'];

        await($client->healthCheck());
        await(delay(0.2));

        expect($client->stats['total_connections'])->toBeGreaterThanOrEqual(2);
        expect($client->stats['total_connections'])->toBeGreaterThanOrEqual(
            min($before, 2)
        );

        $client->close();
    });
});

describe('onConnect hook - edge cases', function (): void {

    it('re-runs the hook after DISCARD ALL when resetConnection is enabled', function (): void {
        $callCount = 0;

        $client = makeClient([
            'maxConnections' => 1,
            'resetConnection' => true,
            'onConnect' => function () use (&$callCount): void {
                $callCount++;
            },
        ]);

        await($client->query('SELECT 1'));
        await($client->query('SELECT 2'));

        expect($callCount)->toBe(2);

        $client->close();
    });

    it('drops the connection and satisfies the waiter when the onConnect hook throws', function (): void {
        $attempts = 0;

        $client = makeClient([
            'maxConnections' => 1,
            'onConnect' => function () use (&$attempts): void {
                $attempts++;

                if ($attempts === 1) {
                    throw new RuntimeException('Hook failure');
                }
            },
        ]);

        try {
            await($client->query('SELECT 1'));
        } catch (RuntimeException) {
        }

        $result = await($client->query('SELECT 42 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(42);

        $client->close();
    });

    it('supports an async (promise-returning) onConnect hook', function (): void {
        $applied = false;

        $client = makeClient([
            'maxConnections' => 1,
            'onConnect' => function (ConnectionSetup $setup) use (&$applied): PromiseInterface {
                return $setup->query("SET application_name = 'hibla_test'")->then(
                    function () use (&$applied): void {
                        $applied = true;
                    }
                );
            },
        ]);

        await($client->query('SELECT 1'));

        expect($applied)->toBeTrue();

        $client->close();
    });
});

describe('fetchValue() - edge cases', function (): void {

    it('returns the value at a numeric column index', function (): void {
        $client = makeClient();

        $val = await($client->fetchValue('SELECT 10 AS a, 20 AS b', 1));

        expect((int) $val)->toBe(20);

        $client->close();
    });

    it('returns null when the column value itself is SQL NULL', function (): void {
        $client = makeClient();

        $val = await($client->fetchValue('SELECT NULL::int AS val'));

        expect($val)->toBeNull();

        $client->close();
    });

    it('returns null when the named column exists but holds NULL', function (): void {
        $client = makeClient();

        $val = await($client->fetchValue('SELECT NULL::text AS name', 'name'));

        expect($val)->toBeNull();

        $client->close();
    });
});

describe('executeGetId() - edge cases', function (): void {

    it('returns 0 when the query produces no rows and no lastInsertId', function (): void {
        $client = makeClient();

        await($client->query('CREATE TEMP TABLE get_id_noop (id int)'));

        $id = await($client->executeGetId('UPDATE get_id_noop SET id = 1 WHERE id = 999'));

        expect($id)->toBe(0);

        $client->close();
    });

    it('casts the RETURNING value to int correctly for large ids', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMP TABLE large_id (id bigserial PRIMARY KEY, v text)'
        ));

        $id = await($client->executeGetId(
            "INSERT INTO large_id (v) VALUES ('x') RETURNING id"
        ));

        expect($id)->toBeGreaterThan(0);
        expect(\is_int($id))->toBeTrue();

        $client->close();
    });
});

describe('stream() - edge cases', function (): void {

    it('streams a parameterized query when statement cache is disabled', function (): void {
        $client = makeClient(['enableStatementCache' => false]);

        $stream = await($client->stream('SELECT generate_series(1, $1) AS n', [4]));
        $rows = [];

        foreach ($stream as $row) {
            $rows[] = (int) $row['n'];
        }

        expect($rows)->toBe([1, 2, 3, 4]);

        $client->close();
    });

    it('releases the connection after an error mid-stream iteration', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        try {
            $stream = await($client->stream(
                'SELECT CASE WHEN n = 3 THEN 1/0 ELSE n END AS val
                 FROM generate_series(1, 5) AS n'
            ));

            foreach ($stream as $row) {
            }
        } catch (Throwable) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('handles an empty result set without error', function (): void {
        $client = makeClient();

        await($client->query('CREATE TEMP TABLE empty_stream_tbl (id int)'));

        $stream = await($client->stream('SELECT * FROM empty_stream_tbl'));
        $rows = [];

        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toBeEmpty();

        $client->close();
    });

    it('can stream the same SQL concurrently on two connections', function (): void {
        $client = makeClient(['maxConnections' => 2]);

        [$s1, $s2] = await(Promise::all([
            $client->stream('SELECT generate_series(1, 3) AS n'),
            $client->stream('SELECT generate_series(4, 6) AS n'),
        ]));

        $a = [];
        foreach ($s1 as $row) {
            $a[] = (int) $row['n'];
        }

        $b = [];
        foreach ($s2 as $row) {
            $b[] = (int) $row['n'];
        }

        expect($a)->toBe([1, 2, 3]);
        expect($b)->toBe([4, 5, 6]);

        $client->close();
    });
});

describe('Parameter parsing - edge cases', function (): void {

    it('resolves named :name placeholders correctly', function (): void {
        $client = makeClient();

        $stmt = await($client->prepare('SELECT :x::int AS x, :y::int AS y'));
        $result = await($stmt->execute(['x' => 10, 'y' => 20]));
        $row = $result->fetchOne();

        expect((int) $row['x'])->toBe(10)
            ->and((int) $row['y'])->toBe(20)
        ;

        await($stmt->close());
        $client->close();
    });

    it('resolves ? positional placeholders correctly', function (): void {
        $client = makeClient();

        $stmt = await($client->prepare('SELECT ?::int AS a, ?::int AS b'));
        $result = await($stmt->execute([5, 15]));
        $row = $result->fetchOne();

        expect((int) $row['a'])->toBe(5)
            ->and((int) $row['b'])->toBe(15)
        ;

        await($stmt->close());
        $client->close();
    });

    it('boolean parameters are coerced to 1/0 strings for pg_send_execute', function (): void {
        $client = makeClient();

        $result = await($client->query('SELECT $1::bool AS flag', [true]));

        expect($result->fetchOne()['flag'])->toBe('t');

        $client->close();
    });
});

describe('Shutdown - edge cases', function (): void {

    it('close() while closeAsync() is pending resolves the async shutdown promise', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $held = $client->query('SELECT pg_sleep(10)');
        $held->catch(static fn () => null);

        $resolved = false;
        $asyncDone = $client->closeAsync()->then(static function () use (&$resolved): void {
            $resolved = true;
        });
        $asyncDone->catch(static fn () => null);

        $client->close();

        await(delay(0));

        expect($resolved)->toBeTrue();
    });

    it('closeAsync() rejects all pending waiters immediately', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $held = $client->query('SELECT pg_sleep(10)');
        $held->catch(static fn () => null);

        $waiter = $client->query('SELECT 1');
        $waiterError = null;
        $waiter->catch(static function (Throwable $e) use (&$waiterError): void {
            $waiterError = $e;
        });

        $client->closeAsync(timeout: 0.05);

        await(delay(0));

        expect($waiterError)->toBeInstanceOf(PoolException::class);

        $client->close();
    });

    it('stats reflect graceful shutdown state while draining', function (): void {
        $client = makeClient(['maxConnections' => 2]);

        $slow = $client->query('SELECT pg_sleep(0.1)');
        $slow->catch(static fn () => null);

        $shutdown = $client->closeAsync();

        $stats = $client->stats;
        expect($stats['is_graceful_shutdown'])->toBeTrue();

        await($shutdown);

        $client->close();
    });

    it('healthCheck() completes without error during graceful shutdown', function (): void {
        $client = makeClient(['minConnections' => 2]);

        await(delay(0.1));

        $shutdown = $client->closeAsync(timeout: 1.0);

        $stats = await($client->healthCheck());

        expect($stats)->toHaveKey('healthy')
            ->toHaveKey('unhealthy')
            ->toHaveKey('total_checked')
        ;

        await($shutdown);
    });

    it('calling query() after closeAsync() returns a PoolException immediately', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $client->closeAsync(timeout: 5.0);

        expect(fn () => await($client->query('SELECT 1')))
            ->toThrow(PoolException::class)
        ;

        $client->close();
    });
});

describe('Error propagation - edge cases', function (): void {

    it('a constraint violation leaves the pool connection usable', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query('CREATE TEMP TABLE uniq_test (id int UNIQUE)'));
        await($client->query('INSERT INTO uniq_test VALUES (1)'));

        try {
            await($client->query('INSERT INTO uniq_test VALUES (1)'));
        } catch (QueryException) {
        }

        $result = await($client->query('SELECT count(*) AS c FROM uniq_test'));
        expect((int) $result->fetchOne()['c'])->toBe(1);

        $client->close();
    });

    it('an error from executeGetId does not leave a dangling connection', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        try {
            await($client->executeGetId('SELECT 1/0 AS bad'));
        } catch (Throwable) {
        }

        $result = await($client->query('SELECT 55 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(55);

        $client->close();
    });

    it('multiple interleaved errors across concurrent queries do not corrupt pool state', function (): void {
        $client = makeClient(['maxConnections' => 3]);

        await(Promise::allSettled([
            $client->query('SELECT 1/0'),
            $client->query('SELECT 2'),
            $client->query('SELECT 1/0'),
        ]));

        $r = await($client->query('SELECT 77 AS val'));
        expect((int) $r->fetchOne()['val'])->toBe(77);
        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });
});
