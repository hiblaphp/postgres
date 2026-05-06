<?php

declare(strict_types=1);

use Hibla\Postgres\Exceptions\NotInitializedException;
use Hibla\Postgres\Exceptions\PoolException;
use Hibla\Postgres\Internals\ManagedPreparedStatement;
use Hibla\Postgres\Internals\RowStream;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\TimeoutException;

use function Hibla\await;
use function Hibla\delay;

describe('query()', function (): void {

    it('executes a plain SQL query and returns a result', function (): void {
        $client = makeClient();

        $result = await($client->query('SELECT 1 AS val'));

        expect($result->fetchOne()['val'])->toBe('1');

        $client->close();
    });

    it('executes a parameterized query using the statement cache', function (): void {
        $client = makeClient(['enableStatementCache' => true]);

        $result = await($client->query('SELECT $1::int AS val', [42]));

        expect((int) $result->fetchOne()['val'])->toBe(42);

        $client->close();
    });

    it('executes a parameterized query without the statement cache', function (): void {
        $client = makeClient(['enableStatementCache' => false]);

        $result = await($client->query('SELECT $1::int AS val', [99]));

        expect((int) $result->fetchOne()['val'])->toBe(99);

        $client->close();
    });

    it('returns multiple rows correctly', function (): void {
        $client = makeClient();

        $result = await($client->query('SELECT generate_series(1, 5) AS n'));
        $rows = $result->fetchAll();

        expect($rows)->toHaveCount(5)
            ->and((int) $rows[0]['n'])->toBe(1)
            ->and((int) $rows[4]['n'])->toBe(5)
        ;

        $client->close();
    });

    it('releases the connection back to the pool after a successful query', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query('SELECT 1'));
        await($client->query('SELECT 2'));
        await($client->query('SELECT 3'));

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('releases the connection back to the pool after a query error', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        try {
            await($client->query('SELECT 1/0'));
        } catch (Throwable) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('throws when called after the client has been closed', function (): void {
        $client = makeClient();
        $client->close();

        expect(fn () => await($client->query('SELECT 1')))
            ->toThrow(NotInitializedException::class)
        ;
    });
});

describe('execute()', function (): void {

    it('returns the number of affected rows for an UPDATE', function (): void {
        $client = makeClient();

        await($client->query('CREATE TEMP TABLE exec_test (id int)'));
        await($client->query('INSERT INTO exec_test VALUES (1),(2),(3)'));

        $affected = await($client->execute('UPDATE exec_test SET id = id + 10'));

        expect($affected)->toBe(3);

        $client->close();
    });

    it('returns 0 when no rows are affected', function (): void {
        $client = makeClient();

        await($client->query('CREATE TEMP TABLE exec_zero (id int)'));

        $affected = await($client->execute('DELETE FROM exec_zero WHERE id = 999'));

        expect($affected)->toBe(0);

        $client->close();
    });
});

describe('executeGetId()', function (): void {

    it('returns the last inserted id via RETURNING', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMP TABLE get_id_test (id serial PRIMARY KEY, val text)'
        ));

        $id = await($client->executeGetId(
            "INSERT INTO get_id_test (val) VALUES ('hello') RETURNING id"
        ));

        expect($id)->toBeGreaterThan(0);

        $client->close();
    });
});

describe('fetchOne()', function (): void {

    it('returns the first row as an associative array', function (): void {
        $client = makeClient();

        $row = await($client->fetchOne("SELECT 'world' AS greeting"));

        expect($row)->toBeArray()
            ->and($row['greeting'])->toBe('world')
        ;

        $client->close();
    });

    it('returns null when the query returns no rows', function (): void {
        $client = makeClient();

        await($client->query('CREATE TEMP TABLE fetch_one_empty (id int)'));

        $row = await($client->fetchOne('SELECT * FROM fetch_one_empty'));

        expect($row)->toBeNull();

        $client->close();
    });
});

describe('fetchValue()', function (): void {

    it('returns the first column of the first row when no column is specified', function (): void {
        $client = makeClient();

        $val = await($client->fetchValue('SELECT 123::int'));

        expect((int) $val)->toBe(123);

        $client->close();
    });

    it('returns the named column when a string column name is provided', function (): void {
        $client = makeClient();

        $val = await($client->fetchValue("SELECT 'hello' AS greeting, 'world' AS name", 'name'));

        expect($val)->toBe('world');

        $client->close();
    });

    it('returns null when the query returns no rows', function (): void {
        $client = makeClient();

        await($client->query('CREATE TEMP TABLE fetch_val_empty (id int)'));

        $val = await($client->fetchValue('SELECT id FROM fetch_val_empty'));

        expect($val)->toBeNull();

        $client->close();
    });

    it('returns null when the specified column does not exist in the row', function (): void {
        $client = makeClient();

        $val = await($client->fetchValue('SELECT 1 AS val', 'nonexistent'));

        expect($val)->toBeNull();

        $client->close();
    });
});

describe('stream()', function (): void {

    it('streams rows from a plain query', function (): void {
        $client = makeClient();

        $stream = await($client->stream('SELECT generate_series(1, 10) AS n'));
        $rows = [];
        foreach ($stream as $row) {
            $rows[] = (int) $row['n'];
        }

        expect($rows)->toHaveCount(10)
            ->and($rows[0])->toBe(1)
            ->and($rows[9])->toBe(10)
        ;

        $client->close();
    });

    it('streams rows from a parameterized query via the statement cache', function (): void {
        $client = makeClient(['enableStatementCache' => true]);

        $stream = await($client->stream('SELECT generate_series(1, $1) AS n', [5]));
        $rows = [];
        foreach ($stream as $row) {
            $rows[] = (int) $row['n'];
        }

        expect($rows)->toHaveCount(5);

        $client->close();
    });

    it('releases the connection after the stream is fully consumed', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $stream = await($client->stream('SELECT generate_series(1, 3) AS n'));
        foreach ($stream as $row) {
        }

        $stream2 = await($client->stream('SELECT 1 AS ok'));
        foreach ($stream2 as $row) {
        }

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('releases the connection when the stream errors', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        try {
            $stream = await($client->stream('SELECT 1/0 AS bad'));
            foreach ($stream as $row) {
            }
        } catch (Throwable) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('returns a RowStream instance', function (): void {
        $client = makeClient();

        $stream = await($client->stream('SELECT 1 AS val'));

        expect($stream)->toBeInstanceOf(RowStream::class);

        foreach ($stream as $row) {
        }

        $client->close();
    });
});

describe('prepare()', function (): void {

    it('returns a ManagedPreparedStatement', function (): void {
        $client = makeClient();

        $stmt = await($client->prepare('SELECT $1::int AS val'));

        expect($stmt)->toBeInstanceOf(ManagedPreparedStatement::class);

        await($stmt->close());
        $client->close();
    });

    it('executes a ManagedPreparedStatement and returns a result', function (): void {
        $client = makeClient();

        $stmt = await($client->prepare('SELECT $1::int AS val'));
        $result = await($stmt->execute([77]));

        expect((int) $result->fetchOne()['val'])->toBe(77);

        await($stmt->close());
        $client->close();
    });

    it('executes a ManagedPreparedStatement multiple times', function (): void {
        $client = makeClient();

        $stmt = await($client->prepare('SELECT $1::int AS val'));

        foreach ([1, 2, 3] as $n) {
            $result = await($stmt->execute([$n]));
            expect((int) $result->fetchOne()['val'])->toBe($n);
        }

        await($stmt->close());
        $client->close();
    });

    it('streams rows via executeStream on a ManagedPreparedStatement', function (): void {
        $client = makeClient();

        $stmt = await($client->prepare('SELECT generate_series(1, $1::int) AS n'));
        $stream = await($stmt->executeStream([5]));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = (int) $row['n'];
        }

        expect($rows)->toHaveCount(5);

        await($stmt->close());
        $client->close();
    });

    it('releases the connection back to the pool when close() is called', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $stmt = await($client->prepare('SELECT 1 AS val'));
        await($stmt->close());

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('calling close() twice on a ManagedPreparedStatement is idempotent', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $stmt = await($client->prepare('SELECT 1'));
        await($stmt->close());
        await($stmt->close());

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('releases the connection if the prepare step fails', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        try {
            await($client->prepare('SELECT $1 FROM nonexistent_table_xyz'));
        } catch (Throwable) {
        }

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });
});

describe('Statement Cache', function (): void {

    it('serves subsequent calls from the cache without re-preparing', function (): void {
        $client = makeClient(['enableStatementCache' => true, 'maxConnections' => 1]);

        $sql = 'SELECT $1::int AS val';

        await($client->query($sql, [1]));
        await($client->query($sql, [2]));
        await($client->query($sql, [3]));

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('clearStatementCache() forces a re-prepare on the next call', function (): void {
        $client = makeClient(['enableStatementCache' => true, 'maxConnections' => 1]);

        $sql = 'SELECT $1::int AS val';

        await($client->query($sql, [10]));

        $client->clearStatementCache();

        $result = await($client->query($sql, [20]));
        expect((int) $result->fetchOne()['val'])->toBe(20);

        $client->close();
    });

    it('evicts the oldest entry when the cache is full', function (): void {
        $client = makeClient(['enableStatementCache' => true, 'statementCacheSize' => 2, 'maxConnections' => 1]);

        await($client->query('SELECT $1::int AS a', [1]));
        await($client->query('SELECT $1::int AS b', [2]));
        await($client->query('SELECT $1::int AS c', [3]));

        $result = await($client->query('SELECT $1::int AS a', [99]));
        expect((int) $result->fetchOne()['a'])->toBe(99);

        $client->close();
    });

    it('cache is bypassed when enableStatementCache is false', function (): void {
        $client = makeClient(['enableStatementCache' => false]);

        $sql = 'SELECT $1::int AS val';

        $r1 = await($client->query($sql, [1]));
        $r2 = await($client->query($sql, [2]));

        expect((int) $r1->fetchOne()['val'])->toBe(1)
            ->and((int) $r2->fetchOne()['val'])->toBe(2)
        ;

        $client->close();
    });

    it('invalidates the cache for a connection after reset', function (): void {
        $client = makeClient([
            'enableStatementCache' => true,
            'resetConnection' => true,
            'maxConnections' => 1,
        ]);

        $sql = 'SELECT $1::int AS val';

        await($client->query($sql, [1]));

        $result = await($client->query($sql, [2]));

        expect((int) $result->fetchOne()['val'])->toBe(2);

        $client->close();
    });
});

describe('Concurrent Execution', function (): void {

    it('executes multiple queries concurrently across different connections', function (): void {
        $client = makeClient(['maxConnections' => 3]);

        [$r1, $r2, $r3] = await(Promise::all([
            $client->query('SELECT 1 AS val'),
            $client->query('SELECT 2 AS val'),
            $client->query('SELECT 3 AS val'),
        ]));

        expect((int) $r1->fetchOne()['val'])->toBe(1)
            ->and((int) $r2->fetchOne()['val'])->toBe(2)
            ->and((int) $r3->fetchOne()['val'])->toBe(3)
        ;

        $client->close();
    });

    it('queues requests as waiters when the pool is at capacity', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $results = await(Promise::all([
            $client->fetchValue('SELECT 10::int'),
            $client->fetchValue('SELECT 20::int'),
            $client->fetchValue('SELECT 30::int'),
        ]));

        expect(array_map('intval', $results))->toBe([10, 20, 30]);

        $client->close();
    });
});

describe('Pool Integration', function (): void {

    it('stats includes both pool and client-specific fields', function (): void {
        $client = makeClient(['enableStatementCache' => true, 'statementCacheSize' => 32]);

        $stats = $client->stats;

        expect($stats)->toHaveKey('active_connections')
            ->toHaveKey('pooled_connections')
            ->toHaveKey('statement_cache_enabled')
            ->toHaveKey('statement_cache_size')
        ;

        expect($stats['statement_cache_enabled'])->toBeTrue()
            ->and($stats['statement_cache_size'])->toBe(32)
        ;

        $client->close();
    });

    it('healthCheck() returns healthy and unhealthy counts', function (): void {
        $client = makeClient(['minConnections' => 2]);

        await(delay(0.1));

        $stats = await($client->healthCheck());

        expect($stats)->toHaveKey('total_checked')
            ->toHaveKey('healthy')
            ->toHaveKey('unhealthy')
        ;

        expect($stats['healthy'])->toBeGreaterThanOrEqual(1);

        $client->close();
    });

    it('rejects with PoolException when maxWaiters is exceeded', function (): void {
        $client = makeClient(['maxConnections' => 1, 'maxWaiters' => 1]);

        $slow = $client->query('SELECT pg_sleep(1)');
        $slow->catch(static function (): void {
        });

        $w1 = $client->query('SELECT 1');
        $w1->catch(static function (): void {
        });

        expect(fn () => await($client->query('SELECT 2')))
            ->toThrow(PoolException::class)
        ;

        $slow->cancel();

        $client->close();
    });

    it('rejects with TimeoutException when acquireTimeout is exceeded', function (): void {
        $client = makeClient(['maxConnections' => 1, 'acquireTimeout' => 0.05]);

        $slow = $client->query('SELECT pg_sleep(1)');
        $slow->catch(static function (): void {
        });

        expect(fn () => await($client->query('SELECT 1')))
            ->toThrow(TimeoutException::class)
        ;

        $slow->cancel();

        $client->close();
    });

    it('runs the onConnect hook for every new physical connection', function (): void {
        $hookCount = 0;

        $client = makeClient([
            'maxConnections' => 2,
            'onConnect' => function () use (&$hookCount): void {
                $hookCount++;
            },
        ]);

        await(Promise::all([
            $client->query('SELECT pg_sleep(0.05)'),
            $client->query('SELECT pg_sleep(0.05)'),
        ]));

        expect($hookCount)->toBe(2);

        $client->close();
    });

    it('minConnections warms up the pool on construction', function (): void {
        $client = makeClient(['minConnections' => 2, 'maxConnections' => 5]);

        await(delay(0.2));

        expect($client->stats['total_connections'])->toBeGreaterThanOrEqual(2);

        $client->close();
    });
});

describe('Shutdown', function (): void {

    it('close() force-closes the pool and makes the client unusable', function (): void {
        $client = makeClient();

        await($client->query('SELECT 1'));

        $client->close();

        expect(fn () => await($client->query('SELECT 1')))
            ->toThrow(NotInitializedException::class)
        ;
    });

    it('calling close() twice does not throw', function (): void {
        $client = makeClient();

        $threw = false;

        try {
            $client->close();
            $client->close();
        } catch (Throwable) {
            $threw = true;
        }

        expect($threw)->toBeFalse();
    });

    it('closeAsync() resolves after all active queries finish', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $slow = $client->query('SELECT pg_sleep(0.1)');

        $resolved = false;
        $shutdown = $client->closeAsync()->then(function () use (&$resolved): void {
            $resolved = true;
        });

        await(delay(0));
        expect($resolved)->toBeFalse();

        await($shutdown);
        expect($resolved)->toBeTrue();
    });

    it('calling closeAsync() twice returns the same promise', function (): void {
        $client = makeClient();

        $p1 = $client->closeAsync();
        $p2 = $client->closeAsync();

        expect($p1)->toBe($p2);

        await($p1);
    });

    it('closeAsync() with a timeout falls back to force close', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $held = $client->query('SELECT pg_sleep(10)');
        $held->catch(static function (): void {
        });

        $resolved = false;
        $shutdown = $client->closeAsync(timeout: 0.1)->then(function () use (&$resolved): void {
            $resolved = true;
        });

        await($shutdown);

        expect($resolved)->toBeTrue();
    });

    it('get() is rejected immediately after closeAsync() is called', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $held = $client->query('SELECT pg_sleep(10)');
        $held->catch(static function (): void {
        });

        $client->closeAsync(timeout: 0.1);

        expect(fn () => await($client->query('SELECT 1')))
            ->toThrow(PoolException::class)
        ;
    });
});

describe('Error Propagation', function (): void {

    it('propagates a syntax error from query() as a QueryException', function (): void {
        $client = makeClient();

        expect(fn () => await($client->query('NOT VALID SQL !!!')))
            ->toThrow(Hibla\Sql\Exceptions\QueryException::class)
        ;

        $client->close();
    });

    it('propagates a division-by-zero error from fetchOne()', function (): void {
        $client = makeClient();

        expect(fn () => await($client->fetchOne('SELECT 1/0 AS bad')))
            ->toThrow(Hibla\Sql\Exceptions\QueryException::class)
        ;

        $client->close();
    });

    it('propagates an error from execute()', function (): void {
        $client = makeClient();

        expect(fn () => await($client->execute('INSERT INTO nonexistent_table VALUES (1)')))
            ->toThrow(Hibla\Sql\Exceptions\QueryException::class)
        ;

        $client->close();
    });

    it('propagates an error from stream() before any row is yielded', function (): void {
        $client = makeClient();

        expect(fn () => await($client->stream('SELECT 1/0 AS bad')))
            ->toThrow(Hibla\Sql\Exceptions\QueryException::class)
        ;

        $client->close();
    });

    it('does not leave dangling connections after multiple consecutive errors', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        for ($i = 0; $i < 3; $i++) {
            try {
                await($client->query('SELECT 1/0'));
            } catch (Throwable) {
            }
        }

        $result = await($client->query('SELECT 42 AS val'));
        expect((int) $result->fetchOne()['val'])->toBe(42)
            ->and($client->stats['active_connections'])->toBe(0)
        ;

        $client->close();
    });
});
