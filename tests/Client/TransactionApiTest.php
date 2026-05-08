<?php

declare(strict_types=1);

use Hibla\Sql\Exceptions\QueryException;
use Hibla\Sql\Exceptions\TransactionException;
use Hibla\Sql\TransactionOptions;

use function Hibla\await;
use function Hibla\delay;

describe('Manual Transactions (beginTransaction)', function (): void {

    it('commits changes successfully', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query(
            'CREATE TEMPORARY TABLE txn_commit_test (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        expect($tx->isActive())->toBeTrue()
            ->and($tx->isClosed())->toBeFalse()
        ;

        await($tx->query("INSERT INTO txn_commit_test (val) VALUES ('x')"));
        await($tx->commit());

        expect($tx->isActive())->toBeFalse();

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_commit_test'));
        expect((int) $result->fetchOne()['c'])->toBe(1);

        $client->close();
    });

    it('rolls back changes successfully', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query(
            'CREATE TEMPORARY TABLE txn_rollback_test (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO txn_rollback_test (val) VALUES ('y')"));
        await($tx->rollback());

        expect($tx->isActive())->toBeFalse();

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_rollback_test'));
        expect((int) $result->fetchOne()['c'])->toBe(0);

        $client->close();
    });

    it('releases the connection back to the pool after commit', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $tx = await($client->beginTransaction());
        await($tx->query('SELECT 1'));
        await($tx->commit());

        expect($client->stats['active_connections'])->toBe(0);

        $result = await($client->query('SELECT 1 AS ok'));
        expect((int) $result->fetchOne()['ok'])->toBe(1);

        $client->close();
    });

    it('releases the connection back to the pool after rollback', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        $tx = await($client->beginTransaction());
        await($tx->query('SELECT 1'));
        await($tx->rollback());

        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('throws when trying to query after commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->commit());

        expect(fn () => await($tx->query('SELECT 1')))
            ->toThrow(TransactionException::class, 'transaction is no longer active')
        ;

        $client->close();
    });
});

describe('Auto-managed Transactions (transaction)', function (): void {

    it('auto-commits when the callback returns successfully', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query(
            'CREATE TEMPORARY TABLE txn_auto_commit (id serial PRIMARY KEY, val text)'
        ));

        $returnValue = await($client->transaction(function ($tx) {
            await($tx->query("INSERT INTO txn_auto_commit (val) VALUES ('hello')"));

            return 'success_payload';
        }));

        expect($returnValue)->toBe('success_payload');

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_auto_commit'));
        expect((int) $result->fetchOne()['c'])->toBe(1);
        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('auto-rolls back and rethrows when the callback throws', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query(
            'CREATE TEMPORARY TABLE txn_auto_rollback (id serial PRIMARY KEY, val text)'
        ));

        try {
            await($client->transaction(function ($tx) {
                await($tx->query("INSERT INTO txn_auto_rollback (val) VALUES ('x')"));

                throw new RuntimeException('Intentional failure');
            }));
        } catch (RuntimeException $e) {
            expect($e->getMessage())->toBe('Intentional failure');
        }

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_auto_rollback'));
        expect((int) $result->fetchOne()['c'])->toBe(0);
        expect($client->stats['active_connections'])->toBe(0);

        $client->close();
    });

    it('retries on a retryable error up to the max attempts', function (): void {
        $client = makeClient(['maxConnections' => 1]);
        $attempts = 0;

        $options = new TransactionOptions(
            attempts: 3,
            retryableExceptions: [RuntimeException::class],
        );

        await($client->transaction(function ($tx) use (&$attempts) {
            $attempts++;
            if ($attempts < 3) {
                throw new RuntimeException('Retryable network blip');
            }

            return 'done';
        }, $options));

        expect($attempts)->toBe(3);

        $client->close();
    });

    it('stops immediately on a tier-2 non-retryable exception', function (): void {
        $client = makeClient(['maxConnections' => 1]);
        $attempts = 0;

        $options = new TransactionOptions(attempts: 5);

        try {
            await($client->transaction(function ($tx) use (&$attempts) {
                $attempts++;

                throw new QueryException('Bad SQL');
            }, $options));
        } catch (QueryException) {
        }

        expect($attempts)->toBe(1);

        $client->close();
    });
});

describe('Transaction::execute()', function (): void {

    it('returns the affected row count on INSERT', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_execute_insert (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $affected = await($tx->execute(
            "INSERT INTO txn_execute_insert (val) VALUES ('a'), ('b'), ('c')"
        ));

        expect($affected)->toBe(3);

        await($tx->commit());
        $client->close();
    });

    it('returns the affected row count on UPDATE', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_execute_update (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_execute_update (val) VALUES ('x'), ('x'), ('y')"
        ));

        $tx = await($client->beginTransaction());

        $affected = await($tx->execute(
            "UPDATE txn_execute_update SET val = 'z' WHERE val = 'x'"
        ));

        expect($affected)->toBe(2);

        await($tx->commit());
        $client->close();
    });

    it('returns the affected row count on DELETE', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_execute_delete (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_execute_delete (val) VALUES ('del'), ('del'), ('keep')"
        ));

        $tx = await($client->beginTransaction());

        $affected = await($tx->execute(
            "DELETE FROM txn_execute_delete WHERE val = 'del'"
        ));

        expect($affected)->toBe(2);

        await($tx->commit());
        $client->close();
    });

    it('supports parameterised queries', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_execute_params (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $affected = await($tx->execute(
            'INSERT INTO txn_execute_params (val) VALUES ($1), ($2)',
            ['alpha', 'beta']
        ));

        expect($affected)->toBe(2);

        await($tx->commit());
        $client->close();
    });

    it('throws when called after commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->commit());

        expect(fn () => await($tx->execute('SELECT 1')))
            ->toThrow(TransactionException::class, 'transaction is no longer active')
        ;

        $client->close();
    });
});

describe('Transaction::executeGetId()', function (): void {

    it('returns the inserted row id via RETURNING', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_get_id (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $id = await($tx->executeGetId(
            "INSERT INTO txn_get_id (val) VALUES ('first') RETURNING id"
        ));

        expect($id)->toBeInt()->toBeGreaterThan(0);

        await($tx->commit());
        $client->close();
    });

    it('returns ids that are sequential across inserts in the same transaction', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_get_id_seq (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $id1 = await($tx->executeGetId(
            "INSERT INTO txn_get_id_seq (val) VALUES ('a') RETURNING id"
        ));
        $id2 = await($tx->executeGetId(
            "INSERT INTO txn_get_id_seq (val) VALUES ('b') RETURNING id"
        ));

        expect($id2)->toBe($id1 + 1);

        await($tx->commit());
        $client->close();
    });

    it('supports parameterised queries', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_get_id_params (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $id = await($tx->executeGetId(
            'INSERT INTO txn_get_id_params (val) VALUES ($1) RETURNING id',
            ['paramval']
        ));

        expect($id)->toBeInt()->toBeGreaterThan(0);

        await($tx->commit());
        $client->close();
    });

    it('returns 0 when no rows are returned and no last insert id is available', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_get_id_empty (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $id = await($tx->executeGetId(
            "UPDATE txn_get_id_empty SET val = 'x' WHERE id = -1 RETURNING id"
        ));

        expect($id)->toBe(0);

        await($tx->commit());
        $client->close();
    });
});

describe('Transaction::fetchOne()', function (): void {

    it('returns the first row as an associative array', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_one (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_fetch_one (val) VALUES ('first'), ('second')"
        ));

        $tx = await($client->beginTransaction());

        $row = await($tx->fetchOne('SELECT val FROM txn_fetch_one ORDER BY id ASC LIMIT 1'));

        expect($row)->toBeArray()
            ->and($row['val'])->toBe('first')
        ;

        await($tx->commit());
        $client->close();
    });

    it('returns null when no rows match', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_one_empty (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $row = await($tx->fetchOne('SELECT val FROM txn_fetch_one_empty WHERE id = -1'));

        expect($row)->toBeNull();

        await($tx->commit());
        $client->close();
    });

    it('supports parameterised queries', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_one_params (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_fetch_one_params (val) VALUES ('target'), ('other')"
        ));

        $tx = await($client->beginTransaction());

        $row = await($tx->fetchOne(
            'SELECT val FROM txn_fetch_one_params WHERE val = $1',
            ['target']
        ));

        expect($row['val'])->toBe('target');

        await($tx->commit());
        $client->close();
    });

    it('sees its own uncommitted writes', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_one_dirty (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        await($tx->query("INSERT INTO txn_fetch_one_dirty (val) VALUES ('dirty_read')"));

        $row = await($tx->fetchOne('SELECT val FROM txn_fetch_one_dirty LIMIT 1'));

        expect($row['val'])->toBe('dirty_read');

        await($tx->rollback());
        $client->close();
    });

    it('throws when called after commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->commit());

        expect(fn () => await($tx->fetchOne('SELECT 1')))
            ->toThrow(TransactionException::class, 'transaction is no longer active')
        ;

        $client->close();
    });
});

describe('Transaction::fetchValue()', function (): void {

    it('returns the first column of the first row by default', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $value = await($tx->fetchValue('SELECT 42 AS answer'));

        expect($value)->toBe('42');

        await($tx->commit());
        $client->close();
    });

    it('returns a column by name', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_val_name (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_fetch_val_name (val) VALUES ('named')"
        ));

        $tx = await($client->beginTransaction());

        $value = await($tx->fetchValue(
            'SELECT id, val FROM txn_fetch_val_name LIMIT 1',
            'val'
        ));

        expect($value)->toBe('named');

        await($tx->commit());
        $client->close();
    });

    it('returns a column by zero-based integer index', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $value = await($tx->fetchValue('SELECT 10 AS a, 20 AS b, 30 AS c', 2));

        expect($value)->toBe('30');

        await($tx->commit());
        $client->close();
    });

    it('returns null when no rows match', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_val_empty (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $value = await($tx->fetchValue(
            'SELECT val FROM txn_fetch_val_empty WHERE id = -1'
        ));

        expect($value)->toBeNull();

        await($tx->commit());
        $client->close();
    });

    it('supports parameterised queries', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_fetch_val_params (id serial PRIMARY KEY, score int)'
        ));
        await($client->query(
            'INSERT INTO txn_fetch_val_params (score) VALUES (99)'
        ));

        $tx = await($client->beginTransaction());

        $value = await($tx->fetchValue(
            'SELECT score FROM txn_fetch_val_params WHERE score = $1',
            null,
            [99]
        ));

        expect((int) $value)->toBe(99);

        await($tx->commit());
        $client->close();
    });

    it('throws when called after commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->commit());

        expect(fn () => await($tx->fetchValue('SELECT 1')))
            ->toThrow(TransactionException::class, 'transaction is no longer active')
        ;

        $client->close();
    });
});

describe('Transaction::stream()', function (): void {

    it('streams all rows from a simple query', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_stream_basic (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_stream_basic (val) VALUES ('a'), ('b'), ('c')"
        ));

        $tx = await($client->beginTransaction());

        $stream = await($tx->stream('SELECT val FROM txn_stream_basic ORDER BY id ASC'));

        $rows = [];

        foreach ($stream as $row) {
            $rows[] = $row['val'];
        }

        expect($rows)->toBe(['a', 'b', 'c']);

        await($tx->commit());
        $client->close();
    });

    it('streams rows using parameterised queries', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_stream_params (id serial PRIMARY KEY, kind text)'
        ));
        await($client->query(
            "INSERT INTO txn_stream_params (kind) VALUES ('keep'), ('drop'), ('keep'), ('drop')"
        ));

        $tx = await($client->beginTransaction());

        $stream = await($tx->stream(
            'SELECT kind FROM txn_stream_params WHERE kind = $1 ORDER BY id ASC',
            ['keep']
        ));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row['kind'];
        }

        expect($rows)->toHaveCount(2)
            ->and($rows[0])->toBe('keep')
            ->and($rows[1])->toBe('keep')
        ;

        await($tx->commit());
        $client->close();
    });

    it('streams an empty result set without error', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_stream_empty (id serial PRIMARY KEY)'
        ));

        $tx = await($client->beginTransaction());

        $stream = await($tx->stream('SELECT id FROM txn_stream_empty'));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toBeEmpty();

        await($tx->commit());
        $client->close();
    });

    it('respects the buffer size option', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_stream_buffer (id serial PRIMARY KEY)'
        ));
        await($client->query(
            'INSERT INTO txn_stream_buffer DEFAULT VALUES;'
                . 'INSERT INTO txn_stream_buffer DEFAULT VALUES;'
                . 'INSERT INTO txn_stream_buffer DEFAULT VALUES;'
                . 'INSERT INTO txn_stream_buffer DEFAULT VALUES;'
                . 'INSERT INTO txn_stream_buffer DEFAULT VALUES;'
        ));

        $tx = await($client->beginTransaction());

        $stream = await($tx->stream('SELECT id FROM txn_stream_buffer ORDER BY id ASC', [], 2));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(5);

        await($tx->commit());
        $client->close();
    });

    it('sees its own uncommitted writes', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_stream_dirty (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        await($tx->query("INSERT INTO txn_stream_dirty (val) VALUES ('uncommitted')"));

        $stream = await($tx->stream('SELECT val FROM txn_stream_dirty'));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row['val'];
        }

        expect($rows)->toBe(['uncommitted']);

        await($tx->rollback());
        $client->close();
    });

    it('throws when called after commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->commit());

        expect(fn () => await($tx->stream('SELECT 1')))
            ->toThrow(TransactionException::class, 'transaction is no longer active')
        ;

        $client->close();
    });

    it('marks the transaction as failed when the stream query itself fails', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $error = null;

        try {
            await($tx->stream('SELECT * FROM definitely_does_not_exist_stream'));
        } catch (QueryException $e) {
            $error = $e;
        }

        expect($error)->not->toBeNull();

        expect(fn () => await($tx->query('SELECT 1')))
            ->toThrow(TransactionException::class, 'Transaction aborted due to a previous query error')
        ;

        await($tx->rollback());
        $client->close();
    });
});

describe('Transaction::prepare() / TransactionPreparedStatement', function (): void {

    it('executes a prepared statement inside the transaction', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_prepare_basic (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $stmt = await($tx->prepare('INSERT INTO txn_prepare_basic (val) VALUES ($1)'));

        await($stmt->execute(['first']));
        await($stmt->execute(['second']));

        await($stmt->close());
        await($tx->commit());

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_prepare_basic'));
        expect((int) $result->fetchOne()['c'])->toBe(2);

        $client->close();
    });

    it('streams results via an in-transaction prepared statement', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_prepare_stream (id serial PRIMARY KEY, val text)'
        ));
        await($client->query(
            "INSERT INTO txn_prepare_stream (val) VALUES ('x'), ('y'), ('z')"
        ));

        $tx = await($client->beginTransaction());

        $stmt = await($tx->prepare(
            'SELECT val FROM txn_prepare_stream ORDER BY id ASC'
        ));

        $stream = await($stmt->executeStream());

        $rows = [];

        foreach ($stream as $row) {
            $rows[] = $row['val'];
        }

        expect($rows)->toBe(['x', 'y', 'z']);

        await($stmt->close());
        await($tx->commit());
        $client->close();
    });

    it('rolls back changes made through a prepared statement', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_prepare_rollback (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());

        $stmt = await($tx->prepare('INSERT INTO txn_prepare_rollback (val) VALUES ($1)'));
        await($stmt->execute(['will_vanish']));

        await($stmt->close());
        await($tx->rollback());

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_prepare_rollback'));
        expect((int) $result->fetchOne()['c'])->toBe(0);

        $client->close();
    });

    it('returns Promise::resolved() from close() when already closed', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $stmt = await($tx->prepare('SELECT 1'));
        await($stmt->close());

        $secondClose = $stmt->close();
        expect($secondClose)->toBeInstanceOf(Hibla\Promise\Interfaces\PromiseInterface::class);
        await($secondClose);

        await($tx->commit());
        $client->close();
    });

    it('throws when prepare is called after commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->commit());

        expect(fn () => await($tx->prepare('SELECT 1')))
            ->toThrow(TransactionException::class, 'transaction is no longer active')
        ;

        $client->close();
    });
});

describe('Savepoints', function (): void {

    it('can create a savepoint, rollback to it, and commit the rest', function (): void {
        $client = makeClient();

        await($client->query(
            'CREATE TEMPORARY TABLE txn_savepoint (id serial PRIMARY KEY, val text)'
        ));

        $tx = await($client->beginTransaction());
        await($tx->query("INSERT INTO txn_savepoint (val) VALUES ('A')"));

        await($tx->savepoint('sp1'));
        await($tx->query("INSERT INTO txn_savepoint (val) VALUES ('B')"));

        await($tx->rollbackTo('sp1'));
        await($tx->query("INSERT INTO txn_savepoint (val) VALUES ('C')"));

        await($tx->commit());

        $result = await($client->query('SELECT val FROM txn_savepoint ORDER BY id ASC'));
        $rows = $result->fetchAll();

        expect($rows)->toHaveCount(2)
            ->and($rows[0]['val'])->toBe('A')
            ->and($rows[1]['val'])->toBe('C')
        ;

        $client->close();
    });

    it('can release a savepoint', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());
        await($tx->savepoint('sp2'));
        await($tx->releaseSavepoint('sp2'));

        await($tx->commit());

        expect(true)->toBeTrue();

        $client->close();
    });
});

describe('Postgres Error States (Tainted Transactions)', function (): void {

    it('marks transaction as failed and rejects subsequent queries if a query fails', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $error = null;

        try {
            await($tx->query('SELECT * FROM definitely_does_not_exist'));
        } catch (QueryException $e) {
            $error = $e;
        }

        expect($error)->not->toBeNull();

        expect(fn () => await($tx->query('SELECT 1')))
            ->toThrow(TransactionException::class, 'Transaction aborted due to a previous query error')
        ;

        await($tx->rollback());

        $client->close();
    });

    it('prevents commit on a tainted transaction', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        try {
            await($tx->query('INVALID SQL SYNTAX !!!'));
        } catch (Throwable) {
        }

        expect(fn () => await($tx->commit()))
            ->toThrow(TransactionException::class, 'Transaction aborted due to a previous query error')
        ;

        await($tx->rollback());
        $client->close();
    });

    it('clears the tainted state when rolling back to a valid savepoint', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        await($tx->query('SELECT 1 AS ok'));
        await($tx->savepoint('safe_place'));

        try {
            await($tx->query('INVALID SQL SYNTAX !!!'));
        } catch (Throwable) {
        }

        expect(fn () => await($tx->query('SELECT 2')))
            ->toThrow(TransactionException::class)
        ;

        await($tx->rollbackTo('safe_place'));

        $result = await($tx->query('SELECT 3 AS recovered'));
        expect((int) $result->fetchOne()['recovered'])->toBe(3);

        await($tx->commit());
        $client->close();
    });
});

describe('Transaction Event Hooks', function (): void {

    it('fires onCommit callbacks only after a successful commit', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $fired = false;
        $tx->onCommit(function () use (&$fired) {
            $fired = true;
        });

        await($tx->query('SELECT 1'));
        expect($fired)->toBeFalse();

        await($tx->commit());
        expect($fired)->toBeTrue();

        $client->close();
    });

    it('fires onRollback callbacks only after a rollback', function (): void {
        $client = makeClient();

        $tx = await($client->beginTransaction());

        $firedRollback = false;
        $firedCommit = false;

        $tx->onRollback(function () use (&$firedRollback) {
            $firedRollback = true;
        });
        $tx->onCommit(function () use (&$firedCommit) {
            $firedCommit = true;
        });

        await($tx->rollback());

        expect($firedRollback)->toBeTrue()
            ->and($firedCommit)->toBeFalse()
        ;

        $client->close();
    });

    it('fires onRollback when auto-managed transaction() fails', function (): void {
        $client = makeClient();

        $firedRollback = false;

        try {
            await($client->transaction(function ($tx) use (&$firedRollback) {
                $tx->onRollback(function () use (&$firedRollback) {
                    $firedRollback = true;
                });

                throw new RuntimeException('fail');
            }));
        } catch (RuntimeException) {
        }

        expect($firedRollback)->toBeTrue();

        $client->close();
    });
});

describe('Transaction Lifecycle & GC', function (): void {

    it('auto-rolls back and releases connection when garbage collected without commit/rollback', function (): void {
        $client = makeClient(['maxConnections' => 1]);

        await($client->query(
            'CREATE TEMPORARY TABLE txn_gc_test (id serial PRIMARY KEY, val text)'
        ));

        $createTx = function () use ($client) {
            $tx = await($client->beginTransaction());
            await($tx->query("INSERT INTO txn_gc_test (val) VALUES ('abandoned')"));
        };

        $createTx();

        gc_collect_cycles();

        await(delay(0.1));

        expect($client->stats['active_connections'])->toBe(0);

        $result = await($client->query('SELECT COUNT(*) AS c FROM txn_gc_test'));
        expect((int) $result->fetchOne()['c'])->toBe(0);

        $client->close();
    });
});
