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
