<?php

declare(strict_types=1);

use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConstraintViolationException;
use Hibla\Sql\Exceptions\DeadlockException;
use Hibla\Sql\Exceptions\LockWaitTimeoutException;

use function Hibla\async;
use function Hibla\await;

describe('Postgres Exception Detection', function () {

    it('detects various constraint violations', function () {
        $client = makeClient();

        await($client->execute('DROP TABLE IF EXISTS constraint_test'));
        await($client->execute('CREATE TABLE constraint_test (
            id INT PRIMARY KEY, 
            email TEXT UNIQUE, 
            age INT NOT NULL CHECK (age >= 18)
        )'));

        await($client->execute("INSERT INTO constraint_test (id, email, age) VALUES (1, 'test@example.com', 25)"));

        expect(fn () => await($client->execute("INSERT INTO constraint_test (id, email, age) VALUES (1, 'other@example.com', 30)")))
            ->toThrow(ConstraintViolationException::class)
        ;

        expect(fn () => await($client->execute("INSERT INTO constraint_test (id, email, age) VALUES (2, 'null@example.com', NULL)")))
            ->toThrow(ConstraintViolationException::class)
        ;

        expect(fn () => await($client->execute("INSERT INTO constraint_test (id, email, age) VALUES (3, 'young@example.com', 16)")))
            ->toThrow(ConstraintViolationException::class)
        ;

        $client->close();
    });

    it('detects lock wait timeouts', function () {
        $client = makeClient(['maxConnections' => 5]);

        await($client->execute('DROP TABLE IF EXISTS lockwait_test'));
        await($client->execute('CREATE TABLE lockwait_test (id INT PRIMARY KEY, val INT)'));
        await($client->execute('INSERT INTO lockwait_test (id, val) VALUES (1, 10)'));

        $tx1 = await($client->beginTransaction());
        $tx2 = await($client->beginTransaction());

        await($tx2->execute("SET lock_timeout = '100ms'"));

        await($tx1->execute('UPDATE lockwait_test SET val = 11 WHERE id = 1'));

        expect(fn () => await($tx2->execute('UPDATE lockwait_test SET val = 12 WHERE id = 1')))
            ->toThrow(LockWaitTimeoutException::class)
        ;

        await($tx1->rollback());
        await($tx2->rollback());
        $client->close();
    });

    it('detects deadlocks', function () {
        $client = makeClient(['maxConnections' => 5]);

        await($client->execute('DROP TABLE IF EXISTS deadlock_test'));
        await($client->execute('CREATE TABLE deadlock_test (id INT PRIMARY KEY, val INT)'));
        await($client->execute('INSERT INTO deadlock_test (id, val) VALUES (1, 10), (2, 20)'));

        $tx1 = await($client->beginTransaction());
        $tx2 = await($client->beginTransaction());

        await($tx1->execute('UPDATE deadlock_test SET val = 11 WHERE id = 1'));

        await($tx2->execute('UPDATE deadlock_test SET val = 22 WHERE id = 2'));

        $f1 = async(fn () => await($tx1->execute('UPDATE deadlock_test SET val = 21 WHERE id = 2')));
        $f2 = async(fn () => await($tx2->execute('UPDATE deadlock_test SET val = 12 WHERE id = 1')));

        $error = null;

        try {
            await(Promise::all([$f1, $f2]));
        } catch (Throwable $e) {
            $error = $e;
        }

        expect($error)->toBeInstanceOf(DeadlockException::class);

        $tx1->isClosed() ?: await($tx1->rollback());
        $tx2->isClosed() ?: await($tx2->rollback());
        $client->close();
    });

    it('identifies standard query errors as QueryException', function () {
        $client = makeClient();

        expect(fn () => await($client->query('SELECT * FROM non_existent_table_xyz')))
            ->toThrow(Hibla\Sql\Exceptions\QueryException::class)
        ;

        $client->close();
    });
});
