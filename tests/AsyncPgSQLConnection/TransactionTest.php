<?php

declare(strict_types=1);

use Hibla\Postgres\AsyncPgSQLConnection;
use Hibla\Postgres\Enums\IsolationLevel;
use Hibla\Postgres\Exceptions\TransactionFailedException;
use Hibla\Promise\Promise;

use Tests\Helpers\TestHelper;

describe('AsyncPgSQLConnection Transactions', function () {
    it('commits successful transaction', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $result = $db->transaction(function ($trx) {
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Alice', 1000.00)");
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Bob', 2000.00)");

            return 'success';
        })->await();

        expect($result)->toBe('success');

        $count = $db->fetchValue('SELECT COUNT(*) FROM accounts')->await();
        expect($count)->toBe('2');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('rolls back transaction on exception', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        try {
            $db->transaction(function ($trx) {
                $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Charlie', 500.00)");

                throw new Exception('Simulated error');
            })->await();
        } catch (TransactionFailedException $e) {
            // Expected
        }

        $count = $db->fetchValue('SELECT COUNT(*) FROM accounts')->await();
        expect($count)->toBe('0');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('performs money transfer with transaction', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $db->execute("INSERT INTO accounts (name, balance) VALUES ('Alice', 1000.00)")->await();
        $db->execute("INSERT INTO accounts (name, balance) VALUES ('Bob', 500.00)")->await();

        $db->transaction(function ($trx) {
            $transferAmount = 300.00;

            $trx->execute('UPDATE accounts SET balance = balance - $1 WHERE name = $2', [$transferAmount, 'Alice']);
            $trx->execute('UPDATE accounts SET balance = balance + $1 WHERE name = $2', [$transferAmount, 'Bob']);
        })->await();

        $aliceBalance = $db->fetchValue("SELECT balance FROM accounts WHERE name = 'Alice'")->await();
        $bobBalance = $db->fetchValue("SELECT balance FROM accounts WHERE name = 'Bob'")->await();

        expect($aliceBalance)->toBe('700.00')
            ->and($bobBalance)->toBe('800.00')
        ;

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('retries transaction on failure', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $attempts = 0;

        try {
            $db->transaction(function ($trx) use (&$attempts) {
                $attempts++;

                $trx->execute("INSERT INTO accounts (name, balance) VALUES ('David', 100.00)");

                if ($attempts < 3) {
                    throw new Exception('Retry me');
                }

                return 'completed';
            }, 3)->await();
        } catch (TransactionFailedException $e) {
            // Should not reach here
        }

        expect($attempts)->toBe(3);

        $count = $db->fetchValue('SELECT COUNT(*) FROM accounts')->await();
        expect($count)->toBe('1');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('fails after max retry attempts', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $attempts = 0;

        expect(function () use ($db, &$attempts) {
            $db->transaction(function ($trx) use (&$attempts) {
                $attempts++;

                throw new Exception('Always fail');
            }, 2)->await();
        })->toThrow(TransactionFailedException::class);

        expect($attempts)->toBe(2);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('returns value from transaction', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $insertedId = $db->transaction(function ($trx) {
            $result = $trx->query("INSERT INTO accounts (name, balance) VALUES ('Eve', 750.00) RETURNING id");

            return $result[0]['id'];
        })->await();

        expect($insertedId)->toBeString();

        $account = $db->fetchOne('SELECT * FROM accounts WHERE id = $1', [$insertedId])->await();
        expect($account['name'])->toBe('Eve');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('handles nested queries within transaction', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $db->transaction(function ($trx) {
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Frank', 1500.00)");

            $account = $trx->fetchOne("SELECT * FROM accounts WHERE name = 'Frank'");

            expect($account['balance'])->toBe('1500.00');

            $trx->execute('UPDATE accounts SET balance = $1 WHERE name = $2', [2000.00, 'Frank']);
        })->await();

        $balance = $db->fetchValue("SELECT balance FROM accounts WHERE name = 'Frank'")->await();
        expect($balance)->toBe('2000.00');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('isolates transactions across concurrent operations', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $db->execute("INSERT INTO accounts (name, balance) VALUES ('Grace', 1000.00)")->await();

        $promise1 = $db->transaction(function ($trx) {
            $row = $trx->fetchOne("SELECT balance FROM accounts WHERE name = 'Grace' FOR UPDATE");
            $currentBalance = (float)$row['balance'];

            $trx->execute('UPDATE accounts SET balance = $1 WHERE name = $2', [$currentBalance + 100, 'Grace']);

            return $currentBalance + 100;
        });

        $promise2 = $db->transaction(function ($trx) {
            $row = $trx->fetchOne("SELECT balance FROM accounts WHERE name = 'Grace' FOR UPDATE");
            $currentBalance = (float)$row['balance'];

            $trx->execute('UPDATE accounts SET balance = $1 WHERE name = $2', [$currentBalance + 200, 'Grace']);

            return $currentBalance + 200;
        });

        Promise::all([$promise1, $promise2])->await();

        $finalBalance = $db->fetchValue("SELECT balance FROM accounts WHERE name = 'Grace'")->await();
        expect((float)$finalBalance)->toBe(1300.00);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('executes onCommit callback after successful transaction', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $commitCallbackExecuted = false;
        $insertedName = null;

        $db->transaction(function ($trx) use (&$commitCallbackExecuted, &$insertedName) {
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Helen', 500.00)");

            $trx->onCommit(function () use (&$commitCallbackExecuted, &$insertedName) {
                $commitCallbackExecuted = true;
                $insertedName = 'Helen';
            });
        })->await();

        expect($commitCallbackExecuted)->toBeTrue()
            ->and($insertedName)->toBe('Helen')
        ;

        $count = $db->fetchValue('SELECT COUNT(*) FROM accounts')->await();
        expect($count)->toBe('1');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('executes onRollback callback after transaction failure', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $rollbackCallbackExecuted = false;
        $errorMessage = null;

        try {
            $db->transaction(function ($trx) use (&$rollbackCallbackExecuted, &$errorMessage) {
                $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Ivan', 300.00)");

                $trx->onRollback(function () use (&$rollbackCallbackExecuted, &$errorMessage) {
                    $rollbackCallbackExecuted = true;
                    $errorMessage = 'Transaction rolled back';
                });

                throw new Exception('Force rollback');
            })->await();
        } catch (TransactionFailedException $e) {
            // Expected
        }

        expect($rollbackCallbackExecuted)->toBeTrue()
            ->and($errorMessage)->toBe('Transaction rolled back')
        ;

        $count = $db->fetchValue('SELECT COUNT(*) FROM accounts')->await();
        expect($count)->toBe('0');

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('executes multiple onCommit callbacks in order', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $executionOrder = [];

        $db->transaction(function ($trx) use (&$executionOrder) {
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Jack', 100.00)");

            $trx->onCommit(function () use (&$executionOrder) {
                $executionOrder[] = 'first';
            });

            $trx->onCommit(function () use (&$executionOrder) {
                $executionOrder[] = 'second';
            });

            $trx->onCommit(function () use (&$executionOrder) {
                $executionOrder[] = 'third';
            });
        })->await();

        expect($executionOrder)->toBe(['first', 'second', 'third']);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('executes multiple onRollback callbacks in order', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $executionOrder = [];

        try {
            $db->transaction(function ($trx) use (&$executionOrder) {
                $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Kate', 200.00)");

                $trx->onRollback(function () use (&$executionOrder) {
                    $executionOrder[] = 'first';
                });

                $trx->onRollback(function () use (&$executionOrder) {
                    $executionOrder[] = 'second';
                });

                $trx->onRollback(function () use (&$executionOrder) {
                    $executionOrder[] = 'third';
                });

                throw new Exception('Force rollback');
            })->await();
        } catch (TransactionFailedException $e) {
            // Expected
        }

        expect($executionOrder)->toBe(['first', 'second', 'third']);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('does not execute onCommit callbacks when transaction rolls back', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $commitExecuted = false;
        $rollbackExecuted = false;

        try {
            $db->transaction(function ($trx) use (&$commitExecuted, &$rollbackExecuted) {
                $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Leo', 400.00)");

                $trx->onCommit(function () use (&$commitExecuted) {
                    $commitExecuted = true;
                });

                $trx->onRollback(function () use (&$rollbackExecuted) {
                    $rollbackExecuted = true;
                });

                throw new Exception('Force rollback');
            })->await();
        } catch (TransactionFailedException $e) {
            // Expected
        }

        expect($commitExecuted)->toBeFalse()
            ->and($rollbackExecuted)->toBeTrue()
        ;

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('does not execute onRollback callbacks when transaction commits', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $commitExecuted = false;
        $rollbackExecuted = false;

        $db->transaction(function ($trx) use (&$commitExecuted, &$rollbackExecuted) {
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Maria', 600.00)");

            $trx->onCommit(function () use (&$commitExecuted) {
                $commitExecuted = true;
            });

            $trx->onRollback(function () use (&$rollbackExecuted) {
                $rollbackExecuted = true;
            });
        })->await();

        expect($commitExecuted)->toBeTrue()
            ->and($rollbackExecuted)->toBeFalse()
        ;

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('can use onCommit to log successful operations', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $auditLog = [];

        $db->transaction(function ($trx) use (&$auditLog) {
            $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Nina', 1500.00)");

            $account = $trx->fetchOne("SELECT * FROM accounts WHERE name = 'Nina'");

            $trx->onCommit(function () use (&$auditLog, $account) {
                $auditLog[] = [
                    'action' => 'account_created',
                    'account_id' => $account['id'],
                    'account_name' => $account['name'],
                    'timestamp' => date('Y-m-d H:i:s'),
                ];
            });
        })->await();

        expect($auditLog)->toHaveCount(1)
            ->and($auditLog[0]['action'])->toBe('account_created')
            ->and($auditLog[0]['account_name'])->toBe('Nina')
        ;

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('can use onRollback to clean up resources', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
        $db->execute('
            CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                balance DECIMAL(10, 2) NOT NULL DEFAULT 0
            )
        ')->await();

        $resourcesAllocated = true;
        $resourcesCleaned = false;

        try {
            $db->transaction(function ($trx) use (&$resourcesAllocated, &$resourcesCleaned) {
                $trx->execute("INSERT INTO accounts (name, balance) VALUES ('Oscar', 800.00)");

                $trx->onRollback(function () use (&$resourcesAllocated, &$resourcesCleaned) {
                    $resourcesAllocated = false;
                    $resourcesCleaned = true;
                });

                throw new Exception('Simulated failure');
            })->await();
        } catch (TransactionFailedException $e) {
            // Expected
        }

        expect($resourcesAllocated)->toBeFalse()
            ->and($resourcesCleaned)->toBeTrue()
        ;

        $db->execute('DROP TABLE IF EXISTS accounts')->await();
    });

    it('uses default READ COMMITTED isolation level when not specified', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $isolationLevel = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        })->await();

        expect($isolationLevel)->toBe('read committed');
    });

    it('sets READ UNCOMMITTED isolation level', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $isolationLevel = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::READ_UNCOMMITTED)->await();

        expect($isolationLevel)->toBe('read uncommitted');
    });

    it('sets READ COMMITTED isolation level', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $isolationLevel = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::READ_COMMITTED)->await();

        expect($isolationLevel)->toBe('read committed');
    });

    it('sets REPEATABLE READ isolation level', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $isolationLevel = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::REPEATABLE_READ)->await();

        expect($isolationLevel)->toBe('repeatable read');
    });

    it('sets SERIALIZABLE isolation level', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $isolationLevel = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        expect($isolationLevel)->toBe('serializable');
    });

    it('changes isolation level across different transactions', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $level1 = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::READ_COMMITTED)->await();

        $level2 = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        $level3 = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::REPEATABLE_READ)->await();

        expect($level1)->toBe('read committed')
            ->and($level2)->toBe('serializable')
            ->and($level3)->toBe('repeatable read')
        ;
    });

    it('maintains different isolation levels in concurrent transactions', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $results = Promise::all([
            $db->transaction(function ($trx) {
                return $trx->fetchValue('SHOW transaction_isolation');
            }, isolationLevel: IsolationLevel::READ_COMMITTED),

            $db->transaction(function ($trx) {
                return $trx->fetchValue('SHOW transaction_isolation');
            }, isolationLevel: IsolationLevel::REPEATABLE_READ),

            $db->transaction(function ($trx) {
                return $trx->fetchValue('SHOW transaction_isolation');
            }, isolationLevel: IsolationLevel::SERIALIZABLE),
        ])->await();

        expect($results[0])->toBe('read committed')
            ->and($results[1])->toBe('repeatable read')
            ->and($results[2])->toBe('serializable')
        ;
    });

    it('resets isolation level after transaction completes', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->transaction(function ($trx) {
            $level = $trx->fetchValue('SHOW transaction_isolation');
            expect($level)->toBe('serializable');
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        $levelAfter = $db->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        })->await();

        expect($levelAfter)->toBe('read committed');
    });

    it('demonstrates REPEATABLE READ prevents non-repeatable reads', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS test_isolation')->await();
        $db->execute('
            CREATE TABLE test_isolation (
                id INT PRIMARY KEY,
                value INT
            )
        ')->await();
        $db->execute('INSERT INTO test_isolation VALUES (1, 100)')->await();

        $readValues = [];

        Promise::all([
            $db->transaction(function ($trx) use (&$readValues) {
                $value1 = $trx->fetchValue('SELECT value FROM test_isolation WHERE id = 1');
                $readValues['trx1_read1'] = $value1;

                Hibla\sleep(0.1);

                $value2 = $trx->fetchValue('SELECT value FROM test_isolation WHERE id = 1');
                $readValues['trx1_read2'] = $value2;
            }, isolationLevel: IsolationLevel::REPEATABLE_READ),

            $db->transaction(function ($trx) {
                Hibla\sleep(0.05);
                $trx->execute('UPDATE test_isolation SET value = 200 WHERE id = 1');
            }),
        ])->await();

        expect($readValues['trx1_read1'])->toBe('100')
            ->and($readValues['trx1_read2'])->toBe('100')
        ;

        $db->execute('DROP TABLE IF EXISTS test_isolation')->await();
    });

    it('demonstrates READ COMMITTED allows non-repeatable reads', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);

        $db->execute('DROP TABLE IF EXISTS test_isolation')->await();
        $db->execute('
            CREATE TABLE test_isolation (
                id INT PRIMARY KEY,
                value INT
            )
        ')->await();
        $db->execute('INSERT INTO test_isolation VALUES (1, 100)')->await();

        $readValues = [];

        Promise::all([
            $db->transaction(function ($trx) use (&$readValues) {
                $value1 = $trx->fetchValue('SELECT value FROM test_isolation WHERE id = 1');
                $readValues['trx1_read1'] = $value1;

                Hibla\sleep(0.1);

                $value2 = $trx->fetchValue('SELECT value FROM test_isolation WHERE id = 1');
                $readValues['trx1_read2'] = $value2;
            }, isolationLevel: IsolationLevel::READ_COMMITTED),

            $db->transaction(function ($trx) {
                Hibla\sleep(0.05);
                $trx->execute('UPDATE test_isolation SET value = 200 WHERE id = 1');
            }),
        ])->await();

        expect($readValues['trx1_read1'])->toBe('100')
            ->and($readValues['trx1_read2'])->toBe('200') // Sees the update!
        ;

        $db->execute('DROP TABLE IF EXISTS test_isolation')->await();
    });
});