<?php

declare(strict_types=1);

namespace Hibla\Postgres\Manager;

use function Hibla\async;
use function Hibla\await;

use Hibla\Postgres\Enums\IsolationLevel;
use Hibla\Postgres\Exceptions\NotInTransactionException;
use Hibla\Postgres\Exceptions\TransactionException;
use Hibla\Postgres\Exceptions\TransactionFailedException;
use Hibla\Postgres\Utilities\QueryExecutor;
use Hibla\Postgres\Utilities\Transaction;
use Hibla\Promise\Interfaces\PromiseInterface;
use PgSql\Connection;

use Throwable;
use WeakMap;

/**
 * Manages database transactions and their callbacks.
 *
 * This class handles transaction lifecycle including begin/commit/rollback operations,
 * retry logic, isolation level management, and execution of registered callbacks.
 */
final class TransactionManager
{
    /** @var WeakMap<Connection, array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null}> Transaction callbacks using WeakMap */
    private WeakMap $transactionCallbacks;

    /** @var Connection|null Current transaction connection for the active fiber tree */
    private ?Connection $currentTransactionConnection = null;

    /**
     * Creates a new TransactionManager instance.
     */
    public function __construct()
    {
        $this->transactionCallbacks = new WeakMap();
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * This method can only be called from within an active transaction.
     * The callback will be executed after the transaction successfully commits
     * but before the transaction() method returns.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     *
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public function onCommit(callable $callback): void
    {
        $connection = $this->getCurrentTransactionConnection();

        if ($connection === null) {
            throw new NotInTransactionException(
                'onCommit() can only be called within a transaction.'
            );
        }

        if (! isset($this->transactionCallbacks[$connection])) {
            throw new TransactionException('Transaction state not found.');
        }

        $transactionData = $this->transactionCallbacks[$connection];
        $commitCallbacks = $transactionData['commit'];
        $commitCallbacks[] = $callback;

        $this->transactionCallbacks[$connection] = [
            'commit' => $commitCallbacks,
            'rollback' => $transactionData['rollback'],
            'fiber' => $transactionData['fiber'],
        ];
    }

    /**
     * Registers a callback to execute when the current transaction rolls back.
     *
     * This method can only be called from within an active transaction.
     * The callback will be executed after the transaction is rolled back
     * but before the exception is re-thrown.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     *
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public function onRollback(callable $callback): void
    {
        $connection = $this->getCurrentTransactionConnection();

        if ($connection === null) {
            throw new NotInTransactionException(
                'onRollback() can only be called within a transaction.'
            );
        }

        if (! isset($this->transactionCallbacks[$connection])) {
            throw new TransactionException('Transaction state not found.');
        }

        $transactionData = $this->transactionCallbacks[$connection];
        $rollbackCallbacks = $transactionData['rollback'];
        $rollbackCallbacks[] = $callback;

        $this->transactionCallbacks[$connection] = [
            'commit' => $transactionData['commit'],
            'rollback' => $rollbackCallbacks,
            'fiber' => $transactionData['fiber'],
        ];
    }

    /**
     * Executes a transaction with retry logic and optional isolation level.
     *
     * Automatically handles transaction begin/commit/rollback. The callback receives
     * a Transaction object for executing queries. If the callback throws an exception,
     * the transaction is rolled back and retried based on the specified number of attempts.
     *
     * Registered onCommit() callbacks are executed after successful commit.
     * Registered onRollback() callbacks are executed after rollback.
     *
     * @param  callable(): PromiseInterface<Connection>  $getConnection  Callback to acquire connection
     * @param  callable(Connection): void  $releaseConnection  Callback to release connection
     * @param  callable(Transaction): mixed  $callback  Transaction callback receiving Transaction object
     * @param  QueryExecutor  $queryExecutor  Query executor instance
     * @param  int  $attempts  Number of times to attempt the transaction
     * @param  IsolationLevel|null  $isolationLevel  Transaction isolation level (optional)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws TransactionFailedException If transaction fails after all attempts
     * @throws \InvalidArgumentException If attempts is less than 1
     */
    public function executeTransaction(
        callable $getConnection,
        callable $releaseConnection,
        callable $callback,
        QueryExecutor $queryExecutor,
        int $attempts,
        ?IsolationLevel $isolationLevel = null
    ): PromiseInterface {
        return async(function () use ($getConnection, $releaseConnection, $callback, $queryExecutor, $attempts, $isolationLevel) {
            if ($attempts < 1) {
                throw new \InvalidArgumentException('Transaction attempts must be at least 1.');
            }

            /** @var Throwable|null $lastException */
            $lastException = null;

            /** @var list<array{attempt: int, error: string, time: float}> */
            $attemptHistory = [];

            for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
                $connection = null;
                $startTime = microtime(true);

                try {
                    $connection = await($getConnection());
                    $result = await($this->runTransaction($connection, $callback, $queryExecutor, $isolationLevel));

                    return $result;
                } catch (Throwable $e) {
                    $lastException = $e;

                    $attemptHistory[] = [
                        'attempt' => $currentAttempt,
                        'error' => $e->getMessage(),
                        'time' => microtime(true) - $startTime,
                    ];

                    if ($currentAttempt < $attempts) {
                        continue;
                    }

                    throw new TransactionFailedException(
                        sprintf(
                            'Transaction failed after %d attempt(s): %s',
                            $attempts,
                            $e->getMessage()
                        ),
                        $attempts,
                        $e,
                        $attemptHistory
                    );
                } finally {
                    if ($connection !== null) {
                        $releaseConnection($connection);
                    }
                }
            }

            if ($lastException !== null) {
                throw new TransactionFailedException(
                    sprintf('Transaction failed after %d attempt(s)', $attempts),
                    $attempts,
                    $lastException,
                    $attemptHistory
                );
            }

            throw new TransactionException('Transaction failed without exception.');
        });
    }

    /**
     * Runs a single transaction attempt.
     *
     * Executes BEGIN, creates Transaction object, runs callback, and either COMMIT or ROLLBACK.
     * Manages transaction state and executes appropriate callbacks.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @param  callable(Transaction): mixed  $callback  Transaction callback receiving Transaction object
     * @param  QueryExecutor  $queryExecutor  Query executor instance
     * @param  IsolationLevel|null  $isolationLevel  Transaction isolation level (optional)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws TransactionException If BEGIN, COMMIT, or isolation level setting fails
     * @throws Throwable If callback throws (after ROLLBACK)
     */
    private function runTransaction(
        Connection $connection,
        callable $callback,
        QueryExecutor $queryExecutor,
        ?IsolationLevel $isolationLevel = null
    ): PromiseInterface {
        return async(function () use ($connection, $callback, $queryExecutor, $isolationLevel) {
            $currentFiber = \Fiber::getCurrent();

            /** @var array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null} $initialState */
            $initialState = [
                'commit' => [],
                'rollback' => [],
                'fiber' => $currentFiber,
            ];

            $this->transactionCallbacks[$connection] = $initialState;

            $previousTransactionConnection = $this->currentTransactionConnection;
            $this->currentTransactionConnection = $connection;

            try {
                $beginSql = 'BEGIN';
                if ($isolationLevel !== null) {
                    $beginSql = "BEGIN ISOLATION LEVEL {$isolationLevel->value}";
                }

                $beginResult = @pg_query($connection, $beginSql);
                if ($beginResult === false) {
                    throw new TransactionException(
                        'Failed to begin transaction: ' . pg_last_error($connection)
                    );
                }

                $transaction = new Transaction($connection, $queryExecutor, $this);
                $result = $callback($transaction);

                if ($result instanceof PromiseInterface) {
                    $result = await($result);
                }

                $commitResult = @pg_query($connection, 'COMMIT');
                if ($commitResult === false) {
                    throw new TransactionException(
                        'Failed to commit transaction: ' . pg_last_error($connection)
                    );
                }

                $this->executeCallbacks($connection, 'commit');

                return $result;
            } catch (Throwable $e) {
                @pg_query($connection, 'ROLLBACK');

                $this->executeCallbacks($connection, 'rollback');

                throw $e;
            } finally {
                $this->currentTransactionConnection = $previousTransactionConnection;

                if (isset($this->transactionCallbacks[$connection])) {
                    unset($this->transactionCallbacks[$connection]);
                }
            }
        });
    }

    /**
     * Gets the current transaction's Connection instance if in a transaction.
     *
     * This method returns the connection for the active transaction context.
     *
     * @return Connection|null Connection instance or null if not in transaction
     */
    public function getCurrentTransactionConnection(): ?Connection
    {
        return $this->currentTransactionConnection;
    }

    /**
     * Executes registered callbacks for commit or rollback.
     *
     * Runs all callbacks registered for the specified transaction event.
     * If any callback throws an exception, execution stops and the first
     * exception is re-thrown after all callbacks have been attempted.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @param  string  $type  'commit' or 'rollback'
     * @return void
     *
     * @throws TransactionException If any callback throws an exception
     */
    private function executeCallbacks(Connection $connection, string $type): void
    {
        if (! isset($this->transactionCallbacks[$connection])) {
            return;
        }

        $transactionData = $this->transactionCallbacks[$connection];

        if ($type !== 'commit' && $type !== 'rollback') {
            return;
        }

        $callbacks = $transactionData[$type];

        /** @var list<Throwable> $exceptions */
        $exceptions = [];

        foreach ($callbacks as $callback) {
            try {
                $callback();
            } catch (Throwable $e) {
                $exceptions[] = $e;
            }
        }

        if (count($exceptions) > 0) {
            throw new TransactionException(
                sprintf(
                    'Transaction %s callback failed: %s',
                    $type,
                    $exceptions[0]->getMessage()
                ),
                0,
                $exceptions[0]
            );
        }
    }
}
