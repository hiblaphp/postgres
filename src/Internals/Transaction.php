<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Cache\ArrayCache;
use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Interfaces\PostgresRowStream;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Postgres\Traits\CancellationHelperTrait;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\TransactionException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\Result as ResultInterface;
use Hibla\Sql\Transaction as TransactionInterface;

/**
 * Transaction implementation with automatic pool management and state protection.
 *
 * @internal Created by PostgresClient::beginTransaction() - do not instantiate directly.
 */
class Transaction implements TransactionInterface
{
    use CancellationHelperTrait;

    /**
     * @var list<callable(): void>
     */
    private array $onCommitCallbacks = [];

    /**
     * @var list<callable(): void>
     */
    private array $onRollbackCallbacks = [];

    private bool $active = true;

    private bool $released = false;

    /**
     * If a query fails mid-transaction, PostgreSQL immediately puts the transaction
     * into an aborted state (INERROR). Any further queries will fail with
     * "current transaction is aborted" until ROLLBACK is issued.
     */
    private bool $failed = false;

    /**
     * @internal Use PostgresClient::beginTransaction() instead.
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly PoolManager $pool,
        private readonly ?ArrayCache $statementCache = null
    ) {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresResult>
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();

        if (\count($params) === 0) {
            $promise = $this->connection->query($sql);

            return $this->withCancellation($this->trackErrorState($promise));
        }

        $innerPromise = null;

        $promise = $this->getCachedStatement($sql)
            ->then(function (array $result) use ($params, &$innerPromise) {
                /** @var PreparedStatement $stmt */
                [$stmt, $isCached] = $result;

                $innerPromise = $stmt->execute($params)
                    ->finally(function () use ($stmt, $isCached) {
                        if (! $isCached) {
                            $stmt->close();
                        }
                    })
                ;

                return $innerPromise;
            })
        ;

        $this->bindInnerCancellation($promise, $innerPromise);

        return $this->withCancellation($this->trackErrorState($promise));
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresRowStream>
     */
    public function stream(string $sql, array $params = [], int $bufferSize = 100): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();

        if (\count($params) === 0) {
            $promise = $this->connection->streamQuery($sql, $bufferSize);

            return $this->withCancellation($this->trackErrorState($promise));
        }

        $innerPromise = null;

        $promise = $this->getCachedStatement($sql)
            ->then(function (array $result) use ($params, $bufferSize, &$innerPromise) {
                /** @var PreparedStatement $stmt */
                [$stmt, $isCached] = $result;

                $innerPromise = $stmt->executeStream($params, $bufferSize)
                    ->then(function (PostgresRowStream $stream) use ($stmt, $isCached): PostgresRowStream {
                        if ($stream instanceof RowStream) {
                            if (! $isCached) {
                                $stream->waitForCommand()->finally($stmt->close(...));
                            }
                        }

                        return $stream;
                    })
                ;

                return $innerPromise;
            })
        ;

        $this->bindInnerCancellation($promise, $innerPromise);

        return $this->withCancellation($this->trackErrorState($promise));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();

        $innerPromise = $this->connection->prepare($sql);

        $promise = $innerPromise->then(
            function (PreparedStatementInterface $stmt) {
                return new TransactionPreparedStatement($stmt, $this->connection);
            }
        );

        $this->bindInnerCancellation($promise, $innerPromise);

        return $this->trackErrorState($promise);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->affectedRows)
        );
    }

    /**
     * {@inheritdoc}
     */
    public function executeGetId(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(function (ResultInterface $result) {
                    $row = $result->fetchOne();

                    if ($row !== null && \count($row) > 0) {
                        $val = reset($row);

                        return \is_scalar($val) ? (int) $val : 0;
                    }

                    return $result->lastInsertId;
                })
        );
    }

    /**
     * {@inheritdoc}
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->fetchOne())
        );
    }

    /**
     * {@inheritdoc}
     */
    public function fetchValue(string $sql, string|int|null $column = null, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(function (ResultInterface $result) use ($column) {
                    $row = $result->fetchOne();

                    if ($row === null) {
                        return null;
                    }

                    if ($column === null) {
                        $value = reset($row);

                        return $value !== false ? $value : null;
                    }

                    if (\is_int($column)) {
                        $values = array_values($row);

                        return $values[$column] ?? null;
                    }

                    return $row[$column] ?? null;
                })
        );
    }

    /**
     * {@inheritdoc}
     */
    public function onCommit(callable $callback): void
    {
        $this->ensureActive();
        $this->onCommitCallbacks[] = $callback;
    }

    /**
     * {@inheritdoc}
     */
    public function onRollback(callable $callback): void
    {
        $this->ensureActive();
        $this->onRollbackCallbacks[] = $callback;
    }

    /**
     * {@inheritdoc}
     *
     * NOTE: withCancellation() is intentionally NOT applied to commit().
     * Dispatching pg_cancel_backend against a COMMIT would leave the transaction
     * in an undefined state on the server. This operation must be allowed
     * to complete or fail on its own terms.
     *
     * @return PromiseInterface<void>
     */
    public function commit(): PromiseInterface
    {
        $this->ensureActive();

        if ($this->failed) {
            return Promise::rejected(
                new TransactionException(
                    'Transaction aborted due to a previous query error. '
                        . 'Call rollback() to abort, or use savepoints to recover from expected failures.'
                )
            );
        }

        $this->active = false;

        return $this->connection->query('COMMIT')
            ->then(
                function (): void {
                    $this->executeCallbacks($this->onCommitCallbacks);
                    $this->onRollbackCallbacks = [];
                },
                function (\Throwable $e): never {
                    throw new TransactionException(
                        'Failed to commit transaction: ' . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * {@inheritdoc}
     *
     * NOTE: withCancellation() is intentionally NOT applied to rollback().
     * Dispatching pg_cancel_backend against a ROLLBACK would leave the transaction
     * in an undefined state on the server. This operation must be allowed
     * to complete or fail on its own terms.
     *
     * @return PromiseInterface<void>
     */
    public function rollback(): PromiseInterface
    {
        $this->ensureActive();
        $this->active = false;
        $this->failed = false;

        return $this->connection->query('ROLLBACK')
            ->then(
                function (): void {
                    $this->executeCallbacks($this->onRollbackCallbacks);
                    $this->onCommitCallbacks = [];
                },
                function (\Throwable $e): never {
                    throw new TransactionException(
                        'Failed to rollback transaction: ' . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
            ->finally($this->releaseConnection(...))
        ;
    }

    /**
     * {@inheritdoc}
     */
    public function savepoint(string $identifier): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();
        $escaped = $this->escapeIdentifier($identifier);

        $promise = $this->connection->query("SAVEPOINT {$escaped}")->catch(
            function (\Throwable $e) use ($identifier): never {
                throw new TransactionException(
                    "Failed to create savepoint '{$identifier}': " . $e->getMessage(),
                    (int) $e->getCode(),
                    $e
                );
            }
        );

        return $this->trackErrorState($promise);
    }

    /**
     * {@inheritdoc}
     */
    public function rollbackTo(string $identifier): PromiseInterface
    {
        $this->ensureActive();
        $escaped = $this->escapeIdentifier($identifier);

        // Rolling back to a savepoint potentially clears the failed state for operations after that savepoint
        $this->failed = false;

        return $this->connection->query("ROLLBACK TO SAVEPOINT {$escaped}")->catch(
            function (\Throwable $e) use ($identifier): never {
                $this->failed = true; // If rollback fails, the whole transaction is dead

                throw new TransactionException(
                    "Failed to rollback to savepoint '{$identifier}': " . $e->getMessage(),
                    (int) $e->getCode(),
                    $e
                );
            }
        );
    }

    /**
     * {@inheritdoc}
     */
    public function releaseSavepoint(string $identifier): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();
        $escaped = $this->escapeIdentifier($identifier);

        $promise = $this->connection->query("RELEASE SAVEPOINT {$escaped}")->catch(
            function (\Throwable $e) use ($identifier): never {
                throw new TransactionException(
                    "Failed to release savepoint '{$identifier}': " . $e->getMessage(),
                    (int) $e->getCode(),
                    $e
                );
            }
        );

        return $this->trackErrorState($promise);
    }

    public function isActive(): bool
    {
        return $this->active && ! $this->connection->isClosed();
    }

    public function isClosed(): bool
    {
        return $this->connection->isClosed();
    }

    /**
     * @template T
     *
     * @param PromiseInterface<T> $promise
     *
     * @return PromiseInterface<T>
     */
    private function trackErrorState(PromiseInterface $promise): PromiseInterface
    {
        return $promise->catch(function (\Throwable $e) {
            $this->failed = true;

            throw $e;
        });
    }

    /**
     * @return PromiseInterface<array{0: PreparedStatement, 1: bool}>
     */
    private function getCachedStatement(string $sql): PromiseInterface
    {
        if ($this->statementCache === null) {
            return $this->connection->prepare($sql)->then(fn ($stmt) => [$stmt, false]);
        }

        return $this->statementCache->get($sql)->then(function (mixed $stmt) use ($sql) {
            /** @phpstan-ignore instanceof.alwaysFalse */
            if ($stmt instanceof PreparedStatement) {
                return [$stmt, true];
            }

            return $this->connection->prepare($sql)->then(function (PreparedStatement $newStmt) use ($sql) {
                $this->statementCache->set($sql, $newStmt);

                return [$newStmt, true];
            });
        });
    }

    private function releaseConnection(): void
    {
        if ($this->released) {
            return;
        }

        $this->onCommitCallbacks = [];
        $this->onRollbackCallbacks = [];
        $this->released = true;
        $this->pool->release($this->connection);
    }

    /**
     * @param list<callable(): void> $callbacks
     */
    private function executeCallbacks(array $callbacks): void
    {
        foreach ($callbacks as $callback) {
            $callback();
        }
    }

    private function ensureActive(): void
    {
        if ($this->connection->isClosed()) {
            throw new TransactionException('Cannot perform operation: connection is closed');
        }

        if (! $this->active) {
            throw new TransactionException('Cannot perform operation: transaction is no longer active');
        }
    }

    private function ensureActiveAndNotFailed(): void
    {
        $this->ensureActive();

        if ($this->failed) {
            throw new TransactionException(
                'Transaction aborted due to a previous query error. '
                    . 'Call rollback() to abort, or use savepoints to recover from expected failures.'
            );
        }
    }

    private function escapeIdentifier(string $identifier): string
    {
        if ($identifier === '') {
            throw new \InvalidArgumentException('Savepoint identifier cannot be empty');
        }

        // Postgres max identifier length is usually 63 bytes
        if (\strlen($identifier) > 63) {
            throw new \InvalidArgumentException('Savepoint identifier too long (max 63 characters)');
        }

        if (strpos($identifier, "\0") !== false) {
            throw new \InvalidArgumentException('Savepoint identifier contains invalid byte values');
        }

        if ($identifier !== trim($identifier)) {
            throw new \InvalidArgumentException('Savepoint identifier cannot start or end with spaces');
        }

        // Postgres uses double quotes to escape identifiers
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    public function __destruct()
    {
        if ($this->active && ! $this->connection->isClosed() && ! $this->released) {
            $this->active = false;
            $this->connection->query('ROLLBACK')->finally($this->releaseConnection(...));
        } elseif (! $this->released) {
            $this->releaseConnection();
        }
    }
}
