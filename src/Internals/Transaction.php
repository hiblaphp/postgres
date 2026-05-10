<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Cache\ArrayCache;
use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Postgres\Traits\CancellationHelperTrait;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\TransactionException;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\Result as ResultInterface;
use Hibla\Sql\RowStream as SqlRowStream;
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
     * @return PromiseInterface<SqlRowStream>
     */
    public function stream(string $sql, array $params = [], int $bufferSize = 100): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();

        if (\count($params) === 0) {
            $promise = $this->connection->streamQuery($sql, $bufferSize);

            // Track taint state from both the outer borrow promise (pre-first-row errors
            // and outer cancellations) and the inner command promise (mid-iteration errors
            // and $stream->cancel() calls inside a foreach loop).
            //
            // Without the command-promise hook in bindStreamErrorState(), cancelling the
            // stream during iteration would send pg_cancel_backend and abort the server-side
            // transaction, but $this->failed would stay false — allowing a subsequent
            // commit() to silently send COMMIT to an already-aborted transaction.
            $tracked = $this->trackErrorState($promise)->then(
                function (SqlRowStream $stream): SqlRowStream {
                    $this->bindStreamErrorState($stream);

                    return $stream;
                }
            );

            return $this->withCancellation($tracked);
        }

        $innerPromise = null;

        $promise = $this->getCachedStatement($sql)
            ->then(function (array $result) use ($params, $bufferSize, &$innerPromise) {
                /** @var PreparedStatement $stmt */
                [$stmt, $isCached] = $result;

                $innerPromise = $stmt->executeStream($params, $bufferSize)
                    ->then(function (SqlRowStream $stream) use ($stmt, $isCached): SqlRowStream {
                        if ($stream instanceof RowStream) {
                            // Hook into mid-iteration errors/cancellations before registering
                            // the statement-close callback so both share the same underlying
                            // command promise without ordering dependencies.
                            $this->bindStreamErrorState($stream);

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
     *
     * The $onStreamError closure passed to TransactionPreparedStatement lets it
     * taint this transaction when a stream returned by the statement is cancelled
     * or errors out mid-iteration — a lifecycle event that occurs entirely outside
     * the promise chain this Transaction can observe directly.
     */
    public function prepare(string $sql): PromiseInterface
    {
        $this->ensureActiveAndNotFailed();

        $innerPromise = $this->connection->prepare($sql);

        $onStreamError = function (): void {
            $this->failed = true;
        };

        $promise = $innerPromise->then(
            function (PreparedStatementInterface $stmt) use ($onStreamError) {
                return new TransactionPreparedStatement($stmt, $this->connection, $onStreamError);
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
     * NOTE: $this->active is now set to false inside the query callbacks rather
     * than before the query is dispatched. This is critical for recoverability:
     * if the server rejects COMMIT (e.g. the connection is in INERROR state due
     * to a prior cancellation), $this->active must remain true so the caller can
     * still invoke rollback() to cleanly close out the transaction. Setting it
     * prematurely before the query meant a failing commit would permanently prevent
     * rollback from running.
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

        $promise = $this->connection->query('COMMIT')
            ->then(
                function (): void {
                    $this->active = false;
                    $this->executeCallbacks($this->onCommitCallbacks);
                    $this->onRollbackCallbacks = [];
                },
                function (\Throwable $e): never {
                    $this->active = false;
                    $this->failed = true;

                    throw new TransactionException(
                        'Failed to commit transaction: ' . $e->getMessage(),
                        (int) $e->getCode(),
                        $e
                    );
                }
            )
            ->finally($this->releaseConnection(...))
        ;

        return $this->shield($promise);
    }

    /**
     * {@inheritdoc}
     *
     * NOTE: withCancellation() is intentionally NOT applied to rollback().
     * Dispatching pg_cancel_backend against a ROLLBACK would leave the transaction
     * in an undefined state on the server. This operation must be allowed
     * to complete or fail on its own terms.
     *
     * NOTE: rollback() is idempotent — calling it on an already-rolled-back or
     * already-committed transaction silently returns a resolved promise rather than
     * throwing. This allows callers to safely place rollback() in finally blocks
     * without needing to guard with isActive() first.
     *
     * NOTE: When the underlying connection has been closed (e.g. via the opt-out
     * cancellation path where enableServerSideCancellation is false), rollback()
     * cannot send ROLLBACK to the server. It still releases the (now-closed)
     * connection back to the pool so the pool's activeConnections counter is
     * decremented and any queued waiters can be served via a fresh connection.
     * Without this release the pool would permanently believe it is at capacity.
     *
     * NOTE: When a query was recently cancelled via pg_cancel_backend
     * (wasQueryCancelled=true), releaseConnection() is called synchronously BEFORE
     * awaiting the ROLLBACK promise rather than via the traditional finally() chain.
     *
     * Rationale: the pool's drainAndRelease() removes the connection from
     * activeConnectionsMap synchronously when called, dropping active_connections to
     * zero immediately. This matters inside async() coroutines where the outer promise
     * may be cancelled: if await() in a cancelled fiber re-throws CancelledException
     * immediately (aborting cleanup), callers checking active_connections right after
     * the outer promise rejects would see a stale count. By decrementing synchronously
     * inside rollback() itself, the count is correct the instant rollback() returns —
     * regardless of whether its returned promise is awaited or discarded.
     *
     * Safety: drainAndRelease() places the connection in drainingConnections (not the
     * idle pool), so it cannot be borrowed by another caller mid-ROLLBACK. The ROLLBACK
     * command is queued on the connection before releaseConnection() is called, ensuring
     * it executes first in command-queue order, followed by drainAndRelease()'s ping.
     *
     * For non-cancelled connections (wasQueryCancelled=false), we must wait for ROLLBACK
     * to complete before releasing to avoid returning a dirty connection to the pool via
     * releaseClean(). The traditional finally() path handles this case.
     *
     * @return PromiseInterface<void>
     */
    public function rollback(): PromiseInterface
    {
        // Idempotency guard: already committed, rolled back, or otherwise closed.
        // Return a resolved promise rather than throwing so callers can safely
        // invoke rollback() in finally blocks or call it a second time defensively.
        if (! $this->active) {
            return Promise::resolved();
        }

        // Opt-out cancellation path: when enableServerSideCancellation is false and
        // a query promise is cancelled, Connection::handleCommandCancellation() calls
        // close() on the connection. The wire cannot idle while a query is running,
        // so closing is the only safe option. The connection is now dead, but it is
        // still registered as active in the pool. We must call releaseConnection()
        // here to decrement the pool counter and unblock any queued waiters.
        if ($this->connection->isClosed()) {
            $this->active = false;
            $this->failed = false;
            $this->releaseConnection();

            return Promise::resolved();
        }

        // Interrupt any running query so the wire is free to receive the ROLLBACK immediately.
        // This bridges the gap when the Fiber is killed but the underlying query promise is orphaned.
        $this->connection->cancelCurrentCommand();

        $this->active = false;
        $this->failed = false;

        $promise = $this->connection->query('ROLLBACK')
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
        ;

        // pg_cancel_backend path: release synchronously so pool stats are correct
        // immediately, before any await() on the returned promise.
        // drainAndRelease() keeps the connection in drainingConnections, so ROLLBACK
        // (already queued) runs safely before the subsequent drain ping.
        if ($this->connection->wasQueryCancelled()) {
            $this->releaseConnection();

            return $promise;
        }

        // Normal path: release only after ROLLBACK completes to prevent a dirty
        // connection from being parked in the idle pool via releaseClean().
        $promise = $promise->finally($this->releaseConnection(...));

        return $this->shield($promise);
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

        // withCancellation() was previously absent here. Without it, calling
        // cancel() on the returned promise had no effect — the underlying query
        // would run to completion and the transaction would not be tainted.
        return $this->withCancellation($this->trackErrorState($promise));
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

        return $this->withCancellation($this->trackErrorState($promise));
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
     * Force-cancels any running query on the connection.
     * Call this before rollback() if the transaction fiber was killed.
     */
    public function forceCancelCurrentQuery(): void
    {
        if (! $this->connection->isClosed()) {
            $this->connection->cancelCurrentCommand();
        }
    }

    /**
     * Wraps a promise so that any rejection or cancellation marks the transaction
     * as failed, reflecting the fact that PostgreSQL has put the connection into
     * INERROR state and requires an explicit ROLLBACK before any further work.
     *
     * Cancellation is handled separately via onCancel() because cancelled promises
     * short-circuit the chain before .catch() handlers fire — they do not propagate
     * through rejection handlers the same way that ordinary rejections do. Without
     * the onCancel hook, cancelling a query promise would successfully send
     * pg_cancel_backend and abort the server-side transaction, but $this->failed
     * would remain false. A subsequent commit() would then return the early-reject
     * path only by coincidence (or not at all), and the next query call would not
     * throw TransactionException as expected.
     *
     * @template T
     *
     * @param PromiseInterface<T> $promise
     *
     * @return PromiseInterface<T>
     */
    private function trackErrorState(PromiseInterface $promise): PromiseInterface
    {
        // onCancel fires synchronously when cancel() is called, before any chain
        // unwinding, ensuring $this->failed is set even though CancelledException
        // never reaches the .catch() handler below.
        $promise->onCancel(function (): void {
            $this->failed = true;
        });

        return $promise->catch(function (\Throwable $e) {
            $this->failed = true;

            throw $e;
        });
    }

    /**
     * Hooks into a RowStream to taint the transaction if the stream is cancelled
     * or errors out during mid-iteration consumption.
     *
     * Two complementary hooks are registered:
     *
     *   1. $stream->onCancel() — fires unconditionally whenever $stream->cancel()
     *      is called, even when the command promise is already settled (i.e. all rows
     *      arrived in one chunk before iteration started). This is the primary guard.
     *
     *   2. commandPromise onCancel / catch — fires when the server-side command is
     *      cancelled or errors out asynchronously (e.g. a deferred constraint violation
     *      while rows are still being sent). Only registered when the command promise
     *      is still pending to avoid redundant no-op registrations on settled promises.
     *
     * @param RowStream $stream The already-resolved stream whose lifecycle to observe.
     */
    private function bindStreamErrorState(RowStream $stream): void
    {
        // Primary: fires whenever $stream->cancel() is called, regardless of whether
        // commandPromise is settled. Covers the executeStream() case where all rows
        // arrive before iteration begins.
        $stream->onCancel(function (): void {
            $this->failed = true;
        });

        // Secondary: covers async server-side errors that arrive mid-stream.
        // Skip if already settled — onCancel/catch on a resolved promise is a no-op.
        $cmd = $stream->waitForCommand();

        if (! $cmd->isSettled()) {
            $cmd->onCancel(function (): void {
                $this->failed = true;
            });

            $cmd->catch(function (): void {
                $this->failed = true;
            });
        }
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
