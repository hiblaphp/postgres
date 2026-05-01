<?php

declare(strict_types=1);

namespace Hibla\Postgres\Manager;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Exceptions\PoolException;
use Hibla\Postgres\Interfaces\ConnectionSetup as ConnectionSetupInterface;
use Hibla\Postgres\Internals\Connection as PgConnection;
use Hibla\Postgres\Internals\ConnectionSetup;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Promise\Exceptions\TimeoutException;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use InvalidArgumentException;
use SplQueue;
use Throwable;

/**
 * @internal This is a low-level, internal class. DO NOT USE IT DIRECTLY.
 *
 * Manages a pool of asynchronous PostgreSQL connections. This class is the
 * core component responsible for creating, reusing, and managing the lifecycle
 * of individual `Connection` objects to prevent resource exhaustion.
 *
 * All pooling logic is handled automatically by the `PostgresClient`. You should
 * never need to interact with the `PoolManager` directly.
 *
 * ## Shutdown Modes
 *
 * Two shutdown strategies are available:
 *
 * - close()      — Force shutdown. Immediately closes all connections in all
 *                  states (idle, active, draining) and rejects all waiters.
 *                  Safe to call from destructors.
 *
 * - closeAsync() — Graceful shutdown. Stops accepting new work immediately,
 *                  rejects all pending waiters, closes idle connections, then
 *                  waits for all active and draining connections to finish
 *                  naturally before tearing down. Accepts an optional timeout;
 *                  if the timeout expires, falls back to close() automatically.
 *
 * The two modes are safe to combine: calling close() while closeAsync() is
 * pending will force-resolve the shutdown promise before tearing everything
 * down, so the caller awaiting closeAsync() is never left hanging.
 *
 * ## Cancellation and Connection Reuse
 *
 * When a query is cancelled and server-side cancellation is enabled, the pool
 * dispatches pg_cancel_backend on a side-channel. The server responds with an
 * ErrorResponse on the main wire, which QueryResultHandler drains automatically
 * via finishCommand() — resetting the connection to READY with no scrub query
 * needed (unlike MySQL's DO SLEEP(0)).
 *
 * During this drain phase the connection is tracked in $drainingConnections to
 * guarantee it is never lost even if close() is called mid-drain.
 *
 * ## Cancellation Opt-Out
 *
 * When server-side cancellation is disabled and a promise is cancelled on an
 * active query, the Connection class closes itself entirely (the Postgres wire
 * protocol cannot idle while a query is running). The pool sees isClosed() = true
 * on release and handles it through the normal closed-connection path — no
 * special draining branch is needed.
 *
 * ## Connection Reset
 *
 * If `resetConnection` is enabled, the pool issues `DISCARD ALL` (via
 * Connection::reset(), which automatically prepends ROLLBACK if inside a
 * transaction) before returning a connection to the idle pool. This clears
 * session state (variables, temporary tables, transactions) to prevent
 * leakage between requests. This phase is also tracked via $drainingConnections.
 *
 * ## onConnect Hook
 *
 * An optional callable may be provided to run once per physical connection
 * immediately after the PostgreSQL handshake completes. The hook receives a
 * ConnectionSetupInterface — a minimal query surface that never leaks the
 * internal Connection object. Both sync and async (promise-returning) hooks
 * are supported. If the hook rejects or throws, the connection is dropped
 * entirely rather than returned to the pool in an unknown session state.
 *
 * This class is not subject to any backward compatibility (BC) guarantees.
 */
class PoolManager
{
    /**
     * @var SplQueue<PgConnection> Idle connections available for reuse.
     */
    private SplQueue $pool;

    /**
     * @var SplQueue<Promise<PgConnection>> Callers waiting for a connection.
     */
    private SplQueue $waiters;

    private int $maxSize;

    private int $minSize;

    private int $activeConnections = 0;

    private PgSqlConfig $pgSqlConfig;

    private bool $configValidated = false;

    private int $idleTimeoutNanos;

    private int $maxLifetimeNanos;

    private int $maxWaiters;

    private float $acquireTimeout;

    private PoolException $exhaustedException;

    /**
     * @var array<int, int> Last-used timestamp (nanoseconds) keyed by spl_object_id.
     */
    private array $connectionLastUsed = [];

    /**
     * @var array<int, int> Creation timestamp (nanoseconds) keyed by spl_object_id.
     */
    private array $connectionCreatedAt = [];

    /**
     * Connections currently waiting for their in-flight cancel to drain (i.e.
     * for QueryResultHandler to consume the ErrorResponse and call finishCommand)
     * or resetting via DISCARD ALL. Tracked to prevent leaks if close() is called
     * during either phase.
     *
     * @var array<int, PgConnection> keyed by spl_object_id.
     */
    private array $drainingConnections = [];

    /**
     * Connections currently checked out by the client.
     * Tracked to ensure they are closed if the pool shuts down while active.
     *
     * @var array<int, PgConnection> keyed by spl_object_id.
     */
    private array $activeConnectionsMap = [];

    /**
     * Set to true during force shutdown via close(). Causes drainAndRelease()
     * and resetAndRelease() to drop connections instead of recycling them, and
     * prevents ensureMinConnections() from spawning replacements.
     */
    private bool $isClosing = false;

    /**
     * Set to true during graceful shutdown via closeAsync(). New connection
     * requests are rejected immediately, idle connections are closed right away,
     * and the pool waits for active and draining connections to finish naturally
     * before resolving $shutdownPromise.
     *
     * Unlike $isClosing, this flag does NOT interrupt drainAndRelease() or
     * resetAndRelease() — those are allowed to complete their work so the
     * connection finishes cleanly before checkShutdownComplete() picks it up.
     */
    private bool $isGracefulShutdown = false;

    /**
     * Resolved by checkShutdownComplete() once all active and draining
     * connections have settled during a graceful shutdown. Null when no
     * graceful shutdown is in progress.
     *
     * @var Promise<void>|null
     */
    private ?Promise $shutdownPromise = null;

    /**
     * @var (callable(ConnectionSetupInterface): (PromiseInterface<mixed>|void)|null)|null
     */
    private readonly mixed $onConnect;

    /**
     * @param PgSqlConfig|array<string, mixed>|string $config
     * @param int $maxSize Maximum number of connections in the pool.
     * @param int $minSize Minimum number of connections to keep open (default: 0).
     * @param int $idleTimeout Seconds before an idle connection is closed.
     * @param int $maxLifetime Seconds before a connection is rotated.
     * @param int $maxWaiters Maximum number of requests allowed to queue waiting
     *                        for a connection. 0 means unlimited.
     * @param float $acquireTimeout Maximum seconds to wait for a connection before
     *                              giving up. 0.0 means unlimited (wait forever).
     * @param bool|null $enableServerSideCancellation Whether cancelling a query
     *                                                promise dispatches pg_cancel_backend.
     *                                                If null, the value from $config is used.
     * @param (callable(ConnectionSetupInterface): (PromiseInterface<mixed>|void))|null $onConnect
     *                                                                                             Optional hook invoked once per physical connection immediately after the
     *                                                                                             PostgreSQL handshake completes.
     */
    public function __construct(
        PgSqlConfig|array|string $config,
        int $maxSize = 10,
        int $minSize = 0,
        int $idleTimeout = 300,
        int $maxLifetime = 3600,
        int $maxWaiters = 0,
        float $acquireTimeout = 0.0,
        ?bool $enableServerSideCancellation = null,
        ?callable $onConnect = null,
    ) {
        $params = match (true) {
            $config instanceof PgSqlConfig => $config,
            \is_array($config) => PgSqlConfig::fromArray($config),
            \is_string($config) => PgSqlConfig::fromUri($config),
        };

        if ($enableServerSideCancellation !== null && $params->enableServerSideCancellation !== $enableServerSideCancellation) {
            $params = $params->withQueryCancellation($enableServerSideCancellation);
        }

        $this->pgSqlConfig = $params;

        if ($maxSize <= 0) {
            throw new InvalidArgumentException('Pool max size must be greater than 0');
        }

        if ($minSize < 0) {
            throw new InvalidArgumentException('Pool min connections must be 0 or greater');
        }

        if ($minSize > $maxSize) {
            throw new InvalidArgumentException(
                \sprintf('Pool min connections (%d) cannot exceed max size (%d)', $minSize, $maxSize)
            );
        }

        if ($idleTimeout <= 0) {
            throw new InvalidArgumentException('Idle timeout must be greater than 0');
        }

        if ($maxLifetime <= 0) {
            throw new InvalidArgumentException('Max lifetime must be greater than 0');
        }

        if ($maxWaiters < 0) {
            throw new InvalidArgumentException('Max waiters must be 0 or greater');
        }

        if ($acquireTimeout < 0.0) {
            throw new InvalidArgumentException('Acquire timeout must be 0.0 or greater');
        }

        // Optimization: Pre-instantiate the exception to avoid stack trace allocation
        // during high-load rejection scenarios.
        $this->exhaustedException = new PoolException(
            "Connection pool exhausted. Max waiters limit ({$maxWaiters}) reached."
        );

        $this->configValidated = true;
        $this->maxWaiters = $maxWaiters;
        $this->acquireTimeout = $acquireTimeout;
        $this->maxSize = $maxSize;
        $this->minSize = $minSize;
        $this->idleTimeoutNanos = $idleTimeout * 1_000_000_000;
        $this->maxLifetimeNanos = $maxLifetime * 1_000_000_000;
        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
        $this->onConnect = $onConnect;

        // Warm up the pool to the minimum required connections.
        $this->ensureMinConnections();
    }

    private int $pendingWaitersCount {
        get {
            $count = 0;
            foreach ($this->waiters as $waiter) {
                if ($waiter->isPending()) {
                    $count++;
                }
            }

            return $count;
        }
    }

    /**
     * Retrieves statistics about the current state of the connection pool.
     *
     * @var array<string, bool|float|int>
     */
    public array $stats {
        get {
            return [
                'active_connections' => $this->activeConnections,
                'pooled_connections' => $this->pool->count(),
                'min_size' => $this->minSize,
                'waiting_requests' => $this->pendingWaitersCount,
                'draining_connections' => \count($this->drainingConnections),
                'max_size' => $this->maxSize,
                'max_waiters' => $this->maxWaiters,
                'acquire_timeout' => $this->acquireTimeout,
                'config_validated' => $this->configValidated,
                'tracked_connections' => \count($this->connectionCreatedAt),
                'query_cancellation_enabled' => $this->pgSqlConfig->enableServerSideCancellation,
                'reset_connection_enabled' => $this->pgSqlConfig->resetConnection,
                'on_connect_hook' => $this->onConnect !== null,
                'is_graceful_shutdown' => $this->isGracefulShutdown,
            ];
        }
    }

    /**
     * Asynchronously acquires a connection from the pool.
     *
     * Rejects immediately if the pool is shutting down (either gracefully or
     * by force), so callers always get a definitive answer without waiting.
     *
     * Uses "Check-on-Borrow" strategy:
     * 1. Idle timeout exceeded → discard.
     * 2. Max lifetime exceeded → discard.
     * 3. Not ready / closed → discard.
     *
     * If no idle connection is available and the pool is not at capacity,
     * a new connection is created. Otherwise the caller is queued as a waiter.
     *
     * Waiter promises support cancellation and timeouts:
     * - If cancelled before connection acquisition, it is skipped.
     * - If acquireTimeout is set and exceeded, the promise rejects with TimeoutException.
     *
     * @return PromiseInterface<PgConnection>
     */
    public function get(): PromiseInterface
    {
        // Reject immediately during any form of shutdown so no new work enters
        // the system. Both force-close and graceful shutdown block new borrows.
        if ($this->isClosing || $this->isGracefulShutdown) {
            return Promise::rejected(new PoolException('Pool is shutting down'));
        }

        while (! $this->pool->isEmpty()) {
            /** @var PgConnection $connection */
            $connection = $this->pool->dequeue();

            $connId = spl_object_id($connection);
            $now = (int) hrtime(true);
            $lastUsed = $this->connectionLastUsed[$connId] ?? 0;
            $createdAt = $this->connectionCreatedAt[$connId] ?? 0;

            if (($now - $lastUsed) > $this->idleTimeoutNanos) {
                $this->removeConnection($connection);

                continue;
            }

            if (($now - $createdAt) > $this->maxLifetimeNanos) {
                $this->removeConnection($connection);

                continue;
            }

            if (! $connection->isReady() || $connection->isClosed()) {
                $this->removeConnection($connection);

                continue;
            }

            unset($this->connectionLastUsed[$connId]);

            // Mark as active so it is tracked if closed mid-usage.
            $this->activeConnectionsMap[$connId] = $connection;

            return Promise::resolved($connection);
        }

        if ($this->activeConnections < $this->maxSize) {
            return $this->createNewConnection();
        }

        if ($this->maxWaiters > 0 && $this->pendingWaitersCount >= $this->maxWaiters) {
            return Promise::rejected($this->exhaustedException);
        }

        // At capacity — enqueue a waiter.
        /** @var Promise<PgConnection> $waiterPromise */
        $waiterPromise = new Promise();

        if ($this->acquireTimeout > 0.0) {
            $timeout = $this->acquireTimeout;
            $timerId = Loop::addTimer($timeout, static function () use ($waiterPromise, $timeout): void {
                if ($waiterPromise->isPending()) {
                    $waiterPromise->reject(new TimeoutException($timeout));
                }
            });

            // Cancel the timer regardless of outcome.
            // Notice: no $this captured here, allowing perfect Garbage Collection.
            $waiterPromise->finally(static function () use ($timerId): void {
                Loop::cancelTimer($timerId);
            })->catch(static function (): void {
                // Ignore — only fired to clean up the timer.
            });
        }

        $this->waiters->enqueue($waiterPromise);

        return $waiterPromise;
    }

    /**
     * Releases a connection back to the pool.
     *
     * Determines whether the connection needs to wait for a cancel drain cycle,
     * undergo a DISCARD ALL flush, or if it can be parked cleanly.
     *
     * Cancellation opt-out note: when server-side cancellation is disabled and
     * a query promise is cancelled, Connection::close() is called internally,
     * making isClosed() true. That path is handled by the first guard below —
     * no special draining branch is needed for the opt-out case.
     */
    public function release(PgConnection $connection): void
    {
        if ($connection->isClosed()) {
            $this->removeConnection($connection);
            $this->satisfyNextWaiter();

            return;
        }

        // 1. Wait for the cancel drain cycle to complete before reuse.
        //
        //    When pg_cancel_backend was dispatched, the server sends an
        //    ErrorResponse on the main wire. QueryResultHandler consumes it
        //    automatically via finishCommand(), resetting state to READY. The
        //    connection is in QUERYING state here — we use a queued ping() as a
        //    synchronization barrier that can only dequeue once READY is reached.
        //    No scrub query (like MySQL's DO SLEEP(0)) is needed for Postgres.
        if ($connection->wasQueryCancelled()) {
            $this->drainAndRelease($connection);

            return;
        }

        // If the connection is not in a READY state and was not explicitly
        // cancelled, it was released in a dirty or corrupted state and cannot
        // safely park in the idle pool.
        if (! $connection->isReady()) {
            $this->removeConnection($connection);
            $this->satisfyNextWaiter();

            return;
        }

        // 2. Perform connection state reset if enabled.
        if ($this->pgSqlConfig->resetConnection) {
            $this->resetAndRelease($connection);

            return;
        }

        $this->releaseClean($connection);
    }

    /**
     * Initiates a graceful shutdown of the pool.
     *
     * Graceful shutdown proceeds in this order:
     *   1. Gates the pool — get() rejects immediately so no new work enters.
     *   2. Closes all idle connections in the pool immediately.
     *   3. Waits for active connections and draining connections to finish.
     *   4. Resolves the returned promise once everything is empty.
     *
     * @param float $timeout Maximum seconds to wait for graceful drain before
     *                       falling back to force close(). 0.0 means no timeout.
     *
     * @return PromiseInterface<void>
     */
    public function closeAsync(float $timeout = 0.0): PromiseInterface
    {
        if ($this->isClosing) {
            return Promise::resolved();
        }

        if ($this->isGracefulShutdown) {
            return $this->shutdownPromise ?? Promise::resolved();
        }

        $this->isGracefulShutdown = true;

        while (! $this->pool->isEmpty()) {
            $connection = $this->pool->dequeue();

            if (! $connection->isClosed()) {
                $connection->close();
            }

            $connId = spl_object_id($connection);
            unset(
                $this->connectionLastUsed[$connId],
                $this->connectionCreatedAt[$connId]
            );

            $this->activeConnections--;
        }

        /** @var Promise<void> $shutdownPromise */
        $shutdownPromise = new Promise();
        $this->shutdownPromise = $shutdownPromise;

        $this->checkShutdownComplete();

        if ($timeout > 0.0 && $this->shutdownPromise !== null) {
            $pendingShutdown = $this->shutdownPromise;

            $timerId = Loop::addTimer($timeout, function (): void {
                if ($this->shutdownPromise !== null && $this->shutdownPromise->isPending()) {
                    $this->close();
                }
            });

            $pendingShutdown->finally(function () use ($timerId): void {
                Loop::cancelTimer($timerId);
            })->catch(static function (): void {
            });
        }

        return $this->shutdownPromise ?? Promise::resolved();
    }

    /**
     * Force-closes all connections in all states (idle, draining, active)
     * and rejects all pending waiters.
     *
     * If a graceful shutdown via closeAsync() is in progress, this method
     * resolves the shutdown promise before tearing everything down so the
     * caller awaiting closeAsync() is never left hanging.
     *
     * Safe to call from destructors.
     */
    public function close(): void
    {
        // If a graceful shutdown is pending, resolve it first so any caller
        // awaiting closeAsync() is not left hanging after everything is destroyed.
        if ($this->shutdownPromise !== null && $this->shutdownPromise->isPending()) {
            $this->shutdownPromise->resolve(null);
            $this->shutdownPromise = null;
        }

        $this->isGracefulShutdown = false;
        $this->isClosing = true;

        while (! $this->pool->isEmpty()) {
            $connection = $this->pool->dequeue();

            if (! $connection->isClosed()) {
                $connection->close();
            }
        }

        // Close connections mid-drain so they are not leaked.
        foreach ($this->drainingConnections as $connection) {
            if (! $connection->isClosed()) {
                $connection->close();
            }
        }

        $this->drainingConnections = [];

        // Close active connections to prevent hanging the event loop.
        foreach ($this->activeConnectionsMap as $connection) {
            if (! $connection->isClosed()) {
                $connection->close();
            }
        }

        $this->activeConnectionsMap = [];

        while (! $this->waiters->isEmpty()) {
            /** @var Promise<PgConnection> $promise */
            $promise = $this->waiters->dequeue();

            if (! $promise->isCancelled()) {
                $promise->reject(new PoolException('Pool is being closed'));
            }
        }

        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
        $this->activeConnections = 0;
        $this->connectionLastUsed = [];
        $this->connectionCreatedAt = [];
    }

    /**
     * Pings all idle connections to verify health.
     *
     * @return PromiseInterface<array<string, int>>
     */
    public function healthCheck(): PromiseInterface
    {
        /** @var Promise<array<string, int>> $promise */
        $promise = new Promise();

        $stats = [
            'total_checked' => 0,
            'healthy' => 0,
            'unhealthy' => 0,
        ];

        /** @var SplQueue<PgConnection> $tempQueue */
        $tempQueue = new SplQueue();

        /** @var array<int, PromiseInterface<bool>> $checkPromises */
        $checkPromises = [];

        while (! $this->pool->isEmpty()) {
            /** @var PgConnection $connection */
            $connection = $this->pool->dequeue();
            $stats['total_checked']++;

            $checkPromises[] = $connection->ping()
                ->then(
                    function () use ($connection, $tempQueue, &$stats): void {
                        $stats['healthy']++;
                        $connId = spl_object_id($connection);
                        $this->connectionLastUsed[$connId] = (int) hrtime(true);
                        $tempQueue->enqueue($connection);
                    },
                    function () use ($connection, &$stats): void {
                        $stats['unhealthy']++;
                        $this->removeConnection($connection);
                    }
                )
            ;
        }

        Promise::all($checkPromises)
            ->then(
                function () use ($promise, $tempQueue, &$stats): void {
                    while (! $tempQueue->isEmpty()) {
                        $conn = $tempQueue->dequeue();

                        if ($this->isClosing || $this->isGracefulShutdown) {
                            $this->removeConnection($conn);
                        } else {
                            $this->pool->enqueue($conn);
                        }
                    }

                    $promise->resolve($stats);
                },
                function (Throwable $e) use ($promise, $tempQueue): void {
                    while (! $tempQueue->isEmpty()) {
                        $conn = $tempQueue->dequeue();

                        if ($this->isClosing || $this->isGracefulShutdown) {
                            $this->removeConnection($conn);
                        } else {
                            $this->pool->enqueue($conn);
                        }
                    }

                    $promise->reject($e);
                }
            )
        ;

        return $promise;
    }

    /**
     * Checks whether the graceful shutdown completion condition has been met
     * and resolves $shutdownPromise if so.
     *
     * Called at the end of every code path that removes a connection from
     * $activeConnectionsMap or $drainingConnections so no completion event
     * is ever missed.
     *
     * Completion condition: graceful shutdown is active AND both the active
     * connections map and draining connections map are fully empty.
     */
    private function checkShutdownComplete(): void
    {
        if (! $this->isGracefulShutdown) {
            return;
        }

        // If active connections are exhausted but waiters remain, they will
        // never be served (no new connections spawn during shutdown). Reject them
        // so they don't hang indefinitely.
        if ($this->activeConnections === 0 && ! $this->waiters->isEmpty()) {
            $shuttingDownException = new PoolException('Pool is shutting down');
            while (! $this->waiters->isEmpty()) {
                $waiter = $this->waiters->dequeue();
                if ($waiter->isPending()) {
                    $waiter->reject($shuttingDownException);
                }
            }
        }

        if ($this->activeConnections > 0 || ! $this->waiters->isEmpty()) {
            return;
        }

        // All in-flight work has settled. Clear state and signal completion.
        $this->activeConnections = 0;
        $this->connectionLastUsed = [];
        $this->connectionCreatedAt = [];

        if ($this->shutdownPromise !== null && $this->shutdownPromise->isPending()) {
            $this->shutdownPromise->resolve(null);
        }

        $this->shutdownPromise = null;
    }

    /**
     * Runs the onConnect hook on a freshly created connection before it is
     * handed to a caller or parked in the pool.
     *
     * @return PromiseInterface<PgConnection>
     */
    private function runOnConnectHook(PgConnection $connection): PromiseInterface
    {
        if ($this->onConnect === null) {
            return Promise::resolved($connection);
        }

        $setup = new ConnectionSetup($connection);

        return \Hibla\async(fn () => ($this->onConnect)($setup))
            ->then(fn () => $connection)
        ;
    }

    /**
     * Waits for the in-flight cancel drain cycle to complete before releasing
     * the connection back to the pool.
     *
     * Postgres-specific drain strategy:
     *   After pg_cancel_backend fires, the server sends an ErrorResponse on the
     *   main wire. QueryResultHandler consumes it automatically via finishCommand(),
     *   resetting the connection to READY — no scrub query is needed.
     *
     *   A queued ping() acts as a synchronization barrier: it cannot dequeue
     *   from processNextCommand() until state is READY, so resolving means the
     *   drain has fully completed and the connection is clean.
     *
     * Not interrupted by graceful shutdown — the drain is allowed to complete
     * so the connection finishes cleanly. checkShutdownComplete() is called
     * when it exits drainingConnections naturally.
     */
    private function drainAndRelease(PgConnection $connection): void
    {
        $connId = spl_object_id($connection);

        unset($this->activeConnectionsMap[$connId]);

        // During force-close, drop immediately instead of waiting for the drain.
        if ($this->isClosing) {
            $this->removeConnection($connection);

            return;
        }

        $this->drainingConnections[$connId] = $connection;

        // ping() queues behind the current QUERYING command and can only
        // execute once finishCommand() fires after the ErrorResponse is consumed.
        // This guarantees the cancel drain is complete before we proceed.
        $connection->ping()
            ->then(
                function () use ($connection, $connId): void {
                    unset($this->drainingConnections[$connId]);

                    if ($this->isClosing) {
                        $this->removeConnection($connection);

                        return;
                    }

                    $connection->clearCancelledFlag();

                    // Mark active again for releaseClean or reset logic.
                    $this->activeConnectionsMap[$connId] = $connection;

                    if ($this->pgSqlConfig->resetConnection) {
                        $this->resetAndRelease($connection);
                    } else {
                        $this->releaseClean($connection);
                    }
                },
                function () use ($connection, $connId): void {
                    unset($this->drainingConnections[$connId]);

                    if ($this->isClosing) {
                        $this->removeConnection($connection);

                        return;
                    }

                    $connection->clearCancelledFlag();

                    // Ping failure means the connection is unhealthy after the drain.
                    if ($connection->isClosed() || ! $connection->isReady()) {
                        $this->removeConnection($connection);
                        $this->satisfyNextWaiter();

                        return;
                    }

                    $this->activeConnectionsMap[$connId] = $connection;

                    if ($this->pgSqlConfig->resetConnection) {
                        $this->resetAndRelease($connection);
                    } else {
                        $this->releaseClean($connection);
                    }
                }
            )
        ;
    }

    /**
     * Issues DISCARD ALL (via Connection::reset(), which prepends ROLLBACK
     * automatically if inside a transaction) to clear session state before
     * making the connection available for the next caller.
     *
     * Not interrupted by graceful shutdown — see drainAndRelease() for the
     * same reasoning.
     */
    private function resetAndRelease(PgConnection $connection): void
    {
        $connId = spl_object_id($connection);

        unset($this->activeConnectionsMap[$connId]);

        // During force-close, drop immediately instead of resetting.
        if ($this->isClosing) {
            $this->removeConnection($connection);

            return;
        }

        $this->drainingConnections[$connId] = $connection;

        $connection->reset()->then(
            function () use ($connection, $connId): void {
                unset($this->drainingConnections[$connId]);

                if ($this->isClosing) {
                    $this->removeConnection($connection);

                    return;
                }

                $this->activeConnectionsMap[$connId] = $connection;

                // Re-run the hook — DISCARD ALL wipes all session state back to
                // server defaults (search_path, time zone, application_name, etc.),
                // putting the connection in an identical state to just after the
                // initial handshake. The hook must restore it for the same reason
                // it ran at connect time.
                $this->runOnConnectHook($connection)->then(
                    fn () => $this->releaseClean($connection),
                    function () use ($connection): void {
                        // Hook failed after reset — unknown session state, drop it.
                        $this->removeConnection($connection);
                        $this->satisfyNextWaiter();
                    }
                );
            },
            function () use ($connection, $connId): void {
                unset($this->drainingConnections[$connId]);

                // If DISCARD ALL fails, the connection state is tainted. Drop it.
                $this->removeConnection($connection);
                $this->satisfyNextWaiter();
            }
        );
    }

    /**
     * Releases a clean connection: either hands it to a waiting caller,
     * parks it in the idle pool, or triggers checkShutdownComplete() if
     * a graceful shutdown is in progress and no waiters are present.
     */
    private function releaseClean(PgConnection $connection): void
    {
        $connId = spl_object_id($connection);

        // Always try to serve existing waiters first, even during shutdown.
        $waiter = $this->dequeueActiveWaiter();

        if ($this->waiters->isEmpty()) {
            $this->waiters = new SplQueue();
        }

        if ($waiter !== null) {
            $waiter->resolve($connection);

            return;
        }

        // If shutting down and no waiters remain, destroy the connection.
        if ($this->isGracefulShutdown) {
            unset($this->activeConnectionsMap[$connId]);
            $this->removeConnection($connection);

            return;
        }

        // No waiters — park in idle pool.
        $now = (int) hrtime(true);
        $createdAt = $this->connectionCreatedAt[$connId] ?? 0;

        if (($now - $createdAt) > $this->maxLifetimeNanos) {
            $this->removeConnection($connection);

            return;
        }

        $this->connectionLastUsed[$connId] = $now;

        unset($this->activeConnectionsMap[$connId]);
        $this->pool->enqueue($connection);
    }

    /**
     * Ensures that the pool maintains the minimum number of connections.
     *
     * Skipped during any form of shutdown to prevent spawning new connections
     * while the pool is being torn down.
     */
    private function ensureMinConnections(): void
    {
        if ($this->isClosing || $this->isGracefulShutdown) {
            return;
        }

        while ($this->activeConnections < $this->minSize) {
            $this->createNewConnection()->then(
                function (PgConnection $connection): void {
                    // Check if a waiter arrived while the connection was being established.
                    $waiter = $this->dequeueActiveWaiter();

                    if ($waiter !== null) {
                        $waiter->resolve($connection);
                    } else {
                        // If a shutdown started while the connection was being established,
                        // drop it immediately.
                        if ($this->isClosing || $this->isGracefulShutdown) {
                            $this->removeConnection($connection);

                            return;
                        }

                        // Otherwise park it in the idle pool.
                        $connId = spl_object_id($connection);
                        $this->connectionLastUsed[$connId] = (int) hrtime(true);
                        unset($this->activeConnectionsMap[$connId]);
                        $this->pool->enqueue($connection);
                    }
                },
                function (Throwable $e): void {
                    // Ignored — the next interaction will eventually retry.
                }
            );
        }
    }

    /**
     * Creates a new connection and resolves the returned promise on success.
     * Runs the onConnect hook before handing the connection to the caller.
     *
     * @return Promise<PgConnection>
     */
    private function createNewConnection(): Promise
    {
        $this->activeConnections++;

        /** @var Promise<PgConnection> $promise */
        $promise = new Promise();

        PgConnection::create($this->pgSqlConfig)
            ->then(
                function (PgConnection $connection) use ($promise): void {
                    // Abort immediately if the pool was force-closed mid-handshake.
                    if ($this->isClosing) {
                        $connection->close();
                        $this->activeConnections--;
                        $promise->reject(new PoolException('Pool is being closed'));
                        $this->checkShutdownComplete();

                        return;
                    }

                    $connId = spl_object_id($connection);
                    $this->connectionCreatedAt[$connId] = (int) hrtime(true);
                    $this->activeConnectionsMap[$connId] = $connection;

                    $this->runOnConnectHook($connection)->then(
                        function () use ($promise, $connection): void {
                            // Check again in case the pool closed during the async hook.
                            if ($this->isClosing) {
                                $this->removeConnection($connection);
                                $promise->reject(new PoolException('Pool is being closed'));

                                return;
                            }

                            // If the caller cancelled the borrow while connecting, release
                            // cleanly so the connection is given to the next waiter or parked.
                            if ($promise->isCancelled()) {
                                $this->releaseClean($connection);

                                return;
                            }

                            $promise->resolve($connection);
                        },
                        function (Throwable $e) use ($promise, $connection): void {
                            $this->removeConnection($connection);
                            $promise->reject($e);
                        }
                    );
                },
                function (Throwable $e) use ($promise): void {
                    $this->activeConnections--;
                    $promise->reject($e);
                    $this->checkShutdownComplete();
                }
            )
        ;

        return $promise;
    }

    /**
     * Creates a new connection specifically to satisfy the next queued waiter.
     * Runs the onConnect hook before resolving the waiter.
     */
    private function createConnectionForWaiter(): void
    {
        $waiter = $this->dequeueActiveWaiter();

        if ($waiter === null) {
            return;
        }

        $this->activeConnections++;

        PgConnection::create($this->pgSqlConfig)
            ->then(
                function (PgConnection $connection) use ($waiter): void {
                    if ($this->isClosing) {
                        $connection->close();
                        $this->activeConnections--;
                        $waiter->reject(new PoolException('Pool is being closed'));
                        $this->checkShutdownComplete();

                        return;
                    }

                    $connId = spl_object_id($connection);
                    $this->connectionCreatedAt[$connId] = (int) hrtime(true);
                    $this->activeConnectionsMap[$connId] = $connection;

                    $this->runOnConnectHook($connection)->then(
                        function () use ($connection, $waiter): void {
                            if ($this->isClosing) {
                                $this->removeConnection($connection);
                                $waiter->reject(new PoolException('Pool is being closed'));

                                return;
                            }

                            // Waiter gave up while hook was running — park cleanly.
                            if ($waiter->isCancelled()) {
                                $this->releaseClean($connection);

                                return;
                            }

                            $waiter->resolve($connection);
                        },
                        function (Throwable $e) use ($connection, $waiter): void {
                            $this->removeConnection($connection);
                            $waiter->reject($e);
                        }
                    );
                },
                function (Throwable $e) use ($waiter): void {
                    $this->activeConnections--;
                    $waiter->reject($e);
                    $this->checkShutdownComplete();
                }
            )
        ;
    }

    /**
     * Dequeues the next valid (pending) waiter promise, discarding any that
     * have been cancelled or rejected by acquire timeout.
     *
     * @return Promise<PgConnection>|null
     */
    private function dequeueActiveWaiter(): ?Promise
    {
        while (! $this->waiters->isEmpty()) {
            /** @var Promise<PgConnection> $waiter */
            $waiter = $this->waiters->dequeue();

            if ($waiter->isPending()) {
                return $waiter;
            }
        }

        return null;
    }

    /**
     * Satisfies the next waiter if pool capacity allows, called after a
     * connection is removed (e.g. health check failure, idle timeout eviction).
     *
     * Not called during shutdown — get() is gated and no new work should
     * enter, so there are no valid waiters to satisfy.
     */
    private function satisfyNextWaiter(): void
    {
        if ($this->isGracefulShutdown || $this->isClosing) {
            return;
        }

        if (! $this->waiters->isEmpty() && $this->activeConnections < $this->maxSize) {
            $this->createConnectionForWaiter();
        }
    }

    /**
     * Closes and removes a connection, cleaning up all tracking metadata.
     *
     * After removing the connection, calls checkShutdownComplete() so that
     * any in-progress graceful shutdown can detect the drained state.
     */
    private function removeConnection(PgConnection $connection): void
    {
        if (! $connection->isClosed()) {
            $connection->close();
        }

        $connId = spl_object_id($connection);
        unset(
            $this->connectionLastUsed[$connId],
            $this->connectionCreatedAt[$connId],
            $this->drainingConnections[$connId],
            $this->activeConnectionsMap[$connId]
        );

        $this->activeConnections--;

        // Only replenish during normal operation.
        if (! $this->isClosing && ! $this->isGracefulShutdown) {
            $this->ensureMinConnections();
        }

        // Always check — this is a no-op when not in graceful shutdown.
        $this->checkShutdownComplete();
    }

    public function __destruct()
    {
        $this->close();
    }
}