<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

use Throwable;

/**
 * @internal Narrow callback interface that handlers use to call back into the
 * Connection coordinator without a hard dependency on the concrete class.
 *
 * Every method here maps to a specific action that only the Connection can
 * perform (lifecycle transitions, watcher registration, queue processing).
 */
interface ConnectionBridge
{
    /**
     * Triggered by ConnectHandler when PGSQL_POLLING_OK is received.
     */
    public function onConnectReady(): void;

    /**
     * Triggered by ConnectHandler when PGSQL_POLLING_FAILED is received.
     */
    public function onConnectFailed(string $errorMsg): void;

    /**
     * Finalises the current command: resets state, resolves/rejects the promise.
     */
    public function finishCommand(?Throwable $error = null, mixed $value = null): void;

    /**
     * Pops and dispatches the next command from the queue.
     */
    public function processNextCommand(): void;

    /**
     * Registers the result-ready read watcher on the socket (idempotent).
     */
    public function addQueryReadWatcher(): void;

    /**
     * Removes the result-ready read watcher (idempotent).
     */
    public function removeQueryReadWatcher(): void;

    /**
     * Returns the backend PID of this connection.
     */
    public function getProcessId(): int;

    /**
     * Returns the current libpq transaction status constant.
     */
    public function getTransactionStatus(): int;

    /**
     * Suspends stream delivery (backpressure).
     */
    public function pauseStream(): void;

    /**
     * Drains libpq's internal result buffer (used on stream resume).
     */
    public function drainResults(): void;
}
