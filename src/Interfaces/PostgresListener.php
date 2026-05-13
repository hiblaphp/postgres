<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

use Hibla\Promise\Interfaces\PromiseInterface;

/**
 * A dedicated, stateful connection wrapper for PostgreSQL asynchronous notifications.
 */
interface PostgresListener
{
    /**
     * Subscribes to a PostgreSQL channel and registers a callback.
     *
     * @param string $channel The name of the channel to listen to.
     * @param callable(string $channel, string $payload, int $pid): void $callback
     *
     * @return PromiseInterface<void>
     */
    public function listen(string $channel, callable $callback): PromiseInterface;

    /**
     * Unsubscribes from a PostgreSQL channel and removes all associated callbacks.
     *
     * @param string $channel The name of the channel to stop listening to.
     *
     * @return PromiseInterface<void>
     */
    public function unlisten(string $channel): PromiseInterface;

    /**
     * Closes the listener connection, dropping all subscriptions immediately.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface;
}
