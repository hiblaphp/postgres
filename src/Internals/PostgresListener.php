<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PostgresListener as PostgresListenerInterface;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;

/**
 * A dedicated, stateful connection wrapper for PostgreSQL asynchronous notifications.
 *
 * This class bypasses the connection pool to maintain a persistent connection
 * to the server, allowing it to safely receive LISTEN/NOTIFY events without
 * interfering with standard queries or being destroyed by DISCARD ALL.
 */
final class PostgresListener implements PostgresListenerInterface
{
    private bool $isClosed = false;

    /**
     * @var array<string, list<callable(string, string, int): void>>
     */
    private array $callbacks = [];

    /**
     * @internal Created via PostgresClient::createListener(). Do not instantiate directly.
     */
    public function __construct(private readonly Connection $connection)
    {
        $this->connection->setNotificationCallback($this->routeNotification(...));
    }

    /**
     * Subscribes to a PostgreSQL channel and registers a callback.
     *
     * @param string $channel The name of the channel to listen to.
     * @param callable(string $channel, string $payload, int $pid): void $callback
     *
     * @return PromiseInterface<void> Resolves when the LISTEN command has been accepted by the server.
     */
    public function listen(string $channel, callable $callback): PromiseInterface
    {
        if ($this->isClosed || $this->connection->isClosed()) {
            return Promise::rejected(new ConnectionException('Cannot listen: Listener connection is closed.'));
        }

        // If this is the first callback for this channel, it must tell the server.
        $isFirst = ! isset($this->callbacks[$channel]) || $this->callbacks[$channel] === [];

        $this->callbacks[$channel][] = $callback;
        $this->connection->setIsListening(true);

        if ($isFirst) {
            $escapedChannel = $this->escapeIdentifier($channel);

            return $this->connection->query("LISTEN {$escapedChannel}")->then(function (): void {
            });
        }

        return Promise::resolved();
    }

    /**
     * Unsubscribes from a PostgreSQL channel and removes all associated callbacks.
     *
     * @param string $channel The name of the channel to stop listening to.
     *
     * @return PromiseInterface<void> Resolves when the UNLISTEN command has been accepted.
     */
    public function unlisten(string $channel): PromiseInterface
    {
        if ($this->isClosed || $this->connection->isClosed()) {
            return Promise::resolved();
        }

        unset($this->callbacks[$channel]);

        if ($this->callbacks === []) {
            $this->connection->setIsListening(false);
        }

        $escapedChannel = $this->escapeIdentifier($channel);

        return $this->connection->query("UNLISTEN {$escapedChannel}")->then(function (): void {
        });
    }

    /**
     * Closes the listener connection, dropping all subscriptions immediately.
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::resolved();
        }

        $this->isClosed = true;
        $this->callbacks = [];
        $this->connection->setIsListening(false);

        if ($this->connection->isClosed()) {
            return Promise::resolved();
        }

        return $this->connection->query('UNLISTEN *')
            ->then(function (): void {
            })
            ->finally(function (): void {
                $this->connection->close();
            })
        ;
    }

    /**
     * Automatically closes the unpooled connection when the listener is garbage collected.
     */
    public function __destruct()
    {
        if (! $this->isClosed && ! $this->connection->isClosed()) {
            $this->connection->close();
        }
    }

    /**
     * Internal router triggered by the Connection when a notification arrives.
     */
    private function routeNotification(string $channel, string $payload, int $pid): void
    {
        if (! isset($this->callbacks[$channel])) {
            return;
        }

        foreach ($this->callbacks[$channel] as $callback) {
            $callback($channel, $payload, $pid);
        }
    }

    /**
     * Safely escapes a channel identifier for PostgreSQL.
     */
    private function escapeIdentifier(string $identifier): string
    {
        if ($identifier === '') {
            throw new \InvalidArgumentException('Channel identifier cannot be empty');
        }

        if (\strlen($identifier) > 63) {
            throw new \InvalidArgumentException('Channel identifier too long (max 63 characters)');
        }

        if (strpos($identifier, "\0") !== false) {
            throw new \InvalidArgumentException('Channel identifier contains invalid byte values');
        }

        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
