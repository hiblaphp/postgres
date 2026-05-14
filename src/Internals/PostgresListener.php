<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Interfaces\PostgresListener as PostgresListenerInterface;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;
use InvalidArgumentException;
use Throwable;

/**
 * @internal Created via PostgresClient::createListener(). Do not instantiate directly.
 *
 * A dedicated, stateful connection wrapper for PostgreSQL asynchronous notifications.
 *
 * This class bypasses the connection pool to maintain a persistent connection
 * to the server, allowing it to safely receive LISTEN/NOTIFY events. It includes
 * transparent auto-reconnection with exponential backoff if the network drops.
 */
final class PostgresListener implements PostgresListenerInterface
{
    private bool $isClosed = false;

    private ?Connection $connection = null;

    /**
     * @var PromiseInterface<Connection>|null
     */
    private ?PromiseInterface $connectionPromise = null;

    /**
     * @var array<string, list<callable(string, string, int): void>>
     */
    private array $callbacks = [];

    /**
     * @param PgSqlConfig $config
     * @param float $minReconnectInterval The initial wait time in seconds before the first reconnect attempt.
     * @param float $maxReconnectInterval The maximum wait time in seconds for exponential backoff.
     */
    public function __construct(
        private readonly PgSqlConfig $config,
        private readonly float $minReconnectInterval = 1.0,
        private readonly float $maxReconnectInterval = 30.0,
    ) {
        if ($this->minReconnectInterval <= 0.0) {
            throw new InvalidArgumentException('Minimum reconnect interval must be greater than 0');
        }

        if ($this->maxReconnectInterval < $this->minReconnectInterval) {
            throw new InvalidArgumentException('Maximum reconnect interval cannot be less than the minimum');
        }
    }

    /**
     * Establishes the initial connection before returning the listener to the user.
     *
     * @return PromiseInterface<void>
     */
    public function initialize(): PromiseInterface
    {
        return $this->getConnection()->then(function (): void {
        });
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void> Resolves when the LISTEN command has been accepted by the server.
     */
    public function listen(string $channel, callable $callback): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::rejected(new ConnectionException('Cannot listen: Listener is closed.'));
        }

        // If this is the first callback for this channel, we must tell the server.
        $isFirst = ! isset($this->callbacks[$channel]) || $this->callbacks[$channel] === [];

        $this->callbacks[$channel][] = $callback;

        return $this->getConnection()->then(function (Connection $conn) use ($channel, $isFirst) {
            if ($isFirst) {
                $conn->setIsListening(true);

                return $conn->query('LISTEN ' . $this->escapeIdentifier($channel))->then(function (): void {
                });
            }

            return;
        });
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void> Resolves when the UNLISTEN command has been accepted.
     */
    public function unlisten(string $channel): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::resolved();
        }

        unset($this->callbacks[$channel]);

        if ($this->connection === null) {
            return Promise::resolved();
        }

        if ($this->callbacks === []) {
            $this->connection->setIsListening(false);
        }

        return $this->connection->query('UNLISTEN ' . $this->escapeIdentifier($channel))
            ->then(function (): void {
            })
        ;
    }

    /**
     * {@inheritdoc}
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

        $conn = $this->connection;
        $this->connection = null;
        $this->connectionPromise = null;

        if ($conn === null || $conn->isClosed()) {
            return Promise::resolved();
        }

        $conn->setIsListening(false);

        /** @var PromiseInterface<void> */
        return $conn->query('UNLISTEN *')
            ->then(function (): void {
            })
            ->finally(function () use ($conn) {
                $conn->close();
            })
        ;
    }

    /**
     * Automatically closes the unpooled connection when the listener is garbage collected.
     */
    public function __destruct()
    {
        if (! $this->isClosed) {
            $this->isClosed = true;
            $this->connection?->close();
        }
    }

    /**
     * Retrieves the active connection or creates a new one if currently disconnected.
     * Automatically re-subscribes to all tracked channels upon a successful connection.
     *
     * @return PromiseInterface<Connection>
     */
    private function getConnection(): PromiseInterface
    {
        if ($this->connectionPromise !== null) {
            return $this->connectionPromise;
        }

        /** @var Promise<Connection> $promise */
        $promise = new Promise();
        $this->connectionPromise = $promise;

        Connection::create($this->config)->then(
            function (Connection $conn) use ($promise): void {
                // If listener was closed while connecting, abort immediately
                if ($this->isClosed) {
                    $conn->close();
                    $promise->reject(new ConnectionException('Listener closed during connection'));

                    return;
                }

                $this->connection = $conn;
                $conn->setNotificationCallback($this->routeNotification(...));
                $conn->onClose($this->handleDisconnect(...));

                $subs = [];

                foreach (array_keys($this->callbacks) as $channel) {
                    $subs[] = $conn->query('LISTEN ' . $this->escapeIdentifier((string) $channel));
                }

                if ($subs !== []) {
                    $conn->setIsListening(true);
                    Promise::all($subs)->then(
                        fn () => $promise->resolve($conn),
                        function (Throwable $e) use ($promise, $conn): void {
                            $conn->close();
                            $promise->reject($e);
                        }
                    );
                } else {
                    $promise->resolve($conn);
                }
            },
            function (Throwable $e) use ($promise): void {
                $this->connectionPromise = null;
                $promise->reject($e);
            }
        );

        return $promise;
    }

    /**
     * Triggered automatically by the Connection when the underlying TCP socket dies.
     */
    private function handleDisconnect(): void
    {
        if ($this->isClosed) {
            return;
        }

        $this->connection = null;
        $this->connectionPromise = null;

        // Start reconnecting immediately using the user-defined minimum interval
        $this->reconnectWithBackoff($this->minReconnectInterval);
    }

    /**
     * Exponential backoff loop that runs until the database comes back online.
     */
    private function reconnectWithBackoff(float $delay): void
    {
        if ($this->isClosed) {
            return;
        }

        Loop::addTimer($delay, function () use ($delay): void {
            if ($this->isClosed) {
                return;
            }

            $this->getConnection()->catch(function () use ($delay): void {
                // Exponential backoff, capped at user-defined maximum
                $this->reconnectWithBackoff(min($delay * 2, $this->maxReconnectInterval));
            });
        });
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
     * Safely escapes a channel identifier for PostgreSQL to prevent SQL injection.
     */
    private function escapeIdentifier(string $identifier): string
    {
        if ($identifier === '') {
            throw new InvalidArgumentException('Channel identifier cannot be empty');
        }

        if (\strlen($identifier) > 63) {
            throw new InvalidArgumentException('Channel identifier too long (max 63 characters)');
        }

        if (strpos($identifier, "\0") !== false) {
            throw new InvalidArgumentException('Channel identifier contains invalid byte values');
        }

        // Postgres uses double quotes to escape identifiers
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
