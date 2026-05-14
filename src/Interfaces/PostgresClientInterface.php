<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Sql\SqlClientInterface;

/**
 * Extends the base SqlClientInterface with PostgreSQL-specific features,
 * such as asynchronous Pub/Sub (LISTEN/NOTIFY).
 */
interface PostgresClientInterface extends SqlClientInterface
{
    /**
     * Sends an asynchronous notification to a PostgreSQL channel.
     *
     * @param string $channel The name of the channel.
     * @param string $payload Optional payload to send with the notification.
     *
     * @return PromiseInterface<void>
     */
    public function notify(string $channel, string $payload = ''): PromiseInterface;

    /**
     * Creates a dedicated, unpooled PostgresListener for Pub/Sub.
     *
     * This creates a standalone TCP connection to PostgreSQL that is completely
     * isolated from the connection pool with transparent auto-reconnection.
     *
     * @param float $minReconnectInterval The initial wait time in seconds before reconnecting.
     * @param float $maxReconnectInterval The maximum wait time in seconds for exponential backoff.
     *
     * @return PromiseInterface<PostgresListener>
     */
    public function createListener(float $minReconnectInterval = 1.0, float $maxReconnectInterval = 30.0): PromiseInterface;
}
