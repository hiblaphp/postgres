<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Postgres\Traits\HandlerHelperTrait;
use Hibla\Sql\Exceptions\ConnectionException;

/**
 * @internal Handles asynchronous LISTEN/NOTIFY messages.
 */
final class PubSubHandler
{
    use HandlerHelperTrait;

    public function __construct(
        private readonly ConnectionContext $ctx,
        private readonly ConnectionBridge $bridge,
    ) {
    }

    /**
     * Called by the Event Loop when the connection is idle but listening.
     */
    public function handle(): void
    {
        $conn = $this->getTypedConnection();

        if (! @pg_consume_input($conn)) {
            $this->bridge->finishCommand(new ConnectionException(
                'Connection lost while listening: ' . pg_last_error($conn)
            ));

            return;
        }

        $this->drain();
    }

    /**
     * Pulls notifications from libpq's internal buffer and dispatches them.
     * Re-entrant: safe to call from QueryResultHandler.
     */
    public function drain(): void
    {
        $conn = $this->getTypedConnection();

        while ($notify = @pg_get_notify($conn, \PGSQL_ASSOC)) {
            $this->bridge->dispatchNotification(
                (string) $notify['message'],
                (string) $notify['payload'],
                (int) $notify['pid']
            );
        }
    }
}
