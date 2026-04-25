<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Sql\Exceptions\ConnectionException;

/**
 * @internal Drives the non-blocking connect-poll handshake.
 *
 * The Event Loop calls handle() repeatedly until the connection either
 * reaches PGSQL_POLLING_OK (success) or PGSQL_POLLING_FAILED (failure),
 * re-arming itself for read or write depending on what libpq requires next.
 */
final class ConnectHandler
{
    public function __construct(
        private readonly ConnectionContext $ctx,
        private readonly ConnectionBridge $bridge,
    ) {
    }

    /**
     * Called by the Event Loop whenever the socket becomes readable/writable
     * during the initial connection handshake.
     */
    public function handle(): void
    {
        $freshSocket = $this->getFreshSocket();

        if ($freshSocket === false) {
            // Socket has gone away before the handshake could complete.
            // We close everything directly here (not via Connection::close) because
            // the command queue will be empty at this point — no commands can be
            // enqueued until the connect promise resolves.
            $errorMsg = $this->ctx->connection
                ? (@pg_last_error($this->ctx->connection) ?: 'Failed to retrieve socket during polling')
                : 'Connection lost';

            $this->ctx->state = ConnectionState::CLOSED;
            $this->clearPollWatchers();

            if ($this->ctx->connection !== null) {
                @pg_close($this->ctx->connection);
                $this->ctx->connection = null;
            }

            $promise = $this->ctx->connectPromise;
            $this->ctx->connectPromise = null;
            $promise?->reject(new ConnectionException($errorMsg));

            return;
        }

        $this->ctx->socket = $freshSocket;
        $status = @pg_connect_poll($this->ctx->connection);

        match (true) {
            $status === PGSQL_POLLING_FAILED => $this->onFailed(),
            $status === PGSQL_POLLING_OK => $this->onReady(),
            default => $this->rearm($status, $freshSocket),
        };
    }

    // ── Poll outcomes ─────────────────────────────────────────────────────────

    private function onFailed(): void
    {
        // Delegate full teardown to Connection (it owns the queue + close logic).
        $errorMsg = pg_last_error($this->ctx->connection) ?: 'Connection polling failed';
        $this->bridge->onConnectFailed($errorMsg);
    }

    private function onReady(): void
    {
        $this->clearPollWatchers();
        $this->ctx->state = ConnectionState::READY;
        $this->bridge->onConnectReady();
    }

    private function rearm(int $status, mixed $socket): void
    {
        // libpq needs more I/O — swap the watcher direction accordingly.
        $this->clearPollWatchers();

        if ($status === PGSQL_POLLING_READING) {
            $this->ctx->pollWatcherId = Loop::addReadWatcher($socket, $this->handle(...));
            $this->ctx->pollWatcherType = 'read';
        } elseif ($status === PGSQL_POLLING_WRITING) {
            $this->ctx->pollWatcherId = Loop::addWriteWatcher($socket, $this->handle(...));
            $this->ctx->pollWatcherType = 'write';
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private function clearPollWatchers(): void
    {
        if ($this->ctx->pollWatcherId === null) {
            return;
        }

        if ($this->ctx->pollWatcherType === 'read') {
            Loop::removeReadWatcher($this->ctx->pollWatcherId);
        } else {
            Loop::removeWriteWatcher($this->ctx->pollWatcherId);
        }

        $this->ctx->pollWatcherId = null;
        $this->ctx->pollWatcherType = null;
    }

    /**
     * @return resource|false
     */
    private function getFreshSocket(): mixed
    {
        if ($this->ctx->connection === null) {
            return false;
        }

        $socket = @pg_socket($this->ctx->connection);

        return $socket !== false ? $socket : false;
    }
}
