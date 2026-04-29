<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Postgres\Traits\HandlerHelperTrait;
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
    use HandlerHelperTrait;

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
            if ($this->ctx->connection !== null) {
                $conn = $this->getTypedConnection();
                $lastError = @pg_last_error($conn);

                $errorMsg = $lastError !== '' ? $lastError : 'Failed to retrieve socket during polling';
            } else {
                $errorMsg = 'Connection lost';
            }

            $this->ctx->state = ConnectionState::CLOSED;
            $this->clearPollWatchers();

            if ($this->ctx->connection !== null) {
                $conn = $this->getTypedConnection();
                @pg_close($conn);
                $this->ctx->connection = null;
            }

            $promise = $this->ctx->connectPromise;
            $this->ctx->connectPromise = null;
            $promise?->reject(new ConnectionException($errorMsg));

            return;
        }

        $this->ctx->socket = $freshSocket;
        $status = @pg_connect_poll($this->getTypedConnection());

        match (true) {
            $status === PGSQL_POLLING_FAILED => $this->onFailed(),
            $status === PGSQL_POLLING_OK => $this->onReady(),
            default => $this->rearm($status, $freshSocket),
        };
    }

    private function onFailed(): void
    {
        $conn = $this->getTypedConnection();

        $lastError = pg_last_error($conn);
        $errorMsg = $lastError !== '' ? $lastError : 'Connection polling failed';

        $this->bridge->onConnectFailed($errorMsg);
    }

    private function onReady(): void
    {
        $this->clearPollWatchers();
        $this->ctx->state = ConnectionState::READY;
        $this->bridge->onConnectReady();
    }

    /**
     * @param resource $socket
     */
    private function rearm(int $status, $socket): void
    {
        $this->clearPollWatchers();

        if ($status === PGSQL_POLLING_READING) {
            $this->ctx->pollWatcherId = Loop::addReadWatcher($socket, $this->handle(...));
            $this->ctx->pollWatcherType = 'read';
        } elseif ($status === PGSQL_POLLING_WRITING) {
            $this->ctx->pollWatcherId = Loop::addWriteWatcher($socket, $this->handle(...));
            $this->ctx->pollWatcherType = 'write';
        }
    }

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

        $conn = $this->getTypedConnection();
        $socket = @pg_socket($conn);

        return $socket !== false ? $socket : false;
    }
}
