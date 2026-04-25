<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\Internals\Connection;
use Hibla\Sql\Exceptions\ConnectionException;

/**
 * Handles the asynchronous connection polling (PGSQL_CONNECT_ASYNC).
 *
 * @internal
 */
class ConnectHandler
{
    /**
     * Polls the connection until it succeeds, fails or changes polling mode.
     * Called from the event loop watcher.
     */
    public static function handlePoll(Connection $conn): void
    {
        $freshSocket = $conn->getFreshSocket();
        if ($freshSocket === false) {
            $errorMsg = $conn->getConnectionResource()
                ? (pg_last_error($conn->getConnectionResource()) ?: 'Failed to retrieve socket during polling')
                : 'Connection lost';
            $conn->setState(ConnectionState::CLOSED);
            $conn->clearWatchers();
            @pg_close($conn->getConnectionResource());
            $conn->setConnectionResource(null);
            $promise = $conn->getConnectPromise();
            $conn->setConnectPromise(null);
            $promise?->reject(new ConnectionException($errorMsg));

            return;
        }

        $conn->setSocket($freshSocket);
        $status = @pg_connect_poll($conn->getConnectionResource());

        if ($status === PGSQL_POLLING_FAILED) {
            $errorMsg = pg_last_error($conn->getConnectionResource()) ?: 'Connection polling failed';
            $promise = $conn->getConnectPromise();
            $conn->setConnectPromise(null);
            $conn->close();
            $promise?->reject(new ConnectionException($errorMsg));

            return;
        }

        if ($status === PGSQL_POLLING_OK) {
            $conn->clearWatchers();
            $conn->setState(ConnectionState::READY);
            $promise = $conn->getConnectPromise();
            $conn->setConnectPromise(null);
            $promise?->resolve($conn);
            $conn->processNextCommand();

            return;
        }

        $conn->clearWatchers();

        if ($status === PGSQL_POLLING_READING) {
            $conn->setPollWatcher(Loop::addReadWatcher($freshSocket, fn () => self::handlePoll($conn)), 'read');
        } elseif ($status === PGSQL_POLLING_WRITING) {
            $conn->setPollWatcher(Loop::addWriteWatcher($freshSocket, fn () => self::handlePoll($conn)), 'write');
        }
    }
}
