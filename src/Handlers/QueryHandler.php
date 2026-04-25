<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Internals\CommandRequest;
use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\Internals\Result;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\QueryException;
use Throwable;

/**
 * Handles query dispatch, result draining (non‑stream) and command finalisation.
 *
 * @internal
 */
class QueryHandler
{
    public function __construct(
        private readonly StreamHandler $streamHandler
    ) {
    }

    /**
     * Sends the current command to the server and sets up the read watcher.
     */
    public function startCommand(Connection $conn): void
    {
        $cmd = $conn->getCurrentCommand();
        if ($cmd === null) {
            return;
        }

        try {
            if ($cmd->type === CommandRequest::TYPE_PING) {
                if (pg_connection_status($conn->getConnectionResource()) === PGSQL_CONNECTION_OK) {
                    $conn->finishCommand(null, true);
                } else {
                    $conn->finishCommand(new ConnectionException('Ping failed. Connection is unhealthy.'));
                }

                return;
            }

            [$sql, $params] = \Hibla\Postgres\Internals\QueryParser::parse($cmd->sql, $cmd->params);
            $params = $conn->normalizeParams($params);

            if ($cmd->type === CommandRequest::TYPE_STREAM && function_exists('pg_set_chunked_rows_size')) {
                $cmd->context->setResumeCallback($conn->resumeStream(...));
                @pg_set_chunked_rows_size($conn->getConnectionResource(), 1);
            }

            if ($cmd->type === CommandRequest::TYPE_STREAM && ! function_exists('pg_set_chunked_rows_size')) {
                $cmd->context->setResumeCallback($conn->resumeStream(...));
                $this->streamHandler->initCursorStream($conn, $sql, $params);

                return;
            }

            $sent = $params === []
                ? @pg_send_query($conn->getConnectionResource(), $sql)
                : @pg_send_query_params($conn->getConnectionResource(), $sql, $params);

            if (! $sent) {
                throw new QueryException('Failed to send query: ' . pg_last_error($conn->getConnectionResource()));
            }

            $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $this->handleResult($conn)));
        } catch (Throwable $e) {
            if (isset($sent) && $sent && $conn->getConnectionResource() !== null) {
                @pg_cancel_query($conn->getConnectionResource());
            }
            $conn->finishCommand($e);
        }
    }

    /**
     * Called by the read watcher when data is available on the socket.
     */
    public function handleResult(Connection $conn): void
    {
        if (! @pg_consume_input($conn->getConnectionResource())) {
            $conn->finishCommand(new ConnectionException(
                'Connection lost during query: ' . pg_last_error($conn->getConnectionResource())
            ));

            return;
        }

        if ($this->streamHandler->isCursorActive()) {
            $this->streamHandler->stepCursorPhase($conn);

            return;
        }

        $this->drainResults($conn);
    }

    /**
     * Drain all available results for a non‑cursor command (plain or chunked‑stream).
     */
    private function drainResults(Connection $conn): void
    {
        while (! @pg_connection_busy($conn->getConnectionResource())) {
            $cmd = $conn->getCurrentCommand();

            // Back‑pressure: stream consumer buffer full
            if ($cmd?->type === CommandRequest::TYPE_STREAM && $this->streamHandler->isPaused()) {
                return;
            }

            $res = @pg_get_result($conn->getConnectionResource());
            if ($res === false) {
                // No more results
                if ($conn->getQueryError() !== null) {
                    $conn->finishCommand($conn->getQueryError());

                    return;
                }
                if ($cmd?->type === CommandRequest::TYPE_STREAM) {
                    $cmd->context->complete();
                    $conn->finishCommand(null, null);
                } else {
                    $this->processAccumulatedResults($conn);
                }

                return;
            }

            $status = pg_result_status($res);
            if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
                $errorMsg = pg_result_error($res);
                $conn->setQueryError(new QueryException($errorMsg !== '' ? $errorMsg : 'Unknown query error'));
                @pg_free_result($res);

                continue;   // drain remaining results
            }

            if ($cmd?->type === CommandRequest::TYPE_STREAM) {
                // Stream rows to the consumer, handling back‑pressure
                $this->streamHandler->pushRows($conn, $res);
            } else {
                $conn->addAccumulatedResult($res);
            }
        }

        // Still more data coming – re‑arm watcher (unless paused)
        if (! $this->streamHandler->isPaused() && $conn->getQueryWatcherId() === null && $conn->getCurrentCommand() !== null) {
            $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $this->handleResult($conn)));
        }
    }

    private function processAccumulatedResults(Connection $conn): void
    {
        $accumulated = $conn->getAccumulatedResults();
        $res = end($accumulated);
        if ($res === false) {
            $conn->finishCommand(null, new Result(connectionId: $conn->getProcessId()));

            return;
        }

        $rows = pg_fetch_all($res) ?: [];
        $affected = pg_affected_rows($res);
        $insertedOid = pg_last_oid($res) !== false ? (int) pg_last_oid($res) : null;

        $fields = [];
        $numFields = pg_num_fields($res);
        for ($i = 0; $i < $numFields; $i++) {
            $fields[] = pg_field_name($res, $i);
        }

        $conn->finishCommand(null, new Result(
            affectedRows: $affected,
            connectionId: $conn->getProcessId(),
            insertedOid: $insertedOid,
            columns: $fields,
            rows: $rows,
        ));
    }
}
