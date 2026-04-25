<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Enums\CursorPhase;
use Hibla\Postgres\Internals\Connection;
use Hibla\Sql\Exceptions\QueryException;
use Throwable;

/**
 * Manages server‑side cursor streaming and back‑pressure for chunked streams.
 *
 * @internal
 */
class StreamHandler
{
    private CursorPhase $cursorPhase = CursorPhase::NONE;

    private string $cursorName = '_hibla_cursor';

    private string $cursorSQL = '';

    private array $cursorParams = [];

    private bool $cursorOwnsTransaction = false;

    private ?Throwable $cursorError = null;

    private bool $isStreamPaused = false;

    public function isCursorActive(): bool
    {
        return $this->cursorPhase !== CursorPhase::NONE;
    }

    public function isPaused(): bool
    {
        return $this->isStreamPaused;
    }

    public function initCursorStream(Connection $conn, string $sql, array $params): void
    {
        $this->cursorSQL = $sql;
        $this->cursorParams = $params;
        $this->cursorOwnsTransaction = (pg_transaction_status($conn->getConnectionResource()) === PGSQL_TRANSACTION_IDLE);

        if ($this->cursorOwnsTransaction) {
            $this->cursorPhase = CursorPhase::BEGIN;
            $sent = @pg_send_query($conn->getConnectionResource(), 'BEGIN');
        } else {
            $this->cursorPhase = CursorPhase::DECLARE;
            $sent = $this->dispatchCursorDeclare($conn);
        }

        if (! $sent) {
            $this->cursorPhase = CursorPhase::NONE;
            $conn->finishCommand(new QueryException('Failed to init cursor stream: ' . pg_last_error($conn->getConnectionResource())));

            return;
        }

        $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $conn->getQueryHandler()->handleResult($conn)));
    }

    private function dispatchCursorDeclare(Connection $conn): bool
    {
        $sql = "DECLARE {$this->cursorName} NO SCROLL CURSOR FOR {$this->cursorSQL}";

        return $this->cursorParams === []
            ? (bool) @pg_send_query($conn->getConnectionResource(), $sql)
            : (bool) @pg_send_query_params($conn->getConnectionResource(), $sql, $this->cursorParams);
    }

    private function dispatchCursorFetch(Connection $conn): void
    {
        $fetchSize = $conn->getCurrentCommand()?->context->bufferSize ?? 100;
        $sent = @pg_send_query($conn->getConnectionResource(), "FETCH {$fetchSize} FROM {$this->cursorName}");

        if (! $sent) {
            $this->cursorPhase = CursorPhase::NONE;
            $conn->finishCommand(new QueryException('Failed to send FETCH: ' . pg_last_error($conn->getConnectionResource())));

            return;
        }

        if ($conn->getQueryWatcherId() === null) {
            $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $conn->getQueryHandler()->handleResult($conn)));
        }
    }

    private function dispatchCursorClose(Connection $conn): void
    {
        $sql = $this->cursorOwnsTransaction
            ? "CLOSE {$this->cursorName}; COMMIT"
            : "CLOSE {$this->cursorName}";
        $sent = @pg_send_query($conn->getConnectionResource(), $sql);

        if (! $sent) {
            $this->cursorPhase = CursorPhase::NONE;
            $conn->getCurrentCommand()?->context->complete();
            $conn->finishCommand(null, null);

            return;
        }

        $this->cursorPhase = CursorPhase::CLOSE;
        if ($conn->getQueryWatcherId() === null) {
            $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $conn->getQueryHandler()->handleResult($conn)));
        }
    }

    private function dispatchCursorRollback(Connection $conn, Throwable $error): void
    {
        $this->cursorError = $error;
        $sent = @pg_send_query($conn->getConnectionResource(), 'ROLLBACK');

        if (! $sent) {
            $this->cursorPhase = CursorPhase::NONE;
            $conn->finishCommand($error);

            return;
        }

        $this->cursorPhase = CursorPhase::ROLLBACK;
        if ($conn->getQueryWatcherId() === null) {
            $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $conn->getQueryHandler()->handleResult($conn)));
        }
    }

    // ── Cursor state machine ──────────────────────────────────────────────

    public function stepCursorPhase(Connection $conn): void
    {
        if (@pg_connection_busy($conn->getConnectionResource())) {
            return;
        }

        $res = @pg_get_result($conn->getConnectionResource());
        if ($res === false) {
            return;
        }

        $status = pg_result_status($res);

        if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
            $msg = pg_result_error($res);
            @pg_free_result($res);
            $error = new QueryException($msg !== '' ? $msg : 'Unknown query error');

            if (
                $this->cursorOwnsTransaction
                && $this->cursorPhase !== CursorPhase::ROLLBACK
                && $this->cursorPhase !== CursorPhase::CLOSE
            ) {
                $this->dispatchCursorRollback($conn, $error);
            } else {
                $this->cursorPhase = CursorPhase::NONE;
                $conn->finishCommand($error);
            }

            return;
        }

        switch ($this->cursorPhase) {
            case CursorPhase::BEGIN:
                @pg_free_result($res);
                $this->cursorPhase = CursorPhase::DECLARE;
                if (! $this->dispatchCursorDeclare($conn)) {
                    $this->dispatchCursorRollback($conn, new QueryException('Failed to send DECLARE: ' . pg_last_error($conn->getConnectionResource())));
                }

                break;

            case CursorPhase::DECLARE:
                @pg_free_result($res);
                $this->cursorPhase = CursorPhase::FETCH;
                $this->dispatchCursorFetch($conn);

                break;

            case CursorPhase::FETCH:
                $rowCount = pg_num_rows($res);
                if ($rowCount > 0) {
                    while ($row = pg_fetch_assoc($res)) {
                        $conn->getCurrentCommand()?->context->push($row);
                    }
                }
                @pg_free_result($res);

                if ($rowCount === 0) {
                    $this->dispatchCursorClose($conn);

                    return;
                }

                // Back‑pressure: pause if buffer full
                if ($conn->getCurrentCommand()?->context->isFull()) {
                    $this->pause($conn);

                    return;
                }

                $this->dispatchCursorFetch($conn);

                break;

            case CursorPhase::CLOSE:
                @pg_free_result($res);
                // Drain any extra COMMIT acknowledgement
                while (! @pg_connection_busy($conn->getConnectionResource())) {
                    $extra = @pg_get_result($conn->getConnectionResource());
                    if ($extra === false) {
                        break;
                    }
                    @pg_free_result($extra);
                }
                $this->cursorPhase = CursorPhase::NONE;
                $conn->getCurrentCommand()?->context->complete();
                $conn->finishCommand(null, null);

                break;

            case CursorPhase::ROLLBACK:
                @pg_free_result($res);
                $error = $this->cursorError;
                $this->cursorPhase = CursorPhase::NONE;
                $this->cursorError = null;
                $conn->finishCommand($error);

                break;

            default:
                // no‑op
        }
    }

    public function pushRows(Connection $conn, mixed $result): void
    {
        // Only called for chunked streams (no cursor)
        $isChunk = \defined('PGSQL_TUPLES_CHUNK') && pg_result_status($result) === PGSQL_TUPLES_CHUNK;
        $isSingle = \defined('PGSQL_SINGLE_TUPLE') && pg_result_status($result) === PGSQL_SINGLE_TUPLE;
        $isOk = pg_result_status($result) === PGSQL_TUPLES_OK;

        if ($isChunk || $isSingle || $isOk) {
            while ($row = pg_fetch_assoc($result)) {
                $conn->getCurrentCommand()?->context->push($row);
                if ($conn->getCurrentCommand()?->context->isFull()) {
                    $this->pause($conn);
                    @pg_free_result($result);

                    return;
                }
            }
        }
        @pg_free_result($result);
    }

    public function pause(Connection $conn): void
    {
        $this->isStreamPaused = true;
        if ($conn->getQueryWatcherId() !== null) {
            Loop::removeReadWatcher($conn->getQueryWatcherId());
            $conn->setQueryWatcherId(null);
        }
    }

    public function resume(Connection $conn): void
    {
        if (! $this->isStreamPaused) {
            return;
        }
        $this->isStreamPaused = false;

        if ($conn->getCurrentCommand() === null) {
            return;
        }

        if ($this->cursorPhase === CursorPhase::FETCH) {
            $this->dispatchCursorFetch($conn);

            return;
        }

        // Chunked mode: continue draining
        $conn->getQueryHandler()->handleResult($conn);

        if (! $this->isStreamPaused && $conn->getQueryWatcherId() === null && $conn->getCurrentCommand() !== null) {
            $conn->setQueryWatcherId(Loop::addReadWatcher($conn->getSocket(), fn () => $conn->getQueryHandler()->handleResult($conn)));
        }
    }

    /**
     * Resets all cursor‑related state (called when a command finishes).
     */
    public function reset(): void
    {
        $this->cursorPhase = CursorPhase::NONE;
        $this->cursorSQL = '';
        $this->cursorParams = [];
        $this->cursorOwnsTransaction = false;
        $this->cursorError = null;
        // isStreamPaused is reset by finishCommand
    }

    public function resetPaused(): void
    {
        $this->isStreamPaused = false;
    }
}
