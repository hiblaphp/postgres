<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\Postgres\Enums\CursorPhase;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Sql\Exceptions\QueryException;
use Throwable;

/**
 * @internal Drives the server-side cursor protocol state machine.
 *
 * Lifecycle per streaming query (when pg_set_chunked_rows_size is unavailable):
 *
 *   (BEGIN →) DECLARE → FETCH* → CLOSE (+ COMMIT)
 *                    ↘ (on error, ownsTransaction) → ROLLBACK
 *
 * step() is called by QueryResultHandler every time the socket has data; it
 * reads exactly one libpq result and advances the phase accordingly.
 */
final class CursorHandler
{
    public function __construct(
        private readonly ConnectionContext $ctx,
        private readonly ConnectionBridge  $bridge,
    ) {
    }

    /**
     * Kicks off the cursor lifecycle for a new streaming command.
     * Sends BEGIN first when we are not already inside a transaction,
     * otherwise goes straight to DECLARE.
     */
    public function init(string $sql, array $params): void
    {
        $cursor = $this->ctx->cursor;
        $cursor->sql = $sql;
        $cursor->params = $params;
        $cursor->ownsTransaction = ($this->bridge->getTransactionStatus() === PGSQL_TRANSACTION_IDLE);

        if ($cursor->ownsTransaction) {
            $cursor->phase = CursorPhase::Begin;
            $sent = (bool) @pg_send_query($this->ctx->connection, 'BEGIN');
        } else {
            $cursor->phase = CursorPhase::Declare;
            $sent = $this->sendDeclare();
        }

        if (! $sent) {
            $cursor->phase = CursorPhase::None;
            $this->bridge->finishCommand(new QueryException(
                'Failed to init cursor stream: ' . pg_last_error($this->ctx->connection)
            ));

            return;
        }

        $this->bridge->addQueryReadWatcher();
    }

    /**
     * Advances the state machine by one step.
     * Called from QueryResultHandler::handle() when the cursor phase is active.
     */
    public function step(): void
    {
        if (@pg_connection_busy($this->ctx->connection)) {
            return; // More data still arriving; watcher will fire again
        }

        $res = @pg_get_result($this->ctx->connection);

        if ($res === false) {
            return; // No complete result in buffer yet
        }

        $status = pg_result_status($res);
        $cursor = $this->ctx->cursor;

        // Any error in a non-terminal phase: surface and clean up the transaction.
        if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
            $msg = pg_result_error($res);
            @pg_free_result($res);
            $error = new QueryException($msg !== '' ? $msg : 'Unknown query error');

            if ($cursor->ownsTransaction
                && $cursor->phase !== CursorPhase::Rollback
                && $cursor->phase !== CursorPhase::Close
            ) {
                $this->sendRollback($error);
            } else {
                $cursor->phase = CursorPhase::None;
                $this->bridge->finishCommand($error);
            }

            return;
        }

        match ($cursor->phase) {
            CursorPhase::Begin => $this->stepBegin($res),
            CursorPhase::Declare => $this->stepDeclare($res),
            CursorPhase::Fetch => $this->stepFetch($res),
            CursorPhase::Close => $this->stepClose($res),
            CursorPhase::Rollback => $this->stepRollback($res),
            CursorPhase::None => @pg_free_result($res),
        };
    }

    /**
     * Sends the next FETCH batch. Public so StreamHandler can call it on resume.
     */
    public function sendFetch(): void
    {
        $bufferSize = $this->ctx->currentCommand->context->bufferSize;
        $sent = @pg_send_query(
            $this->ctx->connection,
            "FETCH {$bufferSize} FROM {$this->ctx->cursor->name}"
        );

        if (! $sent) {
            $this->ctx->cursor->phase = CursorPhase::None;
            $this->bridge->finishCommand(new QueryException(
                'Failed to send FETCH: ' . pg_last_error($this->ctx->connection)
            ));

            return;
        }

        // Re-arm the read watcher if pauseStream() removed it.
        if ($this->ctx->queryWatcherId === null) {
            $this->bridge->addQueryReadWatcher();
        }
    }

    // ── Phase step handlers ───────────────────────────────────────────────────

    private function stepBegin(mixed $res): void
    {
        @pg_free_result($res);
        $this->ctx->cursor->phase = CursorPhase::Declare;

        if (! $this->sendDeclare()) {
            $this->sendRollback(new QueryException(
                'Failed to send DECLARE: ' . pg_last_error($this->ctx->connection)
            ));
        }
    }

    private function stepDeclare(mixed $res): void
    {
        @pg_free_result($res);
        $this->ctx->cursor->phase = CursorPhase::Fetch;
        $this->sendFetch();
    }

    private function stepFetch(mixed $res): void
    {
        $rowCount = pg_num_rows($res);

        if ($rowCount > 0) {
            while ($row = pg_fetch_assoc($res)) {
                $this->ctx->currentCommand->context->push($row);
            }
        }
        @pg_free_result($res);

        if ($rowCount === 0) {
            // Cursor exhausted — send CLOSE (+ COMMIT when we own the transaction).
            $this->sendClose();

            return;
        }

        // BACKPRESSURE: buffer full — pause until the consumer drains below half.
        // StreamHandler::resume() will call sendFetch() for the next batch.
        if ($this->ctx->currentCommand->context->isFull()) {
            $this->bridge->pauseStream();

            return;
        }

        $this->sendFetch();
    }

    private function stepClose(mixed $res): void
    {
        @pg_free_result($res);

        // When "CLOSE name; COMMIT" was sent as one round-trip, drain the COMMIT
        // acknowledgement from the buffer before we declare the command done.
        while (! @pg_connection_busy($this->ctx->connection)) {
            $extra = @pg_get_result($this->ctx->connection);
            if ($extra === false) {
                break;
            }
            @pg_free_result($extra);
        }

        $this->ctx->cursor->phase = CursorPhase::None;
        $this->ctx->currentCommand->context->complete();
        $this->bridge->finishCommand(null, null);
    }

    private function stepRollback(mixed $res): void
    {
        @pg_free_result($res);
        $error = $this->ctx->cursor->error;
        $this->ctx->cursor->phase = CursorPhase::None;
        $this->ctx->cursor->error = null;
        $this->bridge->finishCommand($error);
    }

    // ── Dispatch helpers ─────────────────────────────────────────────────────

    private function sendDeclare(): bool
    {
        $cursor = $this->ctx->cursor;
        $sql = "DECLARE {$cursor->name} NO SCROLL CURSOR FOR {$cursor->sql}";

        return $cursor->params === []
            ? (bool) @pg_send_query($this->ctx->connection, $sql)
            : (bool) @pg_send_query_params($this->ctx->connection, $sql, $cursor->params);
    }

    private function sendClose(): void
    {
        $cursor = $this->ctx->cursor;

        // Bundle CLOSE + COMMIT in one round-trip when we own the transaction.
        $sql = $cursor->ownsTransaction
            ? "CLOSE {$cursor->name}; COMMIT"
            : "CLOSE {$cursor->name}";
        $sent = @pg_send_query($this->ctx->connection, $sql);

        if (! $sent) {
            // Best effort: mark the stream complete and move on.
            $cursor->phase = CursorPhase::None;
            $this->ctx->currentCommand->context->complete();
            $this->bridge->finishCommand(null, null);

            return;
        }

        $cursor->phase = CursorPhase::Close;

        if ($this->ctx->queryWatcherId === null) {
            $this->bridge->addQueryReadWatcher();
        }
    }

    private function sendRollback(Throwable $error): void
    {
        $cursor = $this->ctx->cursor;
        $cursor->error = $error;
        $sent = @pg_send_query($this->ctx->connection, 'ROLLBACK');

        if (! $sent) {
            $cursor->phase = CursorPhase::None;
            $this->bridge->finishCommand($error);

            return;
        }

        $cursor->phase = CursorPhase::Rollback;

        if ($this->ctx->queryWatcherId === null) {
            $this->bridge->addQueryReadWatcher();
        }
    }
}
