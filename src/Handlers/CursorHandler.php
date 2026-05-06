<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\Postgres\Enums\CursorPhase;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Postgres\Traits\HandlerHelperTrait;
use Hibla\Sql\Exceptions\QueryException;
use PgSql\Result as PostgresResult;
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
    use HandlerHelperTrait;

    public function __construct(
        private readonly ConnectionContext $ctx,
        private readonly ConnectionBridge $bridge,
    ) {
    }

    /**
     * Kicks off the cursor lifecycle for a new streaming command.
     * Sends BEGIN first when we are not already inside a transaction,
     * otherwise goes straight to DECLARE.
     *
     * @param array<int|string, mixed> $params
     */
    public function init(string $sql, array $params): void
    {
        $cursor = $this->ctx->cursor;
        $cursor->sql = $sql;
        $cursor->params = $params;
        $cursor->ownsTransaction = ($this->bridge->getTransactionStatus() === PGSQL_TRANSACTION_IDLE);

        $conn = $this->getTypedConnection();

        if ($cursor->ownsTransaction) {
            $cursor->phase = CursorPhase::Begin;
            $sent = (bool) @pg_send_query($conn, 'BEGIN');
        } else {
            $cursor->phase = CursorPhase::Declare;
            $sent = $this->sendDeclare();
        }

        if (! $sent) {
            $cursor->phase = CursorPhase::None;
            $this->bridge->finishCommand(new QueryException(
                'Failed to init cursor stream: ' . pg_last_error($conn)
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
        $conn = $this->getTypedConnection();

        if (@pg_connection_busy($conn)) {
            return;
        }

        $res = @pg_get_result($conn);

        if ($res === false) {
            return;
        }

        $status = pg_result_status($res);
        $cursor = $this->ctx->cursor;

        if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
            $rawMsg = pg_result_error($res);
            $msg = $rawMsg !== false ? $rawMsg : '';
            @pg_free_result($res);
            $error = new QueryException($msg !== '' ? $msg : 'Unknown query error');

            if (
                $cursor->ownsTransaction
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
        $conn = $this->getTypedConnection();
        $context = $this->ctx->currentStreamContext();

        $sent = (bool) @pg_send_query(
            $conn,
            "FETCH {$context->bufferSize} FROM {$this->ctx->cursor->name}"
        );

        if (! $sent) {
            $this->ctx->cursor->phase = CursorPhase::None;
            $this->bridge->finishCommand(new QueryException(
                'Failed to send FETCH: ' . pg_last_error($conn)
            ));

            return;
        }

        if ($this->ctx->queryWatcherId === null) {
            $this->bridge->addQueryReadWatcher();
        }
    }

    private function stepBegin(PostgresResult $res): void
    {
        @pg_free_result($res);
        $this->ctx->cursor->phase = CursorPhase::Declare;

        if (! $this->sendDeclare()) {
            $this->sendRollback(new QueryException(
                'Failed to send DECLARE: ' . pg_last_error($this->getTypedConnection())
            ));
        }
    }

    private function stepDeclare(PostgresResult $res): void
    {
        @pg_free_result($res);
        $this->ctx->cursor->phase = CursorPhase::Fetch;
        $this->sendFetch();
    }

    private function stepFetch(PostgresResult $res): void
    {
        $context = $this->ctx->currentStreamContext();
        $rowCount = pg_num_rows($res);

        if ($rowCount > 0) {
            while ($row = pg_fetch_assoc($res)) {
                $context->push($this->normalizeRow($row));
            }
        }

        @pg_free_result($res);

        if ($rowCount === 0) {
            $this->sendClose();

            return;
        }

        if ($context->isFull()) {
            $this->bridge->pauseStream();

            return;
        }

        $this->sendFetch();
    }

    private function stepClose(PostgresResult $res): void
    {
        $conn = $this->getTypedConnection();
        $context = $this->ctx->currentStreamContext();

        @pg_free_result($res);

        while (! @pg_connection_busy($conn)) {
            $extra = @pg_get_result($conn);
            if ($extra === false) {
                break;
            }
            @pg_free_result($extra);
        }

        $this->ctx->cursor->phase = CursorPhase::None;
        $context->complete();
        $this->bridge->finishCommand(null, null);
    }

    private function stepRollback(PostgresResult $res): void
    {
        @pg_free_result($res);
        $error = $this->ctx->cursor->error;
        $this->ctx->cursor->phase = CursorPhase::None;
        $this->ctx->cursor->error = null;
        $this->bridge->finishCommand($error);
    }

    private function sendDeclare(): bool
    {
        $conn = $this->getTypedConnection();
        $cursor = $this->ctx->cursor;
        $sql = "DECLARE {$cursor->name} NO SCROLL CURSOR FOR {$cursor->sql}";

        return $cursor->params === []
            ? (bool) @pg_send_query($conn, $sql)
            : (bool) @pg_send_query_params($conn, $sql, $cursor->params);
    }

    private function sendClose(): void
    {
        $conn = $this->getTypedConnection();
        $cursor = $this->ctx->cursor;
        $context = $this->ctx->currentStreamContext();

        $sql = $cursor->ownsTransaction
            ? "CLOSE {$cursor->name}; COMMIT"
            : "CLOSE {$cursor->name}";
        $sent = (bool) @pg_send_query($conn, $sql);

        if (! $sent) {
            $cursor->phase = CursorPhase::None;
            $context->complete();
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
        $conn = $this->getTypedConnection();
        $cursor = $this->ctx->cursor;
        $cursor->error = $error;
        $sent = (bool) @pg_send_query($conn, 'ROLLBACK');

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
