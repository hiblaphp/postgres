<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\Postgres\Enums\CursorPhase;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\CommandRequest;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Postgres\Internals\Result;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\QueryException;

/**
 * @internal Consumes query results from libpq's buffer.
 *
 * handle()  — Event Loop entry point; routes to cursor or drain mode.
 * drain()   — Tight loop that pulls results until the buffer is empty or
 *             the consumer signals backpressure. Public so StreamHandler
 *             can call it on resume in chunked mode.
 */
final class QueryResultHandler
{
    public function __construct(
        private readonly ConnectionContext $ctx,
        private readonly ConnectionBridge $bridge,
        private readonly CursorHandler $cursorHandler,
    ) {
    }

    /**
     * Called by the Event Loop when the socket has data ready to read.
     */
    public function handle(): void
    {
        if (! @pg_consume_input($this->ctx->connection)) {
            $this->bridge->finishCommand(new ConnectionException(
                'Connection lost during query: ' . pg_last_error($this->ctx->connection)
            ));

            return;
        }

        // Route to the appropriate handler based on whether we are running in
        // cursor mode (server-side batches) or chunked/standard mode.
        if ($this->ctx->cursor->phase !== CursorPhase::None) {
            $this->cursorHandler->step();
        } else {
            $this->drain();
        }
    }

    /**
     * Drains accumulated results from libpq's internal buffer.
     * Re-entrant: safe to call from StreamHandler on stream resume.
     */
    public function drain(): void
    {
        while (! @pg_connection_busy($this->ctx->connection)) {

            // BACKPRESSURE: stop draining if the consumer's buffer is full.
            // pauseStream() removes the read watcher; resumeStream() re-enters here.
            if ($this->ctx->currentCommand?->type === CommandRequest::TYPE_STREAM) {
                if ($this->ctx->currentCommand->context->isFull()) {
                    $this->bridge->pauseStream();

                    return;
                }
            }

            $res = @pg_get_result($this->ctx->connection);

            if ($res === false) {
                // Buffer fully drained.
                if ($this->ctx->queryError !== null) {
                    $this->bridge->finishCommand($this->ctx->queryError);

                    return;
                }

                if ($this->ctx->currentCommand?->type === CommandRequest::TYPE_STREAM) {
                    $this->ctx->currentCommand->context->complete();
                    $this->bridge->finishCommand(null, null);
                } else {
                    $this->processAccumulatedResults();
                }

                return;
            }

            $status = pg_result_status($res);

            if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
                $errorMsg = pg_result_error($res);
                $this->ctx->queryError = new QueryException($errorMsg !== '' ? $errorMsg : 'Unknown query error');
                @pg_free_result($res);

                continue; // Keep draining so libpq's buffer is fully flushed
            }

            if ($this->ctx->currentCommand?->type === CommandRequest::TYPE_STREAM) {
                $isChunk = defined('PGSQL_TUPLES_CHUNK') && $status === PGSQL_TUPLES_CHUNK;
                $isSingle = defined('PGSQL_SINGLE_TUPLE') && $status === PGSQL_SINGLE_TUPLE;
                $isOk = $status === PGSQL_TUPLES_OK;

                if ($isChunk || $isSingle || $isOk) {
                    while ($row = pg_fetch_assoc($res)) {
                        $this->ctx->currentCommand->context->push($row);
                    }
                }
                @pg_free_result($res);
            } else {
                $this->ctx->accumulatedResults[] = $res;
            }
        }

        // Server still sending data. Re-arm the watcher if it was removed
        // (e.g., by a resume → re-pause cycle) so we get called again when
        // the next chunk arrives.
        if (! $this->ctx->isStreamPaused
            && $this->ctx->queryWatcherId === null
            && $this->ctx->currentCommand !== null
        ) {
            $this->bridge->addQueryReadWatcher();
        }
    }

    private function processAccumulatedResults(): void
    {
        $res = end($this->ctx->accumulatedResults);

        if ($res === false) {
            $this->bridge->finishCommand(null, new Result(
                connectionId: $this->bridge->getProcessId()
            ));

            return;
        }

        $rows = pg_fetch_all($res) ?: [];
        $affected = pg_affected_rows($res);
        $oid = pg_last_oid($res) !== false ? (int) pg_last_oid($res) : null;
        $fields = [];
        $numFields = pg_num_fields($res);

        for ($i = 0; $i < $numFields; $i++) {
            $fields[] = pg_field_name($res, $i);
        }

        $this->bridge->finishCommand(null, new Result(
            affectedRows: $affected,
            connectionId: $this->bridge->getProcessId(),
            insertedOid:  $oid,
            columns:      $fields,
            rows:         $rows,
        ));
    }
}
