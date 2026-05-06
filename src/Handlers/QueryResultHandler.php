<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\Postgres\Enums\CursorPhase;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\CommandRequest;
use Hibla\Postgres\Internals\ConnectionContext;
use Hibla\Postgres\Internals\Result;
use Hibla\Postgres\Traits\HandlerHelperTrait;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\QueryException;
use PgSql\Result as PostgresResult;

/**
 * @internal Consumes query results from libpq's buffer.
 */
final class QueryResultHandler
{
    use HandlerHelperTrait;

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
        $conn = $this->getTypedConnection();

        if (! @pg_consume_input($conn)) {
            $this->bridge->finishCommand(new ConnectionException(
                'Connection lost during query: ' . pg_last_error($conn)
            ));

            return;
        }

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
        $conn = $this->getTypedConnection();

        $isStream = $this->ctx->currentCommand?->type === CommandRequest::TYPE_STREAM
            || $this->ctx->currentCommand?->type === CommandRequest::TYPE_EXECUTE_STREAM;

        while (! @pg_connection_busy($conn)) {

            // Backpressure: pause until the consumer drains below half-capacity.
            if ($isStream) {
                $context = $this->ctx->currentStreamContext();
                if ($context->isFull()) {
                    $this->bridge->pauseStream();

                    return;
                }
            }

            $res = @pg_get_result($conn);

            if ($res === false) {
                // Buffer fully drained.
                if ($this->ctx->queryError !== null) {
                    $this->bridge->finishCommand($this->ctx->queryError);

                    return;
                }

                if ($isStream) {
                    $this->ctx->currentStreamContext()->complete();
                    $this->bridge->finishCommand(null, null);
                } else {
                    $this->processAccumulatedResults();
                }

                return;
            }

            $status = pg_result_status($res);

            if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_BAD_RESPONSE) {
                $rawMsg = pg_result_error($res);
                $this->ctx->queryError = new QueryException($rawMsg !== false && $rawMsg !== '' ? $rawMsg : 'Unknown query error');
                @pg_free_result($res);

                continue; // Keep draining so libpq's buffer is fully flushed
            }

            if ($isStream) {
                $isChunk = defined('PGSQL_TUPLES_CHUNK') && $status === PGSQL_TUPLES_CHUNK;
                $isSingle = defined('PGSQL_SINGLE_TUPLE') && $status === PGSQL_SINGLE_TUPLE;
                $isOk = $status === PGSQL_TUPLES_OK;

                if ($isChunk || $isSingle || $isOk) {
                    $context = $this->ctx->currentStreamContext();
                    while ($row = pg_fetch_assoc($res)) {
                        $context->push($this->normalizeRow($row));
                    }
                }
                @pg_free_result($res);
            } else {
                $this->ctx->accumulatedResults[] = $res;
            }
        }

        // Server still sending, re-arm watcher if a resume/pause cycle removed it.
        if (
            ! $this->ctx->isStreamPaused
            && $this->ctx->queryWatcherId === null
            && $this->ctx->currentCommand !== null
        ) {
            $this->bridge->addQueryReadWatcher();
        }
    }

    private function processAccumulatedResults(): void
    {
        $cmd = $this->ctx->currentCommand;

        if ($cmd?->type === CommandRequest::TYPE_PREPARE) {
            assert($cmd->context instanceof \Closure);
            $stmt = ($cmd->context)();
            $this->bridge->finishCommand(null, $stmt);

            return;
        }

        // If the query returned zero result sets (e.g., an empty string)
        if ($this->ctx->accumulatedResults === []) {
            $this->bridge->finishCommand(null, new Result(
                connectionId: $this->bridge->getProcessId()
            ));

            return;
        }

        /** @var Result|null $head */
        $head = null;
        /** @var Result|null $tail */
        $tail = null;

        foreach ($this->ctx->accumulatedResults as $rawRes) {
            $res = $this->getTypedResult($rawRes);

            // pg_fetch_all returns false when there are zero rows; normalise to
            // an empty array so the Result constructor always receives an array.
            $fetched = pg_fetch_all($res);
            /** @var array<int, array<string, mixed>> $rows */ // @phpstan-ignore-next-line
            $rows = $fetched !== false ? $fetched : [];

            $affected = pg_affected_rows($res);
            $lastOid = pg_last_oid($res);
            $oid = $lastOid !== false ? (int) $lastOid : null;
            $fields = [];
            $numFields = pg_num_fields($res);

            for ($i = 0; $i < $numFields; $i++) {
                $fields[] = pg_field_name($res, $i);
            }

            $result = new Result(
                affectedRows: $affected,
                connectionId: $this->bridge->getProcessId(),
                insertedOid: $oid,
                columns: $fields,
                rows: $rows,
            );

            // Build the linked list
            if ($head === null) {
                $head = $result;
                $tail = $result;
            } else {
                // $tail cannot be null here because $head and $tail are always
                // assigned together in the if-branch above and they are set as a
                // pair or not at all. It use assert() rather than throwing an
                // exception because this is a structural invariant of this loop,
                // not a runtime error condition. A thrown exception would imply
                // the null is something callers should handle; assert() signals
                // it is a programmer bug that should never occur, and is compiled
                // away entirely in production (zend.assertions=-1).
                assert($tail !== null);
                $tail->setNextResult($result);
                $tail = $result;
            }
        }

        $this->bridge->finishCommand(null, $head);
    }

    /**
     * Narrows a PgSql\Result|resource to PgSql\Result.
     *
     * @param PostgresResult|resource $res
     */
    private function getTypedResult(mixed $res): PostgresResult
    {
        assert($res instanceof PostgresResult);

        return $res;
    }
}
