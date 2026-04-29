<?php

declare(strict_types=1);

namespace Hibla\Postgres\Handlers;

use Hibla\Postgres\Enums\CursorPhase;
use Hibla\Postgres\Interfaces\ConnectionBridge;
use Hibla\Postgres\Internals\ConnectionContext;

/**
 * @internal Controls stream backpressure.
 *
 * pause()  — Suspends delivery by removing the Event Loop read watcher.
 * resume() — Restarts delivery:
 *              • cursor mode  → sends next FETCH batch to the server.
 *              • chunked mode → drains whatever libpq already has buffered,
 *                               then re-arms the watcher for new server data.
 */
final class StreamHandler
{
    public function __construct(
        private readonly ConnectionContext $ctx,
        private readonly ConnectionBridge $bridge,
        private readonly CursorHandler $cursorHandler,
        private readonly QueryResultHandler $queryResultHandler,
    ) {
    }

    public function pause(): void
    {
        $this->ctx->isStreamPaused = true;
        $this->bridge->removeQueryReadWatcher();
    }

    public function resume(): void
    {
        if (! $this->ctx->isStreamPaused) {
            return;
        }

        $this->ctx->isStreamPaused = false;

        if ($this->ctx->currentCommand === null) {
            return;
        }

        if ($this->ctx->cursor->phase === CursorPhase::Fetch) {
            $this->cursorHandler->sendFetch();

            return;
        }

        // drain() may call pause() internally, flipping isStreamPaused back to true.
        $this->queryResultHandler->drain();

        // @phpstan-ignore booleanNot.alwaysTrue
        if (! $this->ctx->isStreamPaused && $this->ctx->queryWatcherId === null) {
            $this->bridge->addQueryReadWatcher();
        }
    }
}
