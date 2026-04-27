<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Promise\Promise;
use SplQueue;
use Throwable;

/**
 * @internal Shared mutable state passed to every handler.
 *
 * All properties are intentionally public so that handler classes (which live
 * in the same internal package) can read and write them directly without
 * indirection. This avoids a god-class while keeping a single source of truth.
 */
final class ConnectionContext
{
    /**
     * @var \PgSql\Connection|resource|null
     */
    public mixed $connection = null;

    /**
     * @var resource|null
     */
    public mixed $socket = null;

    public ConnectionState $state = ConnectionState::DISCONNECTED;

    /**
     * @var SplQueue<CommandRequest>
     */
    public SplQueue $commandQueue;

    public ?CommandRequest $currentCommand = null;

    public ?Promise $connectPromise = null;

    public ?string $pollWatcherId = null;

    public ?string $pollWatcherType = null;

    public ?string $queryWatcherId = null;

    public array $accumulatedResults = [];

    public ?Throwable $queryError = null;

    public bool $isStreamPaused = false;

    public readonly CursorState $cursor;

    public function __construct()
    {
        $this->commandQueue = new SplQueue();
        $this->cursor = new CursorState();
    }
}
