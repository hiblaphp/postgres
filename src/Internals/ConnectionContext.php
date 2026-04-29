<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Enums\ConnectionState;
use Hibla\Postgres\Interfaces\StreamContext;
use Hibla\Promise\Promise;
use SplQueue;
use Throwable;

/**
 * @internal
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

    /**
     * @var SplQueue<CommandRequest>
     */
    public SplQueue $commandQueue;

    /**
     * @var Promise<Connection>|null
     */
    public ?Promise $connectPromise = null;

    /**
     * @var array<int, \PgSql\Result|resource>
     */
    public array $accumulatedResults = [];

    public ConnectionState $state = ConnectionState::DISCONNECTED;

    public ?CommandRequest $currentCommand = null;

    public ?string $pollWatcherId = null;

    public ?string $pollWatcherType = null;

    public ?string $queryWatcherId = null;

    public ?Throwable $queryError = null;

    public bool $isStreamPaused = false;

    public readonly CursorState $cursor;

    public function __construct()
    {
        $this->commandQueue = new SplQueue();
        $this->cursor = new CursorState();
    }

    /**
     * Returns the current command's context narrowed to StreamContext.
     * Only valid during streaming commands — asserts at runtime and
     * gives PHPStan a concrete type to work with.
     */
    public function currentStreamContext(): StreamContext
    {
        assert($this->currentCommand !== null);
        assert($this->currentCommand->context instanceof StreamContext);

        return $this->currentCommand->context;
    }
}
