<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Enums\CursorPhase;
use Throwable;

/**
 * @internal Mutable state for a single server-side cursor lifecycle.
 *
 * Isolated here so Connection and CursorHandler share one canonical copy
 * without having to thread individual fields through method signatures.
 */
final class CursorState
{
    public CursorPhase $phase = CursorPhase::None;

    public string $name = '_hibla_cursor';

    public string $sql = '';

    public bool $ownsTransaction = false;

    public ?Throwable $error = null;

    /**
     * @var array<int|string, mixed>
     */
    public array $params = [];

    /**
     * Resets all fields back to their defaults after a cursor command completes.
     */
    public function reset(): void
    {
        $this->phase = CursorPhase::None;
        $this->sql = '';
        $this->params = [];
        $this->ownsTransaction = false;
        $this->error = null;
    }
}
