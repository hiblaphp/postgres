<?php

declare(strict_types=1);

namespace Hibla\Postgres\Enums;

/**
 * Represents the phases of a server‑side cursor used for streaming.
 *
 * @internal
 */
enum CursorPhase
{
    case NONE;
    case BEGIN;
    case DECLARE;
    case FETCH;
    case CLOSE;
    case ROLLBACK;
}
