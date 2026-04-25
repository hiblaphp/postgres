<?php

declare(strict_types=1);

namespace Hibla\Postgres\Enums;

/**
 * Represents the phase of a server-side cursor lifecycle.
 *
 *  None → Begin? → Declare → Fetch* → Close
 *                         ↘ (on error, if ownsTransaction) → Rollback
 */
enum CursorPhase
{
    case None;
    case Begin;
    case Declare;
    case Fetch;
    case Close;
    case Rollback;
}
