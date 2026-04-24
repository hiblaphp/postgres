<?php

declare(strict_types=1);

namespace Hibla\Postgres\Enums;

enum ConnectionState: string
{
    case DISCONNECTED = 'disconnected';
    case CONNECTING = 'connecting';
    case READY = 'ready';
    case QUERYING = 'querying';
    case CLOSED = 'closed';
}