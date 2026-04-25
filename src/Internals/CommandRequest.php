<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Promise\Promise;

/**
 * Represents a queued command to be executed on the PostgreSQL connection.
 *
 * @internal
 */
final class CommandRequest
{
    public const string TYPE_QUERY = 'query';
    public const string TYPE_STREAM = 'stream';
    public const string TYPE_PING = 'ping';
    public const string TYPE_RESET = 'reset';
    public const string TYPE_PREPARE = 'prepare';
    public const string TYPE_EXECUTE = 'execute';
    public const string TYPE_EXECUTE_STREAM = 'execute_stream';

    /**
     * @param string $type One of the TYPE_* constants.
     * @param Promise<mixed> $promise Resolved/rejected when the command completes.
     * @param string $sql SQL string (query/prepare) or statement name (execute).
     * @param array<int, mixed> $params Bound parameters (execute / execute_stream only).
     * @param mixed $context RowStream for stream commands; factory closure for prepare.
     */
    public function __construct(
        public readonly string  $type,
        public readonly Promise $promise,
        public readonly string  $sql = '',
        public readonly array   $params = [],
        public readonly mixed   $context = null,
    ) {
    }
}
