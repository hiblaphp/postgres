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

    /**
     * @param string $type The type of command
     * @param Promise<mixed> $promise The promise to resolve/reject
     * @param string $sql The SQL query string
     * @param array<int, mixed> $params Parameters for the query
     * @param mixed $context Additional context (e.g., RowStream for streaming)
     */
    public function __construct(
        public readonly string $type,
        public readonly Promise $promise,
        public readonly string $sql = '',
        public readonly array $params = [],
        public readonly mixed $context = null,
    ) {
    }
}
