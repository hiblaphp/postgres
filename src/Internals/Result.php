<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PostgresResult;

/**
 * Unified result object for PostgreSQL queries.
 *
 * @internal
 */
class Result implements PostgresResult
{
    /**
     * @inheritDoc
     */
    public readonly int $rowCount;

    /**
     * @inheritDoc
     */
    public readonly int $columnCount;

    private int $position = 0;

    private ?PostgresResult $nextResult = null;

    /**
     * @param int $affectedRows
     * @param int $lastInsertId Note: Postgres usually relies on RETURNING id, so this might be 0 unless parsed.
     * @param int $connectionId Process ID (PID) from pg_get_pid()
     * @param int|null $insertedOid
     * @param array<int, string> $columns
     * @param array<int, array<string, mixed>> $rows
     */
    public function __construct(
        public readonly int $affectedRows = 0,
        public readonly int $lastInsertId = 0,
        public readonly int $connectionId = 0,
        public readonly ?int $insertedOid = null,
        public readonly array $columns = [],
        private readonly array $rows = [],
    ) {
        $this->rowCount = \count($this->rows);
        $this->columnCount = \count($this->columns);
    }

    /**
     * @internal
     *
     * Links the next result set to this one.
     */
    public function setNextResult(PostgresResult $result): void
    {
        $this->nextResult = $result;
    }

    /**
     * @inheritDoc
     */
    public function nextResult(): ?PostgresResult
    {
        return $this->nextResult;
    }

    /**
     * @inheritDoc
     */
    public function hasAffectedRows(): bool
    {
        return $this->affectedRows > 0;
    }

    /**
     * @inheritDoc
     */
    public function hasLastInsertId(): bool
    {
        return $this->lastInsertId > 0;
    }

    /**
     * @inheritDoc
     */
    public function isEmpty(): bool
    {
        return $this->rowCount === 0;
    }

    /**
     * @inheritDoc
     */
    public function fetchAssoc(): ?array
    {
        if ($this->position >= $this->rowCount) {
            return null;
        }

        return $this->rows[$this->position++];
    }

    /**
     * @inheritDoc
     */
    public function fetchAll(): array
    {
        return $this->rows;
    }

    /**
     * @inheritDoc
     */
    public function fetchColumn(string|int $column = 0): array
    {
        return array_map(fn (array $row) => $row[$column] ?? null, $this->rows);
    }

    /**
     * @inheritDoc
     */
    public function fetchOne(): ?array
    {
        return $this->rows[0] ?? null;
    }

    /**
     * @inheritDoc
     */
    public function getIterator(): \Traversable
    {
        return new \ArrayIterator($this->rows);
    }
}
