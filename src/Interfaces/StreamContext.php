<?php

declare(strict_types=1);

namespace Hibla\Postgres\Interfaces;

interface StreamContext
{
    public int $bufferSize { get; }

    /**
     * @param array<string, mixed> $row
     */
    public function push(array $row): void;

    public function isFull(): bool;

    public function complete(): void;
}
