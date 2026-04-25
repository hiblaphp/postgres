<?php

declare(strict_types=1);

require_once __DIR__ . '/vendor/autoload.php';

use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\ValueObjects\PgSqlConfig;

$config = new PgSqlConfig(
    host: '127.0.0.1',
    port: 5443,
    username: 'postgres',
    password: 'postgres',
    database: 'postgres',
);

/**
 * Utility to format bytes
 */
function formatBytes($bytes)
{
    return round($bytes / 1024 / 1024, 2) . ' MB';
}

echo '=== PostgreSQL Memory Usage Benchmark ===' . PHP_EOL;
echo 'PHP Version: ' . PHP_VERSION . PHP_EOL;
echo 'libpq Chunking Support: ' . (function_exists('pg_set_chunked_rows_size') ? 'YES' : 'NO') . PHP_EOL . PHP_EOL;

try {
    $conn = Connection::create($config)->wait();
} catch (Throwable $e) {
    echo 'Connection Failed: ' . $e->getMessage() . PHP_EOL;
    exit(1);
}

// --- TEST 1: Standard Buffered Query ---
echo '[Test 1] Standard Query (100,000 rows)...' . PHP_EOL;
gc_collect_cycles();
$startMem = memory_get_usage();

try {
    $result = $conn->query('SELECT generate_series(1, 100000) AS id, repeat(\'a\', 100) AS padding')->wait();
    $peakMem = memory_get_peak_usage();

    echo '  > Rows fetched: ' . $result->rowCount . PHP_EOL;
    echo '  > Peak Memory during buffered query: ' . formatBytes($peakMem) . PHP_EOL;
} catch (Throwable $e) {
    echo '  > Failed: ' . $e->getMessage() . PHP_EOL;
}

unset($result); // Clear memory
gc_collect_cycles();
echo '  > Memory after clearing: ' . formatBytes(memory_get_usage()) . PHP_EOL . PHP_EOL;

// --- TEST 2: Streaming Query ---
echo '[Test 2] Streaming Query (100,000 rows)...' . PHP_EOL;
gc_collect_cycles();
$baseMem = memory_get_usage();
echo '  > Base memory before stream: ' . formatBytes($baseMem) . PHP_EOL;

try {
    // We request 100,000 rows, but we iterate over them one by one
    $stream = $conn->streamQuery('SELECT generate_series(1, 100000) AS id, repeat(\'a\', 100) AS padding')->wait();

    $count = 0;
    $maxObservedMem = 0;

    foreach ($stream as $row) {
        $count++;
        $currentMem = memory_get_usage();
        if ($currentMem > $maxObservedMem) {
            $maxObservedMem = $currentMem;
        }

        // Periodically report status every 25k rows
        if ($count % 25000 === 0) {
            echo "    ... processed $count rows (Current Mem: " . formatBytes($currentMem) . ')' . PHP_EOL;
        }
    }

    echo "  > Total rows processed: $count" . PHP_EOL;
    echo '  > Max memory observed during streaming: ' . formatBytes($maxObservedMem) . PHP_EOL;
    echo '  > Memory increase during stream: ' . formatBytes($maxObservedMem - $baseMem) . PHP_EOL;

} catch (Throwable $e) {
    echo '  > Failed: ' . $e->getMessage() . PHP_EOL;
}

$conn->close();
echo PHP_EOL . 'Benchmark Complete.' . PHP_EOL;
