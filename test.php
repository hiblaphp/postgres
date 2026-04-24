<?php

declare(strict_types=1);

require_once __DIR__ . '/vendor/autoload.php';

use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\ValueObjects\PgSqlConfig;


$config = new PgSqlConfig(
    host: '127.0.0.1',
    port: 5432,
    username: 'postgres',
    password: 'root',
    database: 'olejos_backend',
);

echo "=== Hibla PostgreSQL Connection Test ===" . PHP_EOL . PHP_EOL;

echo "[1] Connecting... ";

try {
    /** @var Connection $conn */
    $conn = Connection::create($config)->wait();
    echo "OK (pid={$conn->getProcessId()})" . PHP_EOL;
} catch (\Throwable $e) {
    echo "FAILED: {$e->getMessage()}" . PHP_EOL;
    exit(1);
}

// --- Test 2: Ping ---
echo "[2] Ping... ";

try {
    $conn->ping()->wait();
    echo "OK" . PHP_EOL;
} catch (\Throwable $e) {
    echo "FAILED: {$e->getMessage()}" . PHP_EOL;
}

// --- Test 3: Simple SELECT query ---
echo "[3] Simple SELECT... ";

try {
    $result = $conn->query('SELECT 1 + 1 AS answer')->wait();
    $answer = $result->rows[0]['answer'] ?? 'N/A';
    echo "OK (1 + 1 = {$answer})" . PHP_EOL;
} catch (\Throwable $e) {
    echo "FAILED: {$e->getMessage()}" . PHP_EOL;
}

// --- Test 4: Parameterised query ---
echo "[4] Parameterised query... ";

try {
    $result = $conn->query('SELECT $1::text AS greeting', ['Hello, Hibla!'])->wait();
    $greeting = $result->rows[0]['greeting'] ?? 'N/A';
    echo "OK (greeting='{$greeting}')" . PHP_EOL;
} catch (\Throwable $e) {
    echo "FAILED: {$e->getMessage()}" . PHP_EOL;
}

// --- Test 5: Transaction status ---
echo "[5] Transaction status is idle... ";

$status = $conn->getTransactionStatus();
if ($status === PGSQL_TRANSACTION_IDLE) {
    echo "OK" . PHP_EOL;
} else {
    echo "UNEXPECTED STATUS: {$status}" . PHP_EOL;
}

// --- Test 6: Streaming query ---
echo "[6] Stream query (generate_series 1..5)... ";

try {
    $stream = $conn->streamQuery(
        'SELECT generate_series AS n FROM generate_series(1, 5)'
    )->wait();

    $collected = [];
    foreach ($stream as $row) {
        $collected[] = (int) $row['n'];
    }

    $expected = [1, 2, 3, 4, 5];
    if ($collected === $expected) {
        echo "OK (rows=" . implode(',', $collected) . ")" . PHP_EOL;
    } else {
        echo "MISMATCH (got=" . implode(',', $collected) . ")" . PHP_EOL;
    }
} catch (\Throwable $e) {
    echo "FAILED: {$e->getMessage()}" . PHP_EOL;
}

// --- Test 7: Reset (DISCARD ALL) ---
echo "[7] Reset connection... ";

try {
    $conn->reset()->wait();
    echo "OK" . PHP_EOL;
} catch (\Throwable $e) {
    echo "FAILED: {$e->getMessage()}" . PHP_EOL;
}

// --- Done ---
echo PHP_EOL . "All tests done. Closing connection." . PHP_EOL;
$conn->close();