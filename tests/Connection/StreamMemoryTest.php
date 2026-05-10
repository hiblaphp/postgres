<?php

declare(strict_types=1);

use Hibla\Postgres\Internals\RowStream;

use function Hibla\await;

describe('Postgres Connection Streaming Memory Profile', function (): void {

    it('streams 100,000 rows without exceeding a 5MB memory growth', function (): void {
        $conn = pgConn();

        gc_collect_cycles();

        $baselineMemory = memory_get_usage();

        $sql = '
            SELECT 
                n AS id, 
                md5(random()::text) || md5(random()::text) AS random_string
            FROM generate_series(1, 100000) AS n
        ';

        $stream = await($conn->streamQuery($sql, 100));
        expect($stream)->toBeInstanceOf(RowStream::class);

        $rowCount = 0;
        $maxMemoryDuringStream = 0;

        foreach ($stream as $row) {
            $rowCount++;

            if ($rowCount % 1000 === 0) {
                $currentMemory = memory_get_usage();
                if ($currentMemory > $maxMemoryDuringStream) {
                    $maxMemoryDuringStream = $currentMemory;
                }
            }
        }

        expect($rowCount)->toBe(100000);

        $memoryGrowthBytes = $maxMemoryDuringStream - $baselineMemory;
        $memoryGrowthMb = $memoryGrowthBytes / 1024 / 1024;

        expect($memoryGrowthMb)->toBeLessThan(
            5.0,
            sprintf('Memory grew by %.2f MB, which exceeds the 5.0 MB threshold.', $memoryGrowthMb)
        );

        $conn->close();
    });

    it('streams 100,000 rows using a prepared statement without exceeding a 5MB memory growth', function (): void {
        $conn = pgConn();

        gc_collect_cycles();
        $baselineMemory = memory_get_usage();

        $stmt = await($conn->prepare('
            SELECT 
                n AS id, 
                md5(random()::text) || md5(random()::text) AS random_string
            FROM generate_series(1, $1) AS n
        '));

        $stream = await($conn->executeStatementStream($stmt, [100000], 100));

        $rowCount = 0;
        $maxMemoryDuringStream = 0;

        foreach ($stream as $row) {
            $rowCount++;

            if ($rowCount % 1000 === 0) {
                $currentMemory = memory_get_usage();
                if ($currentMemory > $maxMemoryDuringStream) {
                    $maxMemoryDuringStream = $currentMemory;
                }
            }
        }

        expect($rowCount)->toBe(100000);

        $memoryGrowthBytes = $maxMemoryDuringStream - $baselineMemory;
        $memoryGrowthMb = $memoryGrowthBytes / 1024 / 1024;

        expect($memoryGrowthMb)->toBeLessThan(
            5.0,
            sprintf('Memory grew by %.2f MB, which exceeds the 5.0 MB threshold.', $memoryGrowthMb)
        );

        await($stmt->close());
        $conn->close();
    });

});
