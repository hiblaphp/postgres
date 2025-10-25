<?php

declare(strict_types=1);

use Hibla\Postgres\Manager\PoolManager;
use Tests\Helpers\TestHelper;

describe('PoolManager Connection Validation', function () {
    it('validates connection is alive with connection status', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);

        $connection = $pool->get()->await();

        expect(pg_connection_status($connection))->toBe(PGSQL_CONNECTION_OK);

        $pool->release($connection);

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(1);

        $pool->close();
    });

    it('detects dead connection', function () {
        $pool = new PoolManager(TestHelper::getTestConfig(), 5);

        $connection = $pool->get()->await();

        expect(pg_connection_status($connection))->toBe(PGSQL_CONNECTION_OK);

        pg_close($connection);

        $initialActive = $pool->getStats()['active_connections'];
        $pool->release($connection);

        expect($pool->getStats()['active_connections'])->toBeLessThan($initialActive);

        $pool->close();
    });
});
