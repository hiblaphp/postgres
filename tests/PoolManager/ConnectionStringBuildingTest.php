<?php

declare(strict_types=1);

use Hibla\Postgres\Manager\PoolManager;

describe('PoolManager Connection String Building', function () {
    it('builds connection string with minimal config', function () {
        $config = [
            'host' => 'localhost',
            'username' => 'testuser',
            'database' => 'testdb',
        ];

        $pool = new PoolManager($config, 5);

        expect($pool->getStats()['config_validated'])->toBeTrue();

        $pool->close();
    });

    it('builds connection string with all parameters', function () {
        $config = [
            'host' => 'localhost',
            'username' => 'testuser',
            'database' => 'testdb',
            'password' => 'testpass',
            'port' => 5433,
            'sslmode' => 'require',
            'connect_timeout' => 30,
        ];

        $pool = new PoolManager($config, 5);

        expect($pool->getStats()['config_validated'])->toBeTrue();

        $pool->close();
    });
});
