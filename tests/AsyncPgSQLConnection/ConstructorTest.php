<?php

declare(strict_types=1);

use Hibla\Postgres\PgSqlClient;
use Hibla\Postgres\Exceptions\ConfigurationException;
use Tests\Helpers\TestHelper;

describe('PgSqlClient Constructor', function () {
    it('creates connection with valid configuration', function () {
        $db = new PgSqlClient(TestHelper::getTestConfig(), 5);

        expect($db)->toBeInstanceOf(PgSqlClient::class);

        $stats = $db->getStats();
        expect($stats['max_size'])->toBe(5);
    });

    it('uses default pool size of 10', function () {
        $db = new PgSqlClient(TestHelper::getTestConfig());

        $stats = $db->getStats();
        expect($stats['max_size'])->toBe(10);
    });

    it('throws ConfigurationException for invalid config', function () {
        expect(fn () => new PgSqlClient([]))
            ->toThrow(ConfigurationException::class)
        ;
    });

    it('throws ConfigurationException for missing host', function () {
        expect(fn () => new PgSqlClient([
            'username' => 'postgres',
            'database' => 'test_db',
        ]))->toThrow(ConfigurationException::class);
    });

    it('can be reset and becomes unusable', function () {
        $db = new PgSqlClient(TestHelper::getTestConfig(), 5);

        $db->reset();

        expect(fn () => $db->getStats())
            ->toThrow(Hibla\Postgres\Exceptions\NotInitializedException::class)
        ;
    });
});
