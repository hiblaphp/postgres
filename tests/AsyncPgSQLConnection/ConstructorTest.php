<?php

use Hibla\Postgres\AsyncPgSQLConnection;
use Hibla\Postgres\Exceptions\ConfigurationException;
use Tests\Helpers\TestHelper;

describe('AsyncPgSQLConnection Constructor', function () {
    it('creates connection with valid configuration', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);
        
        expect($db)->toBeInstanceOf(AsyncPgSQLConnection::class);
        
        $stats = $db->getStats();
        expect($stats['max_size'])->toBe(5);
    });

    it('uses default pool size of 10', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig());
        
        $stats = $db->getStats();
        expect($stats['max_size'])->toBe(10);
    });

    it('throws ConfigurationException for invalid config', function () {
        expect(fn() => new AsyncPgSQLConnection([]))
            ->toThrow(ConfigurationException::class);
    });

    it('throws ConfigurationException for missing host', function () {
        expect(fn() => new AsyncPgSQLConnection([
            'username' => 'postgres',
            'database' => 'test_db',
        ]))->toThrow(ConfigurationException::class);
    });

    it('can be reset and becomes unusable', function () {
        $db = new AsyncPgSQLConnection(TestHelper::getTestConfig(), 5);
        
        $db->reset();
        
        expect(fn() => $db->getStats())
            ->toThrow(\Hibla\Postgres\Exceptions\NotInitializedException::class);
    });
});