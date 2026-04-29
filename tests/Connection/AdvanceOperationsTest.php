<?php

declare(strict_types=1);

use function Hibla\await;

describe('Transactions', function () {
    it('successfully commits a transaction', function () {
        $conn = pgConn();

        try {
            await($conn->query('BEGIN'));
            await($conn->query('CREATE TEMP TABLE tx_test (id INT)'));
            await($conn->query('INSERT INTO tx_test VALUES (1)'));
            await($conn->query('COMMIT'));

            $result = await($conn->query('SELECT count(*) AS total FROM tx_test'));
            expect((int) $result->fetchOne()['total'])->toBe(1);
        } finally {
            $conn->close();
        }
    });

    it('successfully rolls back a transaction', function () {
        $conn = pgConn();

        try {
            await($conn->query('CREATE TEMP TABLE tx_test_rb (id INT)'));

            await($conn->query('BEGIN'));
            await($conn->query('INSERT INTO tx_test_rb VALUES (1)'));
            await($conn->query('ROLLBACK'));

            $result = await($conn->query('SELECT count(*) AS total FROM tx_test_rb'));
            expect((int) $result->fetchOne()['total'])->toBe(0);
        } finally {
            $conn->close();
        }
    });
});
