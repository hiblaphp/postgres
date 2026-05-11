<?php

declare(strict_types=1);

use function Hibla\await;

describe('Postgres Type Casting consistency', function (): void {

    it('casts to native PHP types when using buffered prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = '
            SELECT 
                $1::int AS my_int, 
                $2::bool AS my_bool, 
                $3::float AS my_float, 
                $4::json AS my_json
        ';

        $stmt = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [1, true, 3.14, '{"key": "value"}']));

        $row = $result->fetchOne();

        expect($row['my_int'])->toBe(1)
            ->and($row['my_bool'])->toBeTrue()
            ->and($row['my_float'])->toBe(3.14)
            ->and($row['my_json'])->toBe('{"key": "value"}')
        ;

        await($stmt->close());
        $conn->close();
    });

    it('casts to native PHP types when using streamed prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = '
            SELECT 
                $1::int AS my_int, 
                $2::bool AS my_bool, 
                $3::float AS my_float, 
                $4::json AS my_json
        ';

        $stmt = await($conn->prepare($sql));
        $stream = await($conn->executeStatementStream($stmt, [2, false, 9.99, '{"stream": true}'], 100));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(1);

        expect($rows[0]['my_int'])->toBe(2)
            ->and($rows[0]['my_bool'])->toBeFalse()
            ->and($rows[0]['my_float'])->toBe(9.99)
            ->and($rows[0]['my_json'])->toBe('{"stream": true}')
        ;

        await($stmt->close());
        $conn->close();
    });

    it('returns raw strings when using buffered plain queries', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = "
            SELECT 
                3::int AS my_int, 
                false::bool AS my_bool, 
                4.5::float AS my_float, 
                '{\"hello\": \"world\"}'::json AS my_json
        ";

        $result = await($conn->query($sql));
        $row = $result->fetchOne();

        expect($row['my_int'])->toBe('3')
            ->and($row['my_bool'])->toBe('f')
            ->and($row['my_float'])->toBe('4.5')
            ->and($row['my_json'])->toBe('{"hello": "world"}')
        ;

        $conn->close();
    });

    it('returns raw strings when using streamed plain queries with multiple rows', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = "
            SELECT 
                n::int AS my_int, 
                (n % 2 = 0)::bool AS my_bool, 
                (n * 1.5)::float AS my_float, 
                '{\"hello\": \"world\"}'::json AS my_json
            FROM generate_series(3, 4) AS n
        ";

        $stream = await($conn->streamQuery($sql, 100));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(2);

        expect($rows[0]['my_int'])->toBe('3')
            ->and($rows[0]['my_bool'])->toBe('f')
            ->and($rows[0]['my_float'])->toBe('4.5')
            ->and($rows[0]['my_json'])->toBe('{"hello": "world"}')
        ;

        expect($rows[1]['my_int'])->toBe('4')
            ->and($rows[1]['my_bool'])->toBe('t')
            ->and($rows[1]['my_float'])->toBe('6')
            ->and($rows[1]['my_json'])->toBe('{"hello": "world"}')
        ;

        $conn->close();
    });

    it('returns raw strings from buffered prepared statements when cast_prepared_types is false', function (): void {
        $conn = pgConn(['cast_prepared_types' => false]);

        $sql = '
            SELECT 
                $1::int AS my_int, 
                $2::bool AS my_bool, 
                $3::float AS my_float, 
                $4::json AS my_json
        ';

        $stmt = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [1, true, 3.14, '{"key": "value"}']));

        $row = $result->fetchOne();

        expect($row['my_int'])->toBe('1')
            ->and($row['my_bool'])->toBe('t')
            ->and($row['my_float'])->toBe('3.14')
            ->and($row['my_json'])->toBe('{"key": "value"}')
        ;

        await($stmt->close());
        $conn->close();
    });

    it('returns raw strings from streamed prepared statements when cast_prepared_types is false', function (): void {
        $conn = pgConn(['cast_prepared_types' => false]);

        $sql = '
            SELECT 
                $1::int AS my_int, 
                $2::bool AS my_bool, 
                $3::float AS my_float, 
                $4::json AS my_json
        ';

        $stmt = await($conn->prepare($sql));
        $stream = await($conn->executeStatementStream($stmt, [2, false, 9.99, '{"stream": true}'], 100));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(1);

        expect($rows[0]['my_int'])->toBe('2')
            ->and($rows[0]['my_bool'])->toBe('f')
            ->and($rows[0]['my_float'])->toBe('9.99')
            ->and($rows[0]['my_json'])->toBe('{"stream": true}')
        ;

        await($stmt->close());
        $conn->close();
    });

    it('respects cast_prepared_types override on PostgresClient', function (): void {
        $client = makeClient(['castPreparedTypes' => true]);

        $sql = 'SELECT $1::int AS my_int, $2::bool AS my_bool, $3::float AS my_float';
        $result = await($client->query($sql, [1, true, 3.14]));

        $row = $result->fetchOne();

        expect($row['my_int'])->toBe(1)
            ->and($row['my_bool'])->toBeTrue()
            ->and($row['my_float'])->toBe(3.14)
        ;

        $client->close();
    });
});
