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

    it('casts primitive postgres arrays to native PHP arrays in buffered prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = '
        SELECT
            $1::int[] AS int_array,
            $2::bool[] AS bool_array,
            $3::float[] AS float_array,
            $4::text[] AS text_array
        ';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [
            '{1,2,3}',
            '{t,f,t}',
            '{1.1,2.2,3.3}',
            '{foo,bar,baz}',
        ]));

        $row = $result->fetchOne();

        expect($row['int_array'])->toBe([1, 2, 3])
            ->and($row['bool_array'])->toBe([true, false, true])
            ->and($row['float_array'])->toBe([1.1, 2.2, 3.3])
            ->and($row['text_array'])->toBe(['foo', 'bar', 'baz'])
        ;

        await($stmt->close());
        $conn->close();
    });

    it('casts postgres arrays containing NULLs to PHP arrays with null elements in prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = '
        SELECT
            $1::int[]  AS int_array_with_null,
            $2::bool[] AS bool_array_with_null,
            $3::text[] AS text_array_with_null
        ';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [
            '{1,NULL,3}',
            '{t,NULL,f}',
            '{foo,NULL,bar}',
        ]));

        $row = $result->fetchOne();

        expect($row['int_array_with_null'])->toBe([1, null, 3])
            ->and($row['bool_array_with_null'])->toBe([true, null, false])
            ->and($row['text_array_with_null'])->toBe(['foo', null, 'bar'])
        ;

        await($stmt->close());
        $conn->close();
    });

    it('casts multidimensional postgres arrays to nested PHP arrays in prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = '
        SELECT
            $1::int[][]  AS matrix_int,
            $2::bool[][] AS matrix_bool
    ';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [
            '{{1,2},{3,4}}',
            '{{t,f},{f,t}}',
        ]));

        $row = $result->fetchOne();

        expect($row['matrix_int'])->toBe([[1, 2], [3, 4]])
            ->and($row['matrix_bool'])->toBe([[true, false], [false, true]])
        ;

        await($stmt->close());
        $conn->close();
    });

    it('returns numeric and decimal arrays as raw string arrays to preserve precision', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        // numeric/decimal are intentionally not cast to float.
        // Callers should use BCMath or brick/math on the raw string values.
        $sql = '
        SELECT
            $1::numeric[] AS numeric_array,
            $2::decimal[] AS decimal_array
    ';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [
            '{1.1111111111111111,2.9999999999999999}',
            '{99999999999999999.99}',
        ]));

        $row = $result->fetchOne();

        expect($row['numeric_array'])->toBe(['1.1111111111111111', '2.9999999999999999'])
            ->and($row['decimal_array'])->toBe(['99999999999999999.99'])
        ;

        await($stmt->close());
        $conn->close();
    });

    it('returns json and jsonb arrays as raw string arrays in prepared statements, leaving decoding to the caller', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        // JSON decoding is a domain concern, the parser extracts the raw
        // string and the caller decides how to decode and handle errors.
        $sql = '
        SELECT
            $1::json[]  AS json_array,
            $2::jsonb[] AS jsonb_array
    ';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, [
            '{"{\\"a\\":1}","{\\"b\\":2}"}',
            '{"{\\"x\\":1}","{\\"y\\":2}"}',
        ]));

        $row = $result->fetchOne();

        expect($row['json_array'])->toBe(['{"a":1}', '{"b":2}'])
            ->and($row['jsonb_array'])->toBe(['{"x": 1}', '{"y": 2}'])
        ;

        await($stmt->close());
        $conn->close();
    });

    it('returns raw array literal strings for plain queries regardless of cast_prepared_types', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        // Plain queries are never cast — arrays come back as raw Postgres literals.
        $sql = "
        SELECT
            ARRAY[1, 2, 3] AS int_array,
            ARRAY[true, false, true] AS bool_array
    ";

        $result = await($conn->query($sql));
        $row    = $result->fetchOne();

        expect($row['int_array'])->toBe('{1,2,3}')
            ->and($row['bool_array'])->toBe('{t,f,t}')
        ;

        $conn->close();
    });

    it('returns raw array literal strings from prepared statements when cast_prepared_types is false', function (): void {
        $conn = pgConn(['cast_prepared_types' => false]);

        $sql = '
        SELECT
            $1::int[]  AS int_array,
            $2::bool[] AS bool_array
    ';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, ['{1,2,3}', '{t,f,t}']));

        $row = $result->fetchOne();

        expect($row['int_array'])->toBe('{1,2,3}')
            ->and($row['bool_array'])->toBe('{t,f,t}')
        ;

        await($stmt->close());
        $conn->close();
    });

    it('casts postgres arrays correctly in streamed prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = '
        SELECT
            $1::int[] AS int_array,
            $2::bool[] AS bool_array
        FROM generate_series(1, 1)
       ';

        $stmt   = await($conn->prepare($sql));
        $stream = await($conn->executeStatementStream($stmt, ['{1,2,3}', '{t,f,t}'], 100));

        $rows = [];
        foreach ($stream as $row) {
            $rows[] = $row;
        }

        expect($rows)->toHaveCount(1);

        expect($rows[0]['int_array'])->toBe([1, 2, 3])
            ->and($rows[0]['bool_array'])->toBe([true, false, true])
        ;

        await($stmt->close());
        $conn->close();
    });

    it('casts an empty postgres array to an empty PHP array in prepared statements', function (): void {
        $conn = pgConn(['cast_prepared_types' => true]);

        $sql = 'SELECT $1::int[] AS empty_array';

        $stmt   = await($conn->prepare($sql));
        $result = await($conn->executeStatement($stmt, ['{}']));

        $row = $result->fetchOne();

        expect($row['empty_array'])->toBe([]);

        await($stmt->close());
        $conn->close();
    });
});
