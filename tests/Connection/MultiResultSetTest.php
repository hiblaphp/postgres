<?php

declare(strict_types=1);

use Hibla\Sql\Exceptions\QueryException;

use function Hibla\await;

describe('Multi-Result Set & Procedural Handling', function () {
    it('traverses a linked list of 3 distinct SELECT results', function () {
        $conn = pgConn();

        try {
            $sql = 'SELECT 1 AS a; SELECT 2 AS b, 3 AS c; SELECT 4 AS d;';
            $res1 = await($conn->query($sql));

            expect($res1->fetchOne()['a'])->toBe('1');

            $res2 = $res1->nextResult();
            expect($res2->columns)->toBe(['b', 'c'])
                ->and($res2->fetchOne()['c'])->toBe('3')
            ;

            $res3 = $res2->nextResult();
            expect($res3->fetchOne()['d'])->toBe('4')
                ->and($res3->nextResult())->toBeNull()
            ;
        } finally {
            $conn->close();
        }
    });

    it('verifies affectedRows and rowCount in a mixed DML chain', function () {
        $conn = pgConn();

        try {
            await($conn->query('CREATE TEMP TABLE multi_test (id INT)'));
            $sql = 'INSERT INTO multi_test VALUES (10), (20); UPDATE multi_test SET id = 30; SELECT * FROM multi_test';

            $res1 = await($conn->query($sql));
            expect($res1->affectedRows)->toBe(2)->and($res1->rowCount)->toBe(0);

            $res2 = $res1->nextResult();
            expect($res2->affectedRows)->toBe(2);

            $res3 = $res2->nextResult();
            expect($res3->rowCount)->toBe(2);
        } finally {
            $conn->close();
        }
    });

    it('rejects the entire promise if the middle statement fails', function () {
        $conn = pgConn();

        try {
            // Statement 1 ok, 2 fails, 3 would have been ok
            $sql = 'SELECT 1; SELECT * FROM invalid_table_name_abc; SELECT 2;';
            expect(fn () => await($conn->query($sql)))
                ->toThrow(QueryException::class)
            ;
        } finally {
            $conn->close();
        }
    });

    it('rejects immediately if the first statement of a multi-query is invalid', function () {
        $conn = pgConn();

        try {
            $sql = 'PARSE ERROR HERE; SELECT 1;';
            expect(fn () => await($conn->query($sql)))
                ->toThrow(QueryException::class)
            ;
        } finally {
            $conn->close();
        }
    });

    it('handles multiple statements that return zero rows', function () {
        $conn = pgConn();

        try {
            $sql = 'SELECT 1 WHERE 1=0; SELECT 2 WHERE 1=0;';
            $res1 = await($conn->query($sql));

            expect($res1->rowCount)->toBe(0)
                ->and($res1->nextResult())->not->toBeNull()
            ;

            expect($res1->nextResult()->rowCount)->toBe(0);
        } finally {
            $conn->close();
        }
    });

    it('does not split statements on semicolons found inside quotes', function () {
        $conn = pgConn();

        try {
            // This is ONE statement, not two.
            $sql = "SELECT 'hello; world' AS val";
            $res = await($conn->query($sql));

            expect($res->fetchOne()['val'])->toBe('hello; world')
                ->and($res->nextResult())->toBeNull()
            ;
        } finally {
            $conn->close();
        }
    });

    it('handles extra semicolons and trailing comments at the end of a block', function () {
        $conn = pgConn();

        try {
            $sql = "SELECT 1 AS a;; ; -- This is a comment\nSELECT 2 AS b;";
            $res1 = await($conn->query($sql));

            expect($res1->fetchOne()['a'])->toBe('1');
            expect($res1->nextResult()->fetchOne()['b'])->toBe('2');
        } finally {
            $conn->close();
        }
    });

    it('executes a stored function returning a result set', function () {
        $conn = pgConn();

        try {
            await($conn->query('
                CREATE OR REPLACE FUNCTION get_test_data(val TEXT) 
                RETURNS TABLE(v TEXT, l INT) AS $$
                BEGIN
                    RETURN QUERY SELECT val, length(val);
                END; $$ LANGUAGE plpgsql;
            '));

            $res = await($conn->query("SELECT * FROM get_test_data('hibla')"));
            $row = $res->fetchOne();

            expect($row['v'])->toBe('hibla')->and((int)$row['l'])->toBe(5);
        } finally {
            $conn->close();
        }
    });

    it('handles an empty or whitespace-only query string gracefully', function () {
        $conn = pgConn();

        try {
            $res = await($conn->query('  ;  '));
            expect($res->rowCount)->toBe(0);
        } finally {
            $conn->close();
        }
    });

    it('verifies that a failed multi-query taints the transaction', function () {
        $conn = pgConn();

        try {
            await($conn->query('BEGIN'));

            try {
                await($conn->query('SELECT 1; SELECT * FROM non_existent_table_123;'));
            } catch (QueryException $e) {
                // Expected failure to trigger the taint
            }

            try {
                await($conn->query('SELECT 1'));

                throw new Exception('Transaction should have been aborted but it was not.');
            } catch (QueryException $e) {
                expect($e->getMessage())->toContain('current transaction is aborted');
            }

            await($conn->query('ROLLBACK'));
        } finally {
            $conn->close();
        }
    });
});
