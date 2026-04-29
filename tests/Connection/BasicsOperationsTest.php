<?php

declare(strict_types=1);

use Hibla\Postgres\Interfaces\PgSqlRowStream;
use Hibla\Promise\Promise;
use Hibla\Sql\Exceptions\ConnectionException;
use Hibla\Sql\Exceptions\PreparedException;
use Hibla\Sql\Exceptions\QueryException;

use function Hibla\await;

describe('Basic Connection & Queries', function () {

    it('establishes a connection and can ping the server', function () {
        $conn = pgConn();

        try {
            expect($conn->isReady())->toBeTrue()->and($conn->isClosed())->toBeFalse();
            $ping = await($conn->ping());
            expect($ping)->toBeTrue();
        } finally {
            $conn->close();
        }
    });

    it('rejects commands on a closed connection', function () {
        $conn = pgConn();
        $conn->close();
        expect(fn () => await($conn->query('SELECT 1')))
            ->toThrow(ConnectionException::class, 'Connection is closed')
        ;
    });

    it('executes a basic plain query', function () {
        $conn = pgConn();

        try {
            $result = await($conn->query("SELECT 42 AS answer, 'hello' AS greeting"));
            $row = $result->fetchOne();
            expect($result->rowCount)->toBe(1)
                ->and((int) $row['answer'])->toBe(42)
                ->and($row['greeting'])->toBe('hello')
            ;
        } finally {
            $conn->close();
        }
    });

    it('executes a basic prepared statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val::text AS v'));

            try {
                $result = await($stmt->execute(['val' => 'async php']));
                $row = $result->fetchOne();
                expect($row['v'])->toBe('async php');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('streams a basic query row-by-row', function () {
        $conn = pgConn();

        try {
            $stream = await($conn->streamQuery('SELECT * FROM generate_series(1, 5) AS n'));
            expect($stream)->toBeInstanceOf(PgSqlRowStream::class);
            $rows = [];
            foreach ($stream as $row) {
                $rows[] = (int) $row['n'];
            }
            expect($rows)->toBe([1, 2, 3, 4, 5]);
        } finally {
            $conn->close();
        }
    });
});

describe('Concurrency', function () {

    it('executes queries concurrently across multiple connections using Promise::map', function () {
        $conns = [pgConn(), pgConn(), pgConn()];

        try {
            $start = microtime(true);

            await(Promise::map(
                $conns,
                fn ($conn) => $conn->query('SELECT pg_sleep(0.5)'),
                count($conns)
            ));

            $duration = microtime(true) - $start;

            expect($duration)
                ->toBeLessThan(1.0)
            ;
        } finally {
            foreach ($conns as $conn) {
                $conn->close();
            }
        }
    });
});

describe('Prepared Statement Handling', function () {

    it('executes the same statement multiple times with different params', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ? AS n'));

            try {
                $result1 = await($stmt->execute(['first']));
                expect($result1->fetchOne()['n'])->toBe('first');

                $result2 = await($stmt->execute(['second']));
                expect($result2->fetchOne()['n'])->toBe('second');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('handles an empty result set from a prepared statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT 1 WHERE 1 = ?'));

            try {
                $result = await($stmt->execute([0]));
                expect($result->rowCount)->toBe(0)
                    ->and($result->fetchAll())->toBe([])
                ;
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('rejects execution on a closed statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT 1'));
            await($stmt->close());

            expect(fn () => $stmt->execute())
                ->toThrow(PreparedException::class, 'Cannot execute a closed prepared statement')
            ;
        } finally {
            $conn->close();
        }
    });

    it('allows closing an already-closed statement without error', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT 1'));
            await($stmt->close());

            $result = await($stmt->close());
            expect($result)->toBeNull();
        } finally {
            $conn->close();
        }
    });

    it('can stream results from a prepared statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT * FROM generate_series(1, :limit) AS n'));

            try {
                $stream = await($stmt->executeStream(['limit' => 5]));
                $rows = [];
                foreach ($stream as $row) {
                    $rows[] = (int) $row['n'];
                }
                expect($rows)->toBe([1, 2, 3, 4, 5]);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('rejects execution if a required named parameter is missing', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :a, :b'));

            try {
                expect(fn () => await($stmt->execute(['a' => 1])))
                    ->toThrow(QueryException::class, "Missing value for named parameter ':b'")
                ;
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});
