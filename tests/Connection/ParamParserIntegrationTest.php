<?php

declare(strict_types=1);

use Hibla\Sql\Exceptions\QueryException;

use function Hibla\await;

describe('Live connection — positional params (?)', function () {

    it('executes a prepared statement with a single ? param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ? AS v'));

            try {
                $row = await($stmt->execute(['hello']))->fetchOne();
                expect($row['v'])->toBe('hello');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('executes with multiple ? params in correct order', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ?, ? AS b'));

            try {
                $row = await($stmt->execute(['first', 'second']))->fetchOne();
                expect(array_values($row))->toBe(['first', 'second']);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('executes with a boolean param (normalised to 0/1)', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ?::boolean AS flag'));

            try {
                $row = await($stmt->execute([true]))->fetchOne();
                expect($row['flag'])->toBe('t');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('executes with a null param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ?::text AS v'));

            try {
                $row = await($stmt->execute([null]))->fetchOne();
                expect($row['v'])->toBeNull();
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('executes the same prepared statement multiple times', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ? AS n'));

            try {
                foreach (['a', 'b', 'c'] as $val) {
                    $row = await($stmt->execute([$val]))->fetchOne();
                    expect($row['n'])->toBe($val);
                }
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});

describe('Live connection — named params (:name)', function () {

    it('executes a prepared statement with a single :name param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :greeting AS g'));

            try {
                $row = await($stmt->execute(['greeting' => 'Hello!']))->fetchOne();
                expect($row['g'])->toBe('Hello!');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('resolves named params regardless of associative key order', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :a AS a, :b AS b'));

            try {
                $row = await($stmt->execute(['b' => 'second', 'a' => 'first']))->fetchOne();
                expect($row['a'])->toBe('first')
                    ->and($row['b'])->toBe('second')
                ;
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('sends the same $n for a repeated :name', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :x::int + :x::int AS double'));

            try {
                $row = await($stmt->execute(['x' => '21']))->fetchOne();
                expect((int) $row['double'])->toBe(42);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('executes with a null named param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :v::text AS v'));

            try {
                $row = await($stmt->execute(['v' => null]))->fetchOne();
                expect($row['v'])->toBeNull();
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('throws when a required named param is omitted at execute time', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :a AS a, :b AS b'));

            try {
                expect(fn () => await($stmt->execute(['a' => 1])))
                    ->toThrow(QueryException::class, "':b'")
                ;
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('handles a named param next to a ::cast without misreading it', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val::int AS n'));

            try {
                $row = await($stmt->execute(['val' => '99']))->fetchOne();
                expect((int) $row['n'])->toBe(99);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('handles :name inside a SQL string literal correctly', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare("SELECT ':ignored' AS s, :real AS r"));

            try {
                $row = await($stmt->execute(['real' => 'yes']))->fetchOne();
                expect($row['s'])->toBe(':ignored')
                    ->and($row['r'])->toBe('yes')
                ;
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});

describe('Live connection — edge cases and guard rails', function () {

    it('rejects a multi-statement query with ? params', function () {
        $conn = pgConn();

        try {
            expect(fn () => await($conn->prepare('SELECT ?; SELECT ?')))
                ->toThrow(QueryException::class)
            ;
        } finally {
            $conn->close();
        }
    });

    it('rejects mixing ? and :name via the parser before hitting the server', function () {
        $conn = pgConn();

        try {
            expect(fn () => await($conn->prepare('SELECT ?, :name')))
                ->toThrow(QueryException::class, 'Cannot mix')
            ;
        } finally {
            $conn->close();
        }
    });

    it('executes a parameterless prepared statement just fine', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT 42 AS answer'));

            try {
                $row = await($stmt->execute([]))->fetchOne();
                expect((int) $row['answer'])->toBe(42);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('rejects execute() on a closed prepared statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :v AS v'));
            await($stmt->close());

            expect(fn () => $stmt->execute(['v' => 1]))
                ->toThrow(Hibla\Sql\Exceptions\PreparedException::class)
            ;
        } finally {
            $conn->close();
        }
    });

    it('can stream results from a named-param prepared statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare(
                'SELECT generate_series AS n FROM generate_series(1, :limit)'
            ));

            try {
                $stream = await($stmt->executeStream(['limit' => '5']));

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
});
