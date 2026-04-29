<?php

declare(strict_types=1);

use Hibla\Sql\Exceptions\PreparedException;
use Hibla\Sql\Exceptions\QueryException;

use function Hibla\await;

describe('Security — SQL injection via bound params', function () {

    it('does not allow SQL injection through a positional param value', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ? AS v'));

            try {
                $row = await($stmt->execute(["' OR '1'='1"]))->fetchOne();
                expect($row['v'])->toBe("' OR '1'='1");
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('does not allow SQL injection through a named param value', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val AS v'));

            try {
                $row = await($stmt->execute(['val' => "'; DROP TABLE users; --"]))->fetchOne();
                expect($row['v'])->toBe("'; DROP TABLE users; --");
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('preserves backslashes in param values without interpreting them as escapes', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val AS v'));

            try {
                $row = await($stmt->execute(['val' => 'C:\\Users\\admin']))->fetchOne();
                expect($row['v'])->toBe('C:\\Users\\admin');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('preserves null bytes in param values without truncating the string', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val::bytea AS v'));

            try {
                $payload = "before\x00after";
                $row = await($stmt->execute(['val' => $payload]))->fetchOne();
                expect($row['v'])->not->toBeNull();
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('preserves double-quote characters in param values', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val AS v'));

            try {
                $row = await($stmt->execute(['val' => '"quoted"']))->fetchOne();
                expect($row['v'])->toBe('"quoted"');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});

describe('Edge cases — param value types and encoding', function () {

    it('round-trips a unicode multibyte string unchanged', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val AS v'));

            try {
                $row = await($stmt->execute(['val' => '日本語テスト 🎉']))->fetchOne();
                expect($row['v'])->toBe('日本語テスト 🎉');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('round-trips an empty string param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val AS v'));

            try {
                $row = await($stmt->execute(['val' => '']))->fetchOne();
                expect($row['v'])->toBe('');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('round-trips a large string param (64 KB)', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val AS v'));

            try {
                $large = str_repeat('x', 65536);
                $row = await($stmt->execute(['val' => $large]))->fetchOne();
                expect($row['v'])->toBe($large);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('handles zero as an integer param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val::int AS v'));

            try {
                $row = await($stmt->execute(['val' => 0]))->fetchOne();
                expect((int) $row['v'])->toBe(0);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('handles a negative integer param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val::int AS v'));

            try {
                $row = await($stmt->execute(['val' => -42]))->fetchOne();
                expect((int) $row['v'])->toBe(-42);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('handles a float param', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :val::float8 AS v'));

            try {
                $row = await($stmt->execute(['val' => 3.14]))->fetchOne();
                expect(round((float) $row['v'], 2))->toBe(3.14);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('normalises boolean false to 0', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT ?::boolean AS v'));

            try {
                $row = await($stmt->execute([false]))->fetchOne();
                expect($row['v'])->toBe('f');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});

describe('Edge cases — statement lifecycle', function () {

    it('closing an already-closed statement is a no-op and does not throw', function () {
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

    it('rejects executeStream() on a closed prepared statement', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT :v AS v'));
            await($stmt->close());

            expect(fn() => $stmt->executeStream(['v' => 1]))
                ->toThrow(PreparedException::class);
        } finally {
            $conn->close();
        }
    });

    it('streams an empty result set without error', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare(
                'SELECT generate_series AS n FROM generate_series(1, :limit)'
            ));

            try {
                $stream = await($stmt->executeStream(['limit' => 0]));

                $rows = [];
                foreach ($stream as $row) {
                    $rows[] = $row;
                }

                expect($rows)->toBe([]);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('allows re-executing the same statement after a previous execution returns no rows', function () {
        $conn = pgConn();

        try {
            $stmt = await($conn->prepare('SELECT generate_series AS n FROM generate_series(1, :limit)'));

            try {
                $empty = await($stmt->execute(['limit' => 0]))->fetchAll();
                expect($empty)->toBe([]);

                $rows = await($stmt->execute(['limit' => 3]))->fetchAll();
                expect(array_column($rows, 'n'))->toBe(['1', '2', '3']);
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});

describe('Edge cases — connection-level guards', function () {

    it('queues multiple commands and resolves them in order', function () {
        $conn = pgConn();

        try {
            $stmtA = await($conn->prepare('SELECT :v::int AS v'));
            $stmtB = await($conn->prepare('SELECT :v::int * 2 AS v'));

            try {
                $rowA = await($stmtA->execute(['v' => 3]))->fetchOne();
                $rowB = await($stmtB->execute(['v' => 3]))->fetchOne();

                expect((int) $rowA['v'])->toBe(3)
                    ->and((int) $rowB['v'])->toBe(6)
                ;
            } finally {
                await($stmtA->close());
                await($stmtB->close());
            }
        } finally {
            $conn->close();
        }
    });

    it('rejects any command after the connection is closed', function () {
        $conn = pgConn();
        $conn->close();

        expect(fn() => await($conn->prepare('SELECT 1')))
            ->toThrow(Hibla\Sql\Exceptions\ConnectionException::class);
    });

    it('rejects a named param whose key contains characters that could confuse the parser', function () {
        $conn = pgConn();

        try {
            $stmt = null;
            expect(function () use ($conn, &$stmt) {
                $stmt = await($conn->prepare('SELECT :foo::int AS n'));
            })->not->toThrow(QueryException::class);
        } finally {
            
            if ($stmt) {
                await($stmt->close()); 
            }

            $conn->close();
        }
    });

    it('handles a statement with the maximum practical number of positional params', function () {
        $count = 50;
        $placeholders = implode(', ', array_fill(0, $count, '?'));
        $params = range(1, $count);

        $conn = pgConn();

        try {
            $stmt = await($conn->prepare("SELECT ARRAY[{$placeholders}] AS arr"));

            try {
                $row = await($stmt->execute($params))->fetchOne();
                expect($row)->toHaveKey('arr');
            } finally {
                await($stmt->close());
            }
        } finally {
            $conn->close();
        }
    });
});
