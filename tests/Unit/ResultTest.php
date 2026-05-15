<?php

declare(strict_types=1);

use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Internals\Result;

function makeRows(): array
{
    return [
        ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
        ['id' => 2, 'name' => 'Bob',   'email' => 'bob@example.com'],
        ['id' => 3, 'name' => 'Carol', 'email' => 'carol@example.com'],
    ];
}

function makeColumns(): array
{
    return [0 => 'id', 1 => 'name', 2 => 'email'];
}

function makeResult(array $overrides = []): Result
{
    return new Result(
        affectedRows: $overrides['affectedRows'] ?? 0,
        lastInsertId: $overrides['lastInsertId'] ?? 0,
        connectionId: $overrides['connectionId'] ?? 0,
        insertedOid: $overrides['insertedOid'] ?? null,
        columns: $overrides['columns'] ?? makeColumns(),
        rows: $overrides['rows'] ?? makeRows(),
    );
}

covers(Result::class);

describe('construction', function () {
    it('implements PostgresResult interface', function () {
        expect(new Result())->toBeInstanceOf(PostgresResult::class);
    });

    it('initialises with correct defaults', function () {
        $result = new Result();

        expect($result)
            ->affectedRows->toBe(0)
            ->lastInsertId->toBe(0)
            ->connectionId->toBe(0)
            ->insertedOid->toBeNull()
            ->columns->toBe([])
            ->rowCount->toBe(0)
            ->columnCount->toBe(0)
        ;
    });

    it('derives rowCount and columnCount from constructor inputs', function () {
        $result = makeResult();

        expect($result)
            ->rowCount->toBe(3)
            ->columnCount->toBe(3)
        ;
    });

    it('stores insertedOid when provided', function () {
        expect(makeResult(['insertedOid' => 42]))->insertedOid->toBe(42);
    });

    it('stores connectionId', function () {
        expect(makeResult(['connectionId' => 9001]))->connectionId->toBe(9001);
    });
});

describe('hasAffectedRows()', function () {
    it('returns true when affectedRows is positive', function () {
        expect(makeResult(['affectedRows' => 1])->hasAffectedRows())->toBeTrue();
        expect(makeResult(['affectedRows' => 99])->hasAffectedRows())->toBeTrue();
    });

    it('returns false when affectedRows is zero', function () {
        expect(makeResult(['affectedRows' => 0])->hasAffectedRows())->toBeFalse();
    });
});

describe('hasLastInsertId()', function () {
    it('returns true when lastInsertId is positive', function () {
        expect(makeResult(['lastInsertId' => 1])->hasLastInsertId())->toBeTrue();
    });

    it('returns false when lastInsertId is zero', function () {
        expect(makeResult(['lastInsertId' => 0])->hasLastInsertId())->toBeFalse();
    });
});

describe('isEmpty()', function () {
    it('returns true when there are no rows', function () {
        expect(makeResult(['rows' => []])->isEmpty())->toBeTrue();
    });

    it('returns false when rows exist', function () {
        expect(makeResult()->isEmpty())->toBeFalse();
    });
});

// -------------------------------------------------------------------------
// fetchAssoc — cursor behaviour
// -------------------------------------------------------------------------

describe('fetchAssoc()', function () {
    it('returns rows sequentially on each call', function () {
        $rows = makeRows();
        $result = makeResult();

        expect($result->fetchAssoc())->toBe($rows[0])
            ->and($result->fetchAssoc())->toBe($rows[1])
            ->and($result->fetchAssoc())->toBe($rows[2])
        ;
    });

    it('returns null once the cursor is exhausted', function () {
        $result = makeResult();

        $result->fetchAssoc();
        $result->fetchAssoc();
        $result->fetchAssoc();

        expect($result->fetchAssoc())->toBeNull()
            ->and($result->fetchAssoc())->toBeNull() // idempotent
        ;
    });

    it('returns null immediately on an empty result set', function () {
        expect(makeResult(['rows' => []])->fetchAssoc())->toBeNull();
    });
});

// -------------------------------------------------------------------------
// fetchAll
// -------------------------------------------------------------------------

describe('fetchAll()', function () {
    it('returns all rows', function () {
        expect(makeResult()->fetchAll())->toBe(makeRows());
    });

    it('returns an empty array when there are no rows', function () {
        expect(makeResult(['rows' => []])->fetchAll())->toBe([]);
    });

    it('does not advance the fetchAssoc cursor', function () {
        $result = makeResult();
        $result->fetchAll();

        expect($result->fetchAssoc())->toBe(makeRows()[0]);
    });
});

// -------------------------------------------------------------------------
// fetchOne
// -------------------------------------------------------------------------

describe('fetchOne()', function () {
    it('returns the first row without advancing the cursor', function () {
        $result = makeResult();

        expect($result->fetchOne())->toBe(makeRows()[0])
            ->and($result->fetchOne())->toBe(makeRows()[0]) // repeatable
        ;
    });

    it('returns null when the result set is empty', function () {
        expect(makeResult(['rows' => []])->fetchOne())->toBeNull();
    });
});

// -------------------------------------------------------------------------
// fetchColumn
// -------------------------------------------------------------------------

describe('fetchColumn()', function () {
    it('extracts a column by name', function () {
        expect(makeResult()->fetchColumn('name'))->toBe(['Alice', 'Bob', 'Carol']);
    });

    it('extracts a column by zero-based integer index', function () {
        $result = makeResult();

        expect($result->fetchColumn(0))->toBe([1, 2, 3])
            ->and($result->fetchColumn(1))->toBe(['Alice', 'Bob', 'Carol'])
            ->and($result->fetchColumn(2))->toBe(['alice@example.com', 'bob@example.com', 'carol@example.com'])
        ;
    });

    it('defaults to index 0 when no argument is given', function () {
        expect(makeResult()->fetchColumn())->toBe([1, 2, 3]);
    });

    it('returns null for each row when a column name does not exist', function () {
        expect(makeResult()->fetchColumn('nonexistent'))->toBe([null, null, null]);
    });

    it('returns null for each row when an integer index is out of bounds', function () {
        expect(makeResult()->fetchColumn(99))->toBe([null, null, null]);
    });

    it('returns an empty array when there are no rows', function () {
        expect(makeResult(['rows' => []])->fetchColumn('id'))->toBe([]);
    });
});

describe('getIterator()', function () {
    it('returns an ArrayIterator', function () {
        expect(makeResult()->getIterator())->toBeInstanceOf(ArrayIterator::class);
    });

    it('iterates over all rows in order', function () {
        $collected = [];

        foreach (makeResult() as $row) {
            $collected[] = $row;
        }

        expect($collected)->toBe(makeRows());
    });

    it('produces an empty iterator when there are no rows', function () {
        $collected = [];

        foreach (makeResult(['rows' => []]) as $row) {
            $collected[] = $row;
        }

        expect($collected)->toBe([]);
    });
});

describe('nextResult()', function () {
    it('returns null by default', function () {
        expect(makeResult()->nextResult())->toBeNull();
    });

    it('returns the linked result after setNextResult() is called', function () {
        $first = makeResult();
        $second = makeResult(['rows' => [['id' => 4, 'name' => 'Dave', 'email' => 'dave@example.com']]]);

        $first->setNextResult($second);

        expect($first->nextResult())->toBe($second);
    });

    it('allows chaining multiple result sets', function () {
        $first = makeResult();
        $second = makeResult(['rows' => []]);
        $third = makeResult(['affectedRows' => 5]);

        $first->setNextResult($second);
        $second->setNextResult($third);

        expect($first->nextResult())->toBe($second)
            ->and($first->nextResult()->nextResult())->toBe($third)
            ->and($first->nextResult()->nextResult()->nextResult())->toBeNull()
        ;
    });
});
