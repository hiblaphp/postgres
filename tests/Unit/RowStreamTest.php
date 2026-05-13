<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Interfaces\StreamContext;
use Hibla\Postgres\Internals\RowStream;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Promise\Promise;
use Hibla\Sql\RowStream as SqlRowStream;

covers(RowStream::class);

function makeStream(int $bufferSize = 5): RowStream
{
    return new RowStream($bufferSize);
}

function makeRow(int $id): array
{
    return ['id' => $id, 'name' => "User {$id}"];
}

function pushRows(RowStream $stream, int $count = 3): array
{
    $rows = [];

    for ($i = 1; $i <= $count; $i++) {
        $row = makeRow($i);
        $rows[] = $row;
        $stream->push($row);
    }

    return $rows;
}

function drainIterator(Generator $gen): array
{
    $collected = [];

    foreach ($gen as $row) {
        $collected[] = $row;
    }

    return $collected;
}

afterEach(function () {
    Loop::reset();
    Mockery::close();
});

describe('construction', function () {
    it('implements SqlRowStream and StreamContext', function () {
        expect(makeStream())
            ->toBeInstanceOf(SqlRowStream::class)
            ->toBeInstanceOf(StreamContext::class)
        ;
    });

    it('stores the buffer size', function () {
        expect(makeStream(50)->bufferSize)->toBe(50);
    });

    it('throws when buffer size is zero', function () {
        expect(fn () => new RowStream(0))->toThrow(InvalidArgumentException::class);
    });

    it('throws when buffer size is negative', function () {
        expect(fn () => new RowStream(-1))->toThrow(InvalidArgumentException::class);
    });

    it('starts with zero columns', function () {
        expect(makeStream())
            ->columnCount->toBe(0)
            ->columns->toBe([])
        ;
    });
});

describe('isFull()', function () {
    it('returns false on a fresh stream', function () {
        expect(makeStream()->isFull())->toBeFalse();
    });

    it('returns false when the buffer has room', function () {
        $stream = makeStream(5);
        $stream->push(makeRow(1));

        expect($stream->isFull())->toBeFalse();
    });

    it('returns true when the buffer is at capacity', function () {
        $stream = makeStream(3);
        pushRows($stream, 3);

        expect($stream->isFull())->toBeTrue();
    });
});

describe('column tracking', function () {
    it('populates columns from the first pushed row', function () {
        $stream = makeStream();
        $stream->push(['id' => 1, 'name' => 'Alice', 'email' => 'a@example.com']);

        expect($stream->columns)->toBe(['id', 'name', 'email'])
            ->and($stream->columnCount)->toBe(3)
        ;
    });

    it('does not update columns on subsequent rows', function () {
        $stream = makeStream();
        $stream->push(['id' => 1, 'name' => 'Alice']);
        $stream->push(['id' => 2, 'name' => 'Bob', 'extra' => 'ignored']);

        expect($stream->columns)->toBe(['id', 'name'])
            ->and($stream->columnCount)->toBe(2)
        ;
    });

    it('remains empty when the stream completes with no rows', function () {
        $stream = makeStream();
        $stream->complete();

        expect($stream->columns)->toBe([])
            ->and($stream->columnCount)->toBe(0)
        ;
    });
});

describe('outer promise callbacks', function () {
    it('fires onReady exactly once on the first push', function () {
        $stream = makeStream();
        $calls = 0;

        $stream->setOuterPromiseCallbacks(
            onReady: function () use (&$calls) {
                $calls++;
            },
            onError: fn () => null,
        );

        $stream->push(makeRow(1));
        $stream->push(makeRow(2));

        expect($calls)->toBe(1);
    });

    it('fires onReady once on complete() when no rows were pushed', function () {
        $stream = makeStream();
        $calls = 0;

        $stream->setOuterPromiseCallbacks(
            onReady: function () use (&$calls) {
                $calls++;
            },
            onError: fn () => null,
        );

        $stream->complete();
        $stream->complete();

        expect($calls)->toBe(1);
    });

    it('fires onError when error() is called before any row arrives', function () {
        $stream = makeStream();
        $caught = null;
        $exception = new RuntimeException('DB error');

        $stream->setOuterPromiseCallbacks(
            onReady: fn () => null,
            onError: function (Throwable $e) use (&$caught) {
                $caught = $e;
            },
        );

        $stream->error($exception);

        expect($caught)->toBe($exception);
    });

    it('does not fire onError after onReady has already fired', function () {
        $stream = makeStream();
        $errorFired = false;

        $stream->setOuterPromiseCallbacks(
            onReady: fn () => null,
            onError: function () use (&$errorFired) {
                $errorFired = true;
            },
        );

        $stream->push(makeRow(1));
        $stream->error(new RuntimeException('too late'));

        expect($errorFired)->toBeFalse();
    });

    it('does not fire either callback after cancel()', function () {
        $stream = makeStream();
        $fired = false;

        $stream->setOuterPromiseCallbacks(
            onReady: function () use (&$fired) {
                $fired = true;
            },
            onError: function () use (&$fired) {
                $fired = true;
            },
        );

        $stream->cancel();
        $stream->push(makeRow(1));
        $stream->complete();
        $stream->error(new RuntimeException());

        expect($fired)->toBeFalse();
    });
});

describe('cancel()', function () {
    it('marks the stream as cancelled', function () {
        $stream = makeStream();
        $stream->cancel();

        expect($stream->isCancelled())->toBeTrue();
    });

    it('is idempotent', function () {
        $stream = makeStream();
        $stream->cancel();

        expect(fn () => $stream->cancel())->not->toThrow(Throwable::class);
        expect($stream->isCancelled())->toBeTrue();
    });

    it('fires onCancelCallback exactly once', function () {
        $stream = makeStream();
        $calls = 0;

        $stream->onCancel(function () use (&$calls) {
            $calls++;
        });
        $stream->cancel();
        $stream->cancel();

        expect($calls)->toBe(1);
    });

    it('fires onCancelCallback but does not fire onReady', function () {
        $stream = makeStream();
        $order = [];

        $stream->setOuterPromiseCallbacks(
            onReady: function () use (&$order) {
                $order[] = 'ready';
            },
            onError: fn () => null,
        );
        $stream->onCancel(function () use (&$order) {
            $order[] = 'cancel';
        });

        $stream->cancel();

        expect($order)->toBe(['cancel']);
    });

    it('does not accept new rows after cancellation', function () {
        $stream = makeStream(10);
        $stream->cancel();
        $stream->push(makeRow(1));

        expect($stream->isFull())->toBeFalse();
    });

    it('cancels an unsettled command promise', function () {
        $stream = makeStream();
        $promise = new Promise();

        $stream->bindCommandPromise($promise);
        $stream->cancel();

        expect($promise->isCancelled())->toBeTrue();
    });

    it('skips cancel() on an already-settled command promise', function () {
        $stream = makeStream();
        $promise = new Promise();
        $promise->resolve('done');

        $stream->bindCommandPromise($promise);
        $stream->cancel();

        expect($promise->isCancelled())->toBeFalse();
    });
});

describe('waitForCommand()', function () {
    it('throws when no command promise is bound', function () {
        expect(fn () => makeStream()->waitForCommand())
            ->toThrow(RuntimeException::class, 'Command promise not bound to stream.')
        ;
    });

    it('returns the bound promise', function () {
        $stream = makeStream();
        $promise = new Promise();

        $stream->bindCommandPromise($promise);

        expect($stream->waitForCommand())->toBe($promise);
    });
});

describe('getIterator() — pre-buffered', function () {
    it('yields all buffered rows in insertion order', function () {
        $stream = makeStream();
        $rows = pushRows($stream, 3);
        $stream->complete();

        expect(drainIterator($stream->getIterator()))->toBe($rows);
    });

    it('yields nothing when the stream completes with no rows', function () {
        $stream = makeStream();
        $stream->complete();

        expect(drainIterator($stream->getIterator()))->toBe([]);
    });

    it('throws the stored error when error() was called before iteration', function () {
        $stream = makeStream();
        $stream->error(new RuntimeException('fatal'));

        expect(fn () => drainIterator($stream->getIterator()))
            ->toThrow(RuntimeException::class, 'fatal')
        ;
    });

    it('throws CancelledException when iterating a cancelled stream', function () {
        $stream = makeStream();
        $stream->cancel();

        expect(fn () => drainIterator($stream->getIterator()))
            ->toThrow(CancelledException::class)
        ;
    });

    it('throws immediately on first iteration when error() was called before iterating', function () {
        $stream = makeStream(10);
        $stream->push(makeRow(1));
        $stream->push(makeRow(2));
        $stream->error(new RuntimeException('mid-stream'));

        $gen = $stream->getIterator();
        $collected = [];

        expect(function () use ($gen, &$collected) {
            foreach ($gen as $row) {
                $collected[] = $row;
            }
        })->toThrow(RuntimeException::class, 'mid-stream');

        expect($collected)->toBe([]);
    });
});

describe('getIterator() — async', function () {
    it('suspends and yields a row when push() arrives after iteration starts', function () {
        $stream = makeStream();
        $collected = [];

        Loop::nextTick(function () use ($stream) {
            $stream->push(makeRow(1));
            $stream->complete();
        });

        foreach ($stream->getIterator() as $row) {
            $collected[] = $row;
        }

        expect($collected)->toBe([makeRow(1)]);
    });

    it('resolves the waiter directly via push() instead of buffering', function () {
        $stream = makeStream();
        $collected = [];

        Loop::nextTick(function () use ($stream) {
            $stream->push(makeRow(1));
            $stream->push(makeRow(2));
            $stream->push(makeRow(3));
            $stream->complete();
        });

        foreach ($stream->getIterator() as $row) {
            $collected[] = $row;
        }

        expect($collected)->toHaveCount(3)
            ->and($collected[0])->toBe(makeRow(1))
        ;
    });

    it('suspends and ends cleanly when complete() arrives on an empty buffer', function () {
        $stream = makeStream();
        $collected = [];

        Loop::nextTick(fn () => $stream->complete());

        foreach ($stream->getIterator() as $row) {
            $collected[] = $row;
        }

        expect($collected)->toBe([]);
    });

    it('suspends and throws when error() arrives mid-iteration', function () {
        $stream = makeStream();
        $exception = new RuntimeException('async error');

        Loop::nextTick(function () use ($stream, $exception) {
            $stream->push(makeRow(1));
            $stream->error($exception);
        });

        $collected = [];

        expect(function () use ($stream, &$collected) {
            foreach ($stream->getIterator() as $row) {
                $collected[] = $row;
            }
        })->toThrow(RuntimeException::class, 'async error');

        expect($collected)->toBe([makeRow(1)]);
    });

    it('suspends and throws CancelledException when cancel() is called mid-iteration', function () {
        $stream = makeStream();

        Loop::nextTick(function () use ($stream) {
            $stream->push(makeRow(1));
            $stream->cancel();
        });

        $collected = [];

        expect(function () use ($stream, &$collected) {
            foreach ($stream->getIterator() as $row) {
                $collected[] = $row;
            }
        })->toThrow(CancelledException::class);

        expect($collected)->toBe([makeRow(1)]);
    });

    it('yields rows across multiple deferred pushes before completing', function () {
        $stream = makeStream();
        $collected = [];

        Loop::defer(function () use ($stream) {
            $stream->push(makeRow(1));
            $stream->push(makeRow(2));
        });

        Loop::defer(function () use ($stream) {
            $stream->push(makeRow(3));
            $stream->complete();
        });

        foreach ($stream->getIterator() as $row) {
            $collected[] = $row;
        }

        expect($collected)->toBe([makeRow(1), makeRow(2), makeRow(3)]);
    });
});

describe('resumeCallback', function () {
    it('fires when the buffer drops to or below half capacity during sync iteration', function () {
        $stream = makeStream(4);
        $calls = 0;

        $stream->setResumeCallback(function () use (&$calls) {
            $calls++;
        });

        pushRows($stream, 4);
        $stream->complete();

        drainIterator($stream->getIterator());

        expect($calls)->toBeGreaterThanOrEqual(1);
    });

    it('fires during async iteration when the buffer is empty and the iterator suspends', function () {
        $stream = makeStream();
        $resumeFired = false;

        $stream->setResumeCallback(function () use (&$resumeFired) {
            $resumeFired = true;
        });

        Loop::defer(fn () => $stream->push(makeRow(1)));
        Loop::defer(fn () => $stream->complete());

        drainIterator($stream->getIterator());

        expect($resumeFired)->toBeTrue();
    });
});

describe('__destruct()', function () {
    it('calls cancel() when the stream is neither complete nor cancelled', function () {
        $cancelFired = false;

        $factory = function () use (&$cancelFired): void {
            $stream = makeStream();
            $stream->onCancel(function () use (&$cancelFired) {
                $cancelFired = true;
            });
        };

        $factory();

        expect($cancelFired)->toBeTrue();
    });

    it('does not call cancel() when the stream already completed', function () {
        $cancelFired = false;

        $factory = function () use (&$cancelFired): void {
            $stream = makeStream();
            $stream->onCancel(function () use (&$cancelFired) {
                $cancelFired = true;
            });
            $stream->complete();
        };

        $factory();

        expect($cancelFired)->toBeFalse();
    });

    it('does not call cancel() twice when already cancelled', function () {
        $calls = 0;

        $factory = function () use (&$calls): void {
            $stream = makeStream();
            $stream->onCancel(function () use (&$calls) {
                $calls++;
            });
            $stream->cancel();
        };

        $factory();

        expect($calls)->toBe(1);
    });
});
