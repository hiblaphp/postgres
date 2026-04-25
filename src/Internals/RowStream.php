<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PgSqlRowStream;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use SplQueue;
use Throwable;

use function Hibla\await;

/**
 * @internal
 */
class RowStream implements PgSqlRowStream
{
    private SplQueue $buffer;

    private array $columnNames = [];

    private ?Promise $waiter = null;

    private ?PromiseInterface $commandPromise = null;

    private ?Throwable $error = null;

    private bool $completed = false;

    private bool $cancelled = false;

    /**
     * @var \Closure(): void|null
     */
    private ?\Closure $resumeCallback = null;

    /**
     * @inheritDoc
     */
    public int $columnCount {
        get => \count($this->columnNames);
    }

    /**
     * @inheritDoc
     */
    public array $columns {
        get => $this->columnNames;
    }

    public function __construct(public readonly int $bufferSize = 100)
    {
        $this->buffer = new SplQueue();
    }

    public function isFull(): bool
    {
        return $this->buffer->count() >= $this->bufferSize;
    }

    public function setResumeCallback(\Closure $callback): void
    {
        $this->resumeCallback = $callback;
    }

    /**
     * @internal
     */
    public function bindCommandPromise(PromiseInterface $promise): void
    {
        $this->commandPromise = $promise;
    }

    /**
     * @inheritdoc
     */
    public function getIterator(): \Generator
    {
        while (true) {
            if ($this->error !== null) {
                throw $this->error;
            }

            if (! $this->buffer->isEmpty()) {
                $row = $this->buffer->dequeue();

                // BACKPRESSURE: buffer dropped below half — tell the connection to resume
                if ($this->resumeCallback !== null && $this->buffer->count() <= ($this->bufferSize / 2)) {
                    ($this->resumeCallback)();
                }

                yield $row;

                continue;
            }

            if ($this->completed) {
                break;
            }

            // Buffer is empty and stream is not done — resume the connection
            // so it starts sending rows again, then suspend until data arrives
            if ($this->resumeCallback !== null) {
                ($this->resumeCallback)();
            }

            $this->waiter = new Promise();
            $row = await($this->waiter);

            if ($row === null) {
                break;
            }

            yield $row;
        }
    }

    /**
     * @inheritdoc
     */
    public function cancel(): void
    {
        if ($this->cancelled) {
            return;
        }

        $this->cancelled = true;
        $this->error = new CancelledException('Stream was cancelled');
        $this->completed = true;

        if ($this->commandPromise !== null && ! $this->commandPromise->isSettled()) {
            $this->commandPromise->cancel();
        }

        if ($this->waiter !== null) {
            $waiter = $this->waiter;
            $this->waiter = null;
            $waiter->reject($this->error);
        }

        $this->buffer = new SplQueue();
    }

    /**
     * @inheritdoc
     */
    public function isCancelled(): bool
    {
        return $this->cancelled;
    }

    /**
     * @internal
     */
    public function push(array $row): void
    {
        if ($this->cancelled) {
            return;
        }

        if ($this->columnNames === []) {
            $this->columnNames = array_keys($row);
        }

        if ($this->waiter !== null) {
            $promise = $this->waiter;
            $this->waiter = null;
            $promise->resolve($row);
        } else {
            $this->buffer->enqueue($row);
        }
    }

    /**
     * @internal
     */
    public function complete(): void
    {
        if ($this->cancelled) {
            return;
        }

        $this->completed = true;

        if ($this->waiter !== null) {
            $promise = $this->waiter;
            $this->waiter = null;
            $promise->resolve(null);
        }
    }

    /**
     * @internal
     */
    public function error(Throwable $e): void
    {
        if ($this->cancelled) {
            return;
        }

        $this->error = $e;
        $this->completed = true;

        if ($this->waiter !== null) {
            $promise = $this->waiter;
            $this->waiter = null;
            $promise->reject($e);
        }
    }

    public function __destruct()
    {
        if (! $this->completed && ! $this->cancelled) {
            $this->cancel();
        }
    }
}
