<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PostgresRowStream;
use Hibla\Postgres\Interfaces\StreamContext;
use Hibla\Promise\Exceptions\CancelledException;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use SplQueue;
use Throwable;

use function Hibla\await;

/**
 * @internal
 */
class RowStream implements PostgresRowStream, StreamContext
{
    /**
     * @var SplQueue<array<string, string|null>>
     */
    private SplQueue $buffer;

    /**
     * @var list<string>
     */
    private array $columnNames = [];

    /**
     * @var Promise<array<string, string|null>|null>|null
     */
    private ?Promise $waiter = null;

    /**
     * @var PromiseInterface<mixed>|null
     */
    private ?PromiseInterface $commandPromise = null;

    private ?Throwable $error = null;

    private bool $completed = false;

    private bool $cancelled = false;

    /**
     * @var \Closure(): void|null
     */
    private ?\Closure $resumeCallback = null;

    /**
     * Called once when the first row arrives or the stream completes with no
     * rows. Used to resolve the outer borrow promise returned by streamQuery()
     * and executeStatementStream().
     *
     * @var \Closure(): void|null
     */
    private ?\Closure $onReady = null;

    /**
     * Called when the stream fails before the first row arrives.
     * Used to reject the outer borrow promise.
     *
     * @var \Closure(Throwable): void|null
     */
    private ?\Closure $onError = null;

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
        if ($this->bufferSize <= 0) {
            throw new \InvalidArgumentException('Buffer size must be greater than 0.');
        }

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
     * Returns a promise that resolves when the underlying database command is fully complete
     * and the connection is ready to be released back to the pool.
     *
     * @return PromiseInterface<mixed>
     *
     * @internal
     */
    public function waitForCommand(): PromiseInterface
    {
        if ($this->commandPromise === null) {
            throw new \RuntimeException('Command promise not bound to stream.');
        }

        return $this->commandPromise;
    }

    /**
     * Wires the two outer promise callbacks used by the two-phase stream design.
     *
     * $onReady  — resolves the outer promise on the first row or empty completion.
     * $onError  — rejects the outer promise on a pre-first-row failure.
     *
     * Both callbacks are one-shot: whichever fires first clears both references
     * so subsequent push()/complete()/error() calls are not double-delivered.
     *
     * @internal
     */
    public function setOuterPromiseCallbacks(\Closure $onReady, \Closure $onError): void
    {
        $this->onReady = $onReady;
        $this->onError = $onError;
    }

    /**
     * @internal
     *
     * @param PromiseInterface<mixed> $promise
     */
    public function bindCommandPromise(PromiseInterface $promise): void
    {
        $this->commandPromise = $promise;
    }

    /**
     * @inheritdoc
     *
     * @return \Generator<int, array<string, string|null>, mixed, void>
     */
    public function getIterator(): \Generator
    {
        while (true) {
            if ($this->error !== null) {
                throw $this->error;
            }

            if (! $this->buffer->isEmpty()) {
                $row = $this->buffer->dequeue();

                if ($this->resumeCallback !== null && $this->buffer->count() <= ($this->bufferSize / 2)) {
                    ($this->resumeCallback)();
                }

                yield $row;

                continue;
            }

            if ($this->completed) {
                break;
            }

            if ($this->resumeCallback !== null) {
                ($this->resumeCallback)();
            }

            $this->waiter = new Promise(); // @phpstan-ignore-line
            $row = await($this->waiter);

            if ($row === null) {
                break;
            }

            //@phpstan-ignore-next-line
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

        // Clear outer promise callbacks — cancellation is signalled via the
        // outer promise's onCancel handler, not through these callbacks.
        $this->onReady = null;
        $this->onError = null;

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
     *
     * @param array<string, string|null> $row
     */
    public function push(array $row): void
    {
        if ($this->cancelled) {
            return;
        }

        if ($this->columnNames === []) {
            $this->columnNames = array_keys($row);
        }

        // Resolve the outer promise on the first row so the caller's await()
        // unblocks. Subsequent pushes are no-ops for the outer promise.
        $this->fireOnReady();

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

        // Resolve the outer promise even when the result set is empty, so the
        // caller's await() unblocks with the stream (not with an exception).
        $this->fireOnReady();

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

        // Reject the outer promise so the caller's await() surfaces the error.
        $this->fireOnError($e);

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

    /**
     * Fires $onReady once and clears both outer callbacks so they cannot
     * trigger again on subsequent push()/complete() calls.
     */
    private function fireOnReady(): void
    {
        if ($this->onReady === null) {
            return;
        }

        $cb = $this->onReady;
        $this->onReady = null;
        $this->onError = null;
        $cb();
    }

    /**
     * Fires $onError once and clears both outer callbacks.
     */
    private function fireOnError(Throwable $e): void
    {
        if ($this->onError === null) {
            return;
        }

        $cb = $this->onError;
        $this->onReady = null;
        $this->onError = null;
        $cb($e);
    }
}
