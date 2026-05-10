<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Traits\CancellationHelperTrait;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\PreparedStatement as PreparedStatementInterface;
use Hibla\Sql\RowStream as SqlRowStream;

/**
 * A wrapper around PreparedStatement used strictly inside Transactions.
 *
 * This class automatically sends a close command to the server when the
 * statement is closed or goes out of scope (garbage collected), preventing
 * server-side memory leaks.
 *
 * Crucially, unlike ManagedPreparedStatement, this DOES NOT release the
 * underlying TCP connection back to the pool, as the Transaction still owns it.
 *
 * @internal
 */
class TransactionPreparedStatement implements PreparedStatementInterface
{
    use CancellationHelperTrait;

    private bool $isClosed = false;

    /**
     * @param PreparedStatementInterface $statement The underlying prepared statement.
     * @param Connection $connection The connection that owns this statement.
     * @param \Closure(): void|null $onStreamError Optional callback invoked when a stream returned by executeStream() is
     *                                             cancelled or errors out mid-iteration. Injected by Transaction::prepare()
     *                                             so the owning transaction can set its $failed flag for lifecycle events
     *                                             that happen entirely inside a foreach loop — after the outer borrow promise
     *                                             has already resolved — and are therefore invisible to the promise chain
     *                                             that Transaction::trackErrorState() monitors.
     */
    public function __construct(
        private readonly PreparedStatementInterface $statement,
        private readonly Connection $connection,
        private readonly ?\Closure $onStreamError = null,
    ) {
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresResult>
     */
    public function execute(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<PostgresResult> $promise */
        $promise = $this->statement->execute($params);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * When an $onStreamError callback was provided by the owning Transaction,
     * this method hooks into the resolved stream's command promise so that
     * $stream->cancel() inside a foreach loop — or a server-side error mid-stream
     * — taints the transaction by firing the callback.
     *
     * Both onCancel and .catch() are registered for the same reason as
     * Transaction::trackErrorState(): promise cancellation short-circuits .catch()
     * chains, so cancellation must be handled via a dedicated onCancel hook.
     *
     * The .catch() handler intentionally does not re-throw; it only needs the
     * side-effect of invoking $onStreamError. Suppressing the error here does not
     * hide it from the iterator — RowStream stores the error internally and throws
     * it from getIterator() on the next iteration.
     *
     * @return PromiseInterface<SqlRowStream>
     */
    public function executeStream(array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<SqlRowStream> $promise */
        $promise = $this->statement->executeStream($params);

        if ($this->onStreamError !== null) {
            $onStreamError = $this->onStreamError;

            $promise = $promise->then(
                function (SqlRowStream $stream) use ($onStreamError): SqlRowStream {
                    if ($stream instanceof RowStream) {
                        // Primary: fires whenever $stream->cancel() is called, regardless
                        // of commandPromise state. Necessary because executeStream() with
                        // small result sets resolves the command promise before iteration
                        // begins, making commandPromise-level hooks unreachable.
                        $stream->onCancel($onStreamError);

                        // Secondary: covers async server-side errors mid-stream.
                        $cmd = $stream->waitForCommand();

                        if (! $cmd->isSettled()) {
                            $cmd->onCancel($onStreamError);

                            $cmd->catch(static function () use ($onStreamError): void {
                                $onStreamError();
                            });
                        }
                    }

                    return $stream;
                }
            );
        }

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<void>
     */
    public function close(): PromiseInterface
    {
        if ($this->isClosed) {
            return Promise::resolved();
        }

        $this->isClosed = true;

        return $this->statement->close();
    }

    /**
     * Destructor ensures the server-side statement is closed when the object
     * goes out of scope.
     */
    public function __destruct()
    {
        if (! $this->isClosed && ! $this->connection->isClosed()) {
            $this->close();
        }
    }
}
