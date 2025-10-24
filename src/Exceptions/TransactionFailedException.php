<?php

namespace Hibla\Postgres\Exception;

/**
 * Thrown when a transaction fails after all retry attempts.
 *
 * This exception is thrown when a transaction cannot be completed
 * successfully even after the specified number of retry attempts.
 */
class TransactionFailedException extends TransactionException
{
    /** @var int Number of attempts made before failure */
    private int $attempts;

    /**
     * Creates a new TransactionFailedException.
     *
     * @param  string  $message  Error message describing the failure
     * @param  int  $attempts  Number of attempts made before failure
     * @param  \Throwable|null  $previous  Previous exception for chaining
     */
    public function __construct(string $message, int $attempts, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
        $this->attempts = $attempts;
    }

    /**
     * Gets the number of attempts made before the transaction failed.
     *
     * @return int Number of attempts
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }
}