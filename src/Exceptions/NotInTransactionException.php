<?php

namespace Hibla\Postgres\Exceptions;

/**
 * Thrown when transaction-specific operations are attempted outside a transaction.
 *
 * This exception occurs when methods like onCommit() or onRollback() are called
 * when no active transaction exists in the current fiber context.
 */
class NotInTransactionException extends TransactionException
{
}
