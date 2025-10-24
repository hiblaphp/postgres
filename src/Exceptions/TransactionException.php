<?php

namespace Hibla\Postgres\Exceptions;

/**
 * Base exception for transaction-related errors.
 *
 * This exception is thrown when transaction operations fail, including
 * begin, commit, and rollback operations, as well as callback execution.
 */
class TransactionException extends PgSQLException
{
}
