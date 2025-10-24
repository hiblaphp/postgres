<?php 

namespace Hibla\Postgres\Exceptions;

use RuntimeException;

class TransactionException extends RuntimeException
{
    public function __construct(string $message)
    {
        parent::__construct($message);
    }
}