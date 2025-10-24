<?php

namespace Hibla\Postgres\Exceptions;

/**
 * Thrown when a database query execution fails.
 *
 * This exception provides context about the failed query including
 * the SQL statement and parameters that were used.
 */
class QueryException extends PgSQLException
{
    /** @var string The SQL query that failed */
    private string $sql;

    /** @var array<int, mixed> The parameters used in the query */
    private array $params;

    /**
     * Creates a new QueryException.
     *
     * @param  string  $message  Error message describing the failure
     * @param  string  $sql  The SQL query that failed
     * @param  array<int, mixed>  $params  The parameters used in the query
     * @param  \Throwable|null  $previous  Previous exception for chaining
     */
    public function __construct(string $message, string $sql, array $params = [], ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
        $this->sql = $sql;
        $this->params = $params;
    }

    /**
     * Gets the SQL query that failed.
     *
     * @return string The SQL query
     */
    public function getSql(): string
    {
        return $this->sql;
    }

    /**
     * Gets the parameters that were used in the failed query.
     *
     * @return array<int, mixed> The query parameters
     */
    public function getParams(): array
    {
        return $this->params;
    }
}
