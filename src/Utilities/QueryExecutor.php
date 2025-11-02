<?php

declare(strict_types=1);

namespace Hibla\Postgres\Utilities;

use function Hibla\async;

use Hibla\Async\Timer;

use function Hibla\await;

use Hibla\Postgres\Exceptions\QueryException;
use Hibla\Promise\Interfaces\PromiseInterface;

use PgSql\Connection;
use PgSql\Result;

/**
 * Handles asynchronous query execution and result processing.
 *
 * This class manages the complete lifecycle of PostgreSQL query execution including
 * query sending, async completion waiting, and result processing.
 */
final class QueryExecutor
{
    /**
     * Executes an async query with the specified result processing type.
     *
     * This method handles the complete lifecycle of query execution including
     * query sending, result waiting, and result processing.
     * Automatically converts ? placeholders to PostgreSQL $n format if needed.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @param  string  $sql  SQL query/statement
     * @param  array<int, mixed>  $params  Query parameters
     * @param  string  $resultType  Type of result processing ('fetchAll', 'fetchOne', 'execute', 'fetchValue')
     * @return PromiseInterface<mixed> Promise resolving to processed result
     *
     * @throws QueryException If query execution fails
     */
    public function executeQuery(
        Connection $connection,
        string $sql,
        array $params,
        string $resultType
    ): PromiseInterface {
        return async(function () use ($connection, $sql, $params, $resultType) {
            $convertedSql = $this->convertPlaceholders($sql);
            $indexedParams = array_values($params);

            $this->sendQuery($connection, $convertedSql, $indexedParams);
            $result = await($this->waitForAsyncCompletion($connection));

            return $this->processResult($result, $resultType, $connection, $convertedSql, $indexedParams);
        });
    }

    /**
     * Converts ? placeholders to PostgreSQL $1, $2, $3 format.
     * If the SQL already uses $n placeholders, it is returned unchanged.
     * Skips question marks inside string literals (single quotes).
     *
     * @param  string  $sql  SQL with ? placeholders or $n placeholders
     * @return string SQL with $n placeholders
     *
     * @throws QueryException If mixing ? and $n placeholders
     */
    private function convertPlaceholders(string $sql): string
    {
        $hasDollarPlaceholders = preg_match('/\$\d+/', $sql) === 1;
        $hasQuestionPlaceholders = str_contains($sql, '?');

        if ($hasDollarPlaceholders && $hasQuestionPlaceholders) {
            throw new QueryException(
                'Cannot mix ? and $n placeholder formats in the same query',
                $sql,
                []
            );
        }

        if ($hasDollarPlaceholders || ! $hasQuestionPlaceholders) {
            return $sql;
        }

        $count = 0;
        $inSingleQuote = false;
        $inDoubleQuote = false;
        $result = '';
        $length = strlen($sql);

        for ($i = 0; $i < $length; $i++) {
            $char = $sql[$i];

            if ($char === "'" && ! $inDoubleQuote) {
                if ($i + 1 < $length && $sql[$i + 1] === "'") {
                    $result .= "''";
                    $i++;

                    continue;
                }
                $inSingleQuote = ! $inSingleQuote;
                $result .= $char;

                continue;
            }

            if ($char === '"' && ! $inSingleQuote) {
                if ($i + 1 < $length && $sql[$i + 1] === '"') {
                    $result .= '""';
                    $i++;

                    continue;
                }
                $inDoubleQuote = ! $inDoubleQuote;
                $result .= $char;

                continue;
            }

            if ($char === '?' && ! $inSingleQuote && ! $inDoubleQuote) {
                $prevChar = $i > 0 ? $sql[$i - 1] : ' ';
                $validPrecedingChars = [' ', ',', '(', '=', '>', '<', '!', "\n", "\r", "\t"];

                if (! in_array($prevChar, $validPrecedingChars, true)) {
                    throw new QueryException(
                        "Invalid placeholder position: '?' must be preceded by whitespace or an operator. " .
                            "Found '?' after '{$prevChar}' at position {$i}. " .
                            "If you need a literal '?' character, wrap it in quotes.",
                        $sql,
                        []
                    );
                }

                $count++;
                $result .= '$' . $count;

                continue;
            }

            $result .= $char;
        }

        return $result;
    }

    /**
     * Sends the query to the PostgreSQL connection.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @param  string  $sql  SQL query with $n placeholders
     * @param  array<int, mixed>  $params  Query parameters
     * @return void
     *
     * @throws QueryException If query send fails
     */
    private function sendQuery(Connection $connection, string $sql, array $params): void
    {
        if (count($params) > 0) {
            $sendResult = @pg_send_query_params($connection, $sql, $params);
            if ($sendResult === false) {
                throw new QueryException(
                    'Failed to send parameterized query: ' . pg_last_error($connection),
                    $sql,
                    $params
                );
            }
        } else {
            $sendResult = @pg_send_query($connection, $sql);
            if ($sendResult === false) {
                throw new QueryException(
                    'Failed to send query: ' . pg_last_error($connection),
                    $sql,
                    $params
                );
            }
        }
    }

    /**
     * Waits for an async query to complete using non-blocking polling.
     *
     * This method polls the connection status at increasing intervals
     * (exponential backoff) until the query completes. This provides
     * efficient non-blocking behavior without busy-waiting.
     *
     * @param  Connection  $connection  PostgreSQL connection
     * @return PromiseInterface<Result|false> Promise resolving to query result
     */
    private function waitForAsyncCompletion(Connection $connection): PromiseInterface
    {
        return async(function () use ($connection) {
            $pollInterval = 100; // microseconds
            $maxInterval = 1000;

            while (pg_connection_busy($connection)) {
                await(Timer::delay($pollInterval / 1000000));
                $pollInterval = (int) min($pollInterval * 1.2, $maxInterval);
            }

            return pg_get_result($connection);
        });
    }

    /**
     * Processes a query result based on the specified result type.
     *
     * This method converts the raw PostgreSQL result into the appropriate
     * PHP data structure based on the requested result type.
     *
     * @param  Result|false  $result  PostgreSQL query result
     * @param  string  $resultType  Type of result processing
     * @param  Connection  $connection  PostgreSQL connection for error reporting
     * @param  string  $sql  The SQL query for error context
     * @param  array<int, mixed>  $params  The query parameters for error context
     * @return mixed Processed result based on result type
     *
     * @throws QueryException If result is false or processing fails
     */
    private function processResult(
        Result|false $result,
        string $resultType,
        Connection $connection,
        string $sql,
        array $params
    ): mixed {
        if ($result === false) {
            $error = pg_last_error($connection);

            throw new QueryException(
                'Query execution failed: ' . ($error !== '' ? $error : 'Unknown error'),
                $sql,
                $params
            );
        }

        $resultStatus = pg_result_status($result, PGSQL_STATUS_LONG);
        if (
            $resultStatus === PGSQL_BAD_RESPONSE ||
            $resultStatus === PGSQL_NONFATAL_ERROR ||
            $resultStatus === PGSQL_FATAL_ERROR
        ) {
            $error = pg_result_error($result);

            throw new QueryException(
                'Query execution failed: ' . ($error !== '' ? $error : 'Unknown error'),
                $sql,
                $params
            );
        }

        return match ($resultType) {
            'fetchAll' => $this->handleFetchAll($result),
            'fetchOne' => $this->handleFetchOne($result),
            'fetchValue' => $this->handleFetchValue($result),
            'execute' => $this->handleExecute($result),
            default => $result,
        };
    }

    /**
     * Fetches all rows from a query result.
     *
     * Converts the PostgreSQL result into an array of associative arrays,
     * where each array represents a row with column names as keys.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return array<int, array<string, mixed>> Array of associative arrays
     */
    private function handleFetchAll(Result $result): array
    {
        $rows = pg_fetch_all($result);

        /** @var array<int, array<string, mixed>> $rows */
        return $rows;
    }

    /**
     * Fetches the first row from a query result.
     *
     * Converts the first row of the PostgreSQL result into an associative array
     * with column names as keys. Returns null if no rows exist.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return array<int|string, string|null>|null Associative array or null if no rows
     */
    private function handleFetchOne(Result $result): ?array
    {
        $row = pg_fetch_assoc($result);

        if ($row === false) {
            return null;
        }

        return $row;
    }

    /**
     * Fetches a single column value from the first row.
     *
     * Extracts the first column of the first row from the result set.
     * Useful for aggregate queries like COUNT, SUM, MAX, etc.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return mixed Scalar value or null if no rows
     */
    private function handleFetchValue(Result $result): mixed
    {
        $row = pg_fetch_row($result);

        if ($row === false) {
            return null;
        }

        return $row[0];
    }

    /**
     * Gets the number of affected rows from a query result.
     *
     * Returns the count of rows affected by an INSERT, UPDATE, or DELETE statement.
     *
     * @param  Result  $result  PostgreSQL query result
     * @return int Number of affected rows
     */
    private function handleExecute(Result $result): int
    {
        return pg_affected_rows($result);
    }
}
