<?php

namespace Hibla\Postgres;

use Hibla\Async\Timer;
use PgSql\Connection;
use PgSql\Result;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use Throwable;

use function Hibla\async;
use function Hibla\await;

final class AsyncPostgreSQL
{
    private static ?PoolManager $pool = null;
    private static bool $isInitialized = false;

    public static function init(array $dbConfig, int $poolSize = 10): void
    {
        if (self::$isInitialized) {
            return;
        }

        self::$pool = new PoolManager($dbConfig, $poolSize);
        self::$isInitialized = true;
    }

    public static function reset(): void
    {
        if (self::$pool) {
            self::$pool->close();
        }
        self::$pool = null;
        self::$isInitialized = false;
    }

    public static function run(callable $callback): PromiseInterface
    {
        return async(function () use ($callback) {
            $connection = null;

            try {
                $connection = await(self::getPool()->get());

                return $callback($connection);
            } finally {
                if ($connection) {
                    self::getPool()->release($connection);
                }
            }
        });
    }

    public static function query(string $sql, array $params = []): PromiseInterface
    {
        return self::executeAsyncQuery($sql, $params, 'fetchAll');
    }

    public static function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return self::executeAsyncQuery($sql, $params, 'fetchOne');
    }

    public static function execute(string $sql, array $params = []): PromiseInterface
    {
        return self::executeAsyncQuery($sql, $params, 'execute');
    }

    public static function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return self::executeAsyncQuery($sql, $params, 'fetchValue');
    }

    public static function transaction(callable $callback): PromiseInterface
    {
        return self::run(function (Connection $connection) use ($callback) {
            pg_query($connection, 'BEGIN');

            try {
                $result = $callback($connection);
                pg_query($connection, 'COMMIT');

                return $result;
            } catch (Throwable $e) {
                pg_query($connection, 'ROLLBACK');

                throw $e;
            }
        });
    }

    private static function executeAsyncQuery(string $sql, array $params, string $resultType): PromiseInterface
    {
        return async(function () use ($sql, $params, $resultType) {
            $connection = await(self::getPool()->get());

            try {
                if (! empty($params)) {
                    if (! pg_send_query_params($connection, $sql, $params)) {
                        throw new \RuntimeException('Failed to send parameterized query: '.pg_last_error($connection));
                    }
                } else {
                    if (! pg_send_query($connection, $sql)) {
                        throw new \RuntimeException('Failed to send query: '.pg_last_error($connection));
                    }
                }

                $result = await(self::waitForAsyncCompletion($connection));

                return self::processResult($result, $resultType, $connection);
            } finally {
                self::getPool()->release($connection);
            }
        });
    }

    private static function waitForAsyncCompletion(Connection $connection): PromiseInterface
    {
        return async(function () use ($connection) {
            $pollInterval = 100; // microseconds
            $maxInterval = 1000;

            while (pg_connection_busy($connection)) {
                await(Timer::delay($pollInterval / 1000000)); 
                $pollInterval = min($pollInterval * 1.2, $maxInterval);
            }

            return pg_get_result($connection);
        });
    }

    private static function processResult(Result|false $result, string $resultType, Connection $connection): mixed
    {
        if ($result === false) {
            $error = pg_last_error($connection);

            throw new \RuntimeException('Query execution failed: '.($error ?: 'Unknown error'));
        }

        return match ($resultType) {
            'fetchAll' => self::handleFetchAll($result),
            'fetchOne' => self::handleFetchOne($result),
            'fetchValue' => self::handleFetchValue($result),
            'execute' => self::handleExecute($result),
            default => $result,
        };
    }

    private static function handleFetchAll(Result $result): array
    {
        return pg_fetch_all($result) ?: [];
    }

    private static function handleFetchOne(Result $result): ?array
    {
        return pg_fetch_assoc($result) ?: null;
    }

    private static function handleFetchValue(Result $result): mixed
    {
        $row = pg_fetch_row($result);

        return $row ? $row[0] : null;
    }

    private static function handleExecute(Result $result): int
    {
        return pg_affected_rows($result);
    }

    public static function getPool(): PoolManager
    {
        if (! self::$isInitialized) {
            throw new \RuntimeException(
                'AsyncPostgreSQL has not been initialized. Please call AsyncPostgreSQL::init() at application startup.'
            );
        }

        return self::$pool;
    }
}
