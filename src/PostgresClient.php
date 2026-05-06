<?php

declare(strict_types=1);

namespace Hibla\Postgres;

use Hibla\Cache\ArrayCache;
use Hibla\Postgres\Exceptions\ConfigurationException;
use Hibla\Postgres\Exceptions\NotInitializedException;
use Hibla\Postgres\Interfaces\PostgresResult;
use Hibla\Postgres\Interfaces\PostgresRowStream;
use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\Internals\ManagedPreparedStatement;
use Hibla\Postgres\Internals\PreparedStatement;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Postgres\Traits\CancellationHelperTrait;
use Hibla\Postgres\ValueObjects\PgSqlConfig;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use Hibla\Sql\IsolationLevelInterface;
use Hibla\Sql\Result as ResultInterface;
use Hibla\Sql\SqlClientInterface;
use Hibla\Sql\TransactionOptions;

/**
 * Instance-based Asynchronous PostgreSQL Client with Connection Pooling.
 *
 * This class provides a high-level API for managing PostgreSQL database connections.
 * Each instance is completely independent, allowing true multi-database support
 * without global state.
 */
final class PostgresClient implements SqlClientInterface
{
    use CancellationHelperTrait;

    private ?PoolManager $pool = null;

    /**
     * @var \WeakMap<Connection, ArrayCache>|null
     */
    private ?\WeakMap $statementCaches = null;

    private int $statementCacheSize;

    private bool $enableStatementCache;

    private bool $resetConnectionEnabled = false;

    private bool $isClosing = false;

    /**
     * @var PromiseInterface<void>|null
     */
    private ?PromiseInterface $closePromise = null;

    /**
     * Creates a new independent PostgresClient instance.
     *
     * @param PgSqlConfig|array<string, mixed>|string $config Database configuration.
     * @param int $minConnections Minimum number of connections to keep open.
     * @param int $maxConnections Maximum number of connections in the pool.
     * @param int $idleTimeout Seconds a connection can remain idle before being closed.
     * @param int $maxLifetime Maximum seconds a connection can live before being rotated.
     * @param int $statementCacheSize Maximum number of prepared statements to cache per connection.
     * @param bool $enableStatementCache Whether to enable prepared statement caching. Defaults to true.
     * @param int $maxWaiters Maximum number of requests that can wait for a connection
     *                        before throwing a PoolException. 0 means unlimited. Defaults to 0.
     * @param float $acquireTimeout Maximum seconds to wait for a connection from the pool.
     * @param bool|null $enableServerSideCancellation Explicit override for the cancellation strategy.
     * @param bool|null $resetConnection Explicit override for connection resetting behavior.
     * @param callable|null $onConnect Optional hook invoked on new connections.
     *
     * @throws ConfigurationException If configuration is invalid.
     */
    public function __construct(
        PgSqlConfig|array|string $config,
        int $minConnections = 0,
        int $maxConnections = 10,
        int $idleTimeout = 60,
        int $maxLifetime = 3600,
        int $statementCacheSize = 256,
        bool $enableStatementCache = true,
        int $maxWaiters = 0,
        float $acquireTimeout = 10.0,
        ?bool $enableServerSideCancellation = null,
        ?bool $resetConnection = null,
        ?callable $onConnect = null,
    ) {
        try {
            $params = match (true) {
                $config instanceof PgSqlConfig => $config,
                \is_array($config) => PgSqlConfig::fromArray($config),
                \is_string($config) => PgSqlConfig::fromUri($config),
            };

            $finalCancellation = $enableServerSideCancellation ?? $params->enableServerSideCancellation;
            $finalReset = $resetConnection ?? $params->resetConnection;

            if (
                $finalCancellation !== $params->enableServerSideCancellation
                || $finalReset !== $params->resetConnection
            ) {
                $params = new PgSqlConfig(
                    host: $params->host,
                    port: $params->port,
                    username: $params->username,
                    password: $params->password,
                    database: $params->database,
                    sslmode: $params->sslmode,
                    sslCa: $params->sslCa,
                    sslCert: $params->sslCert,
                    sslKey: $params->sslKey,
                    connectTimeout: $params->connectTimeout,
                    applicationName: $params->applicationName,
                    killTimeoutSeconds: $params->killTimeoutSeconds,
                    enableServerSideCancellation: $finalCancellation,
                    resetConnection: $finalReset,
                );
            }

            $this->pool = new PoolManager(
                config: $params,
                maxSize: $maxConnections,
                minSize: $minConnections,
                idleTimeout: $idleTimeout,
                maxLifetime: $maxLifetime,
                maxWaiters: $maxWaiters,
                acquireTimeout: $acquireTimeout,
                enableServerSideCancellation: null, // Defer to config
                onConnect: $onConnect,
            );

            $this->resetConnectionEnabled = $params->resetConnection;
            $this->statementCacheSize = $statementCacheSize;
            $this->enableStatementCache = $enableStatementCache;

            if ($this->enableStatementCache) {
                /** @var \WeakMap<Connection, ArrayCache> $map */
                $map = new \WeakMap();
                $this->statementCaches = $map;
            }
        } catch (\InvalidArgumentException $e) {
            throw new ConfigurationException(
                'Invalid database configuration: ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    public array $stats {
        get {
            $stats = $this->getPool()->stats;

            /** @var array<string, bool|int> $clientStats */
            $clientStats = [];

            foreach ($stats as $key => $val) {
                if (\is_string($key) && (\is_bool($val) || \is_int($val))) {
                    $clientStats[$key] = $val;
                }
            }

            $clientStats['statement_cache_enabled'] = $this->enableStatementCache;
            $clientStats['statement_cache_size'] = $this->statementCacheSize;

            return $clientStats;
        }
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<ManagedPreparedStatement>
     */
    public function prepare(string $sql): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;
        $innerPromise = null;

        $promise = $this->borrowConnection()
            ->then(function (Connection $conn) use ($sql, $pool, &$connection, &$innerPromise) {
                $connection = $conn;

                $innerPromise = $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($conn, $pool) {
                        return new ManagedPreparedStatement($stmt, $conn, $pool);
                    })
                ;

                return $innerPromise;
            })
            ->catch(function (\Throwable $e) use ($pool, &$connection) {
                if ($connection !== null) {
                    $pool->release($connection);
                }

                throw $e;
            })
        ;

        $this->bindInnerCancellation($promise, $innerPromise);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresResult>
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        $pool = $this->getPool();
        $connection = null;
        $innerPromise = null;

        $promise = $this->borrowConnection()
            ->then(function (Connection $conn) use ($sql, $params, &$connection, &$innerPromise) {
                $connection = $conn;

                if (\count($params) === 0) {
                    $innerPromise = $conn->query($sql);

                    return $innerPromise;
                }

                if ($this->enableStatementCache) {
                    $innerPromise = $this->getCachedStatement($conn, $sql)
                        ->then(function (PreparedStatement $stmt) use ($params) {
                            return $stmt->execute($params);
                        })
                    ;

                    return $innerPromise;
                }

                $innerPromise = $conn->prepare($sql)
                    ->then(function (PreparedStatement $stmt) use ($params) {
                        return $stmt->execute($params)
                            ->finally(function () use ($stmt): void {
                                $stmt->close();
                            })
                        ;
                    })
                ;

                return $innerPromise;
            })
            ->finally(function () use ($pool, &$connection): void {
                if ($connection !== null) {
                    $pool->release($connection);
                }
            })
        ;

        $this->bindInnerCancellation($promise, $innerPromise);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<int>
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->affectedRows)
        );
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<int>
     */
    public function executeGetId(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(function (ResultInterface $result) {
                    $row = $result->fetchOne();

                    if ($row !== null && \count($row) > 0) {
                        return (int) reset($row);
                    }

                    return $result->lastInsertId;
                })
        );
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<array<string, mixed>|null>
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(fn (ResultInterface $result) => $result->fetchOne())
        );
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<mixed>
     */
    public function fetchValue(string $sql, string|int|null $column = null, array $params = []): PromiseInterface
    {
        return $this->withCancellation(
            $this->query($sql, $params)
                ->then(function (ResultInterface $result) use ($column) {
                    $row = $result->fetchOne();

                    if ($row === null) {
                        return null;
                    }

                    if ($column === null) {
                        $value = reset($row);

                        return $value !== false ? $value : null;
                    }

                    return $row[$column] ?? null;
                })
        );
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<PostgresRowStream>
     */
    public function stream(string $sql, array $params = [], int $bufferSize = 100): PromiseInterface
    {
        $pool = $this->getPool();
        $innerPromise = null;

        $state = new class () {
            public ?Connection $connection = null;

            public bool $released = false;
        };

        $releaseOnce = function () use ($pool, $state): void {
            if ($state->released || $state->connection === null) {
                return;
            }
            $state->released = true;
            $pool->release($state->connection);
        };

        $promise = $this->borrowConnection()
            ->then(function (Connection $conn) use ($sql, $params, $bufferSize, $pool, $state, &$innerPromise) {
                $state->connection = $conn;

                if (\count($params) === 0) {
                    $innerPromise = $conn->streamQuery($sql, $bufferSize);
                } else {
                    $innerPromise = $this->getCachedStatement($conn, $sql)
                        ->then(function (PreparedStatement $stmt) use ($params, $bufferSize) {
                            return $stmt->executeStream($params, $bufferSize);
                        })
                    ;
                }

                $query = $innerPromise->then(
                    function (PostgresRowStream $stream) use ($conn, $pool, $state): PostgresRowStream {

                        if ($stream instanceof Internals\RowStream) {
                            $state->released = true;
                            $stream->waitForCommand()->finally(function () use ($pool, $conn): void {
                                $pool->release($conn);
                            });
                        } else {
                            $state->released = true;
                            $pool->release($conn);
                        }

                        return $stream;
                    },
                    function (\Throwable $e) use ($conn, $pool, $state): never {
                        if (! $state->released) {
                            $state->released = true;
                            $pool->release($conn);
                        }

                        throw $e;
                    }
                );

                $query->onCancel(static function () use (&$innerPromise): void {
                    if (! $innerPromise->isSettled()) {
                        $innerPromise->cancelChain();
                    }
                });

                return $query;
            })
            ->finally($releaseOnce)
        ;

        $this->bindInnerCancellation($promise, $innerPromise);

        return $this->withCancellation($promise);
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction(?IsolationLevelInterface $isolationLevel = null): PromiseInterface
    {
        return Promise::rejected(new \RuntimeException('Transactions are not yet implemented in PostgresClient.'));
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(callable $callback, ?TransactionOptions $options = null): PromiseInterface
    {
        return Promise::rejected(new \RuntimeException('Transactions are not yet implemented in PostgresClient.'));
    }

    /**
     * {@inheritdoc}
     *
     * @return PromiseInterface<array<string, int>>
     */
    public function healthCheck(): PromiseInterface
    {
        return $this->getPool()->healthCheck();
    }

    /**
     * Clears the prepared statement cache for all connections.
     */
    public function clearStatementCache(): void
    {
        if ($this->statementCaches !== null) {
            /** @var \WeakMap<Connection, ArrayCache> $map */
            $map = new \WeakMap();
            $this->statementCaches = $map;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function closeAsync(float $timeout = 0.0): PromiseInterface
    {
        if ($this->pool === null) {
            return Promise::resolved();
        }

        if ($this->closePromise !== null) {
            return $this->closePromise;
        }

        $pool = $this->pool;

        $this->closePromise = $pool->closeAsync($timeout)
            ->then(function (): void {
                if ($this->isClosing) {
                    return;
                }

                $this->pool = null;
                $this->statementCaches = null;
                $this->closePromise = null;
            })
        ;

        return $this->closePromise;
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        if ($this->pool === null) {
            return;
        }

        $this->isClosing = true;

        $this->pool->close();
        $this->pool = null;
        $this->statementCaches = null;
        $this->closePromise = null;

        $this->isClosing = false;
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Borrows a connection from the pool and handles cache invalidation.
     *
     * @return PromiseInterface<Connection>
     */
    private function borrowConnection(): PromiseInterface
    {
        $pool = $this->getPool();

        return $pool->get()->then(function (Connection $conn) {
            if ($this->resetConnectionEnabled && $this->statementCaches !== null) {
                $this->statementCaches->offsetUnset($conn);
            }

            return $conn;
        });
    }

    /**
     * Helper to retrieve or create the statement cache for a specific connection.
     *
     * @return ArrayCache|null
     */
    private function getCacheForConnection(Connection $conn): ?ArrayCache
    {
        if (! $this->enableStatementCache || $this->statementCaches === null) {
            return null;
        }

        if (! $this->statementCaches->offsetExists($conn)) {
            $cache = new ArrayCache($this->statementCacheSize, function (string $key, mixed $stmt) use ($conn) {
                if ($stmt instanceof PreparedStatement && ! $conn->isClosed()) {
                    $stmt->close()->catch(fn () => null);
                }
            });

            $this->statementCaches->offsetSet($conn, $cache);
        }

        return $this->statementCaches->offsetGet($conn);
    }

    /**
     * Gets a prepared statement from cache or prepares and caches a new one.
     *
     * @return PromiseInterface<PreparedStatement>
     */
    private function getCachedStatement(Connection $conn, string $sql): PromiseInterface
    {
        $cache = $this->getCacheForConnection($conn);

        if ($cache === null) {
            return $conn->prepare($sql);
        }

        /** @var PromiseInterface<mixed> $cachePromise */
        $cachePromise = $cache->get($sql);

        return $cachePromise->then(function (mixed $stmt) use ($conn, $sql, $cache) {
            if ($stmt instanceof PreparedStatement) {
                return Promise::resolved($stmt);
            }

            return $conn->prepare($sql)
                ->then(function (PreparedStatement $stmt) use ($sql, $cache) {
                    $cache->set($sql, $stmt);

                    return $stmt;
                })
            ;
        });
    }

    /**
     * Gets the connection pool instance.
     *
     * @throws NotInitializedException If the client has been closed.
     */
    private function getPool(): PoolManager
    {
        if ($this->pool === null) {
            throw new NotInitializedException(
                'PostgresClient instance has not been initialized or has been closed.'
            );
        }

        return $this->pool;
    }
}
