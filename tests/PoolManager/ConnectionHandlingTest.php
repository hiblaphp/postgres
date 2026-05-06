<?php

declare(strict_types=1);

use Hibla\EventLoop\Loop;
use Hibla\Postgres\Exceptions\PoolException;
use Hibla\Postgres\Internals\Connection;
use Hibla\Postgres\Manager\PoolManager;
use Hibla\Postgres\ValueObjects\PgSqlConfig;

use function Hibla\async;
use function Hibla\await;
use function Hibla\delay;

describe('Basic Acquisition and Release', function (): void {

    it('acquires a ready connection from the pool', function (): void {
        $pool = makePool();

        $conn = await($pool->get());

        expect($conn)->toBeInstanceOf(Connection::class)
            ->and($conn->isReady())->toBeTrue()
        ;

        $pool->release($conn);
        $pool->close();
    });

    it('reuses the same connection after release', function (): void {
        $pool = makePool(maxSize: 1);

        $conn1 = await($pool->get());
        $pid1 = $conn1->getProcessId();
        $pool->release($conn1);

        $conn2 = await($pool->get());
        $pid2 = $conn2->getProcessId();

        expect($pid1)->toBe($pid2);

        $pool->release($conn2);
        $pool->close();
    });

    it('returns different connections when multiple are acquired concurrently', function (): void {
        $pool = makePool(maxSize: 2);

        $conn1 = await($pool->get());
        $conn2 = await($pool->get());

        expect($conn1->getProcessId())->not->toBe($conn2->getProcessId());

        $pool->release($conn1);
        $pool->release($conn2);
        $pool->close();
    });

    it('executes queries on borrowed connections', function (): void {
        $pool = makePool();

        $conn = await($pool->get());
        $result = await($conn->query('SELECT 42 AS val'));

        expect((int) $result->fetchOne()['val'])->toBe(42);

        $pool->release($conn);
        $pool->close();
    });

    it('removes a closed connection on release instead of returning it to the pool', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $conn->close();

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue()
            ->and($conn2->getProcessId())->not->toBe($conn->getProcessId())
        ;

        $pool->release($conn2);
        $pool->close();
    });

    it('removes a non-ready connection on release', function (): void {
        $pool = makePool(maxSize: 1);
        $statsBefore = $pool->stats;

        $conn = await($pool->get());

        $conn->close();
        $pool->release($conn);

        $conn2 = await($pool->get());
        expect($conn2->isReady())->toBeTrue();

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Pool Size Enforcement', function (): void {

    it('enforces the maxSize limit by queuing waiters', function (): void {
        $pool = makePool(maxSize: 2);

        $conn1 = await($pool->get());
        $conn2 = await($pool->get());

        $waiter = $pool->get();
        expect($waiter->isPending())->toBeTrue();

        $pool->release($conn1);
        $conn3 = await($waiter);

        expect($conn3->isReady())->toBeTrue();

        $pool->release($conn2);
        $pool->release($conn3);
        $pool->close();
    });

    it('respects maxWaiters and rejects excess requests immediately', function (): void {
        $pool = makePool(maxSize: 1, maxWaiters: 1);

        $conn = await($pool->get());
        $waiter1 = $pool->get();

        $exception = null;

        try {
            await($pool->get());
        } catch (PoolException $e) {
            $exception = $e;
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->not->toBeNull();

        $pool->release($conn);
        $conn2 = await($waiter1);
        $pool->release($conn2);
        $pool->close();
    });

    it('rejects new borrows immediately during graceful shutdown', function (): void {
        $pool = makePool();

        $shutdownPromise = $pool->closeAsync();

        $exception = null;

        try {
            await($pool->get());
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->not->toBeNull();

        await($shutdownPromise);
    });
});

describe('Minimum Connections', function (): void {

    it('pre-warms the pool to minSize on construction', function (): void {
        $pool = makePool(maxSize: 5, minSize: 2);

        await(delay(0.1));

        $stats = $pool->stats;

        expect($stats['active_connections'])->toBeGreaterThanOrEqual(2);

        $pool->close();
    });

    it('replenishes to minSize after a connection is removed', function (): void {
        $pool = makePool(maxSize: 3, minSize: 1);

        await(delay(0.1));

        $conn = await($pool->get());
        $conn->close();
        $pool->release($conn);

        await(delay(0.1));

        expect($pool->stats['active_connections'])->toBeGreaterThanOrEqual(1);

        $pool->close();
    });
});

describe('Idle Timeout and Max Lifetime', function (): void {

    it('evicts idle connections that exceed idleTimeout on next borrow', function (): void {
        $pool = makePool(maxSize: 2, idleTimeout: 1);

        $conn = await($pool->get());
        $pid = $conn->getProcessId();
        $pool->release($conn);

        await(delay(1.5));

        $conn2 = await($pool->get());

        expect($conn2->getProcessId())->not->toBe($pid);

        $pool->release($conn2);
        $pool->close();
    });

    it('evicts connections that exceed maxLifetime on next borrow', function (): void {
        $pool = makePool(maxSize: 1, maxLifetime: 1);

        $conn = await($pool->get());
        $pid = $conn->getProcessId();
        $pool->release($conn);

        await(delay(1.5));

        $conn2 = await($pool->get());

        expect($conn2->getProcessId())->not->toBe($pid);

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Acquire Timeout', function (): void {

    it('rejects a waiter with TimeoutException when acquireTimeout is exceeded', function (): void {
        $pool = makePool(maxSize: 1, acquireTimeout: 0.3);

        $conn = await($pool->get());

        $exception = null;
        $start = microtime(true);

        try {
            await($pool->get());
        } catch (Hibla\Promise\Exceptions\TimeoutException $e) {
            $exception = $e;
        }

        expect($exception)->not->toBeNull()
            ->and(round(microtime(true) - $start, 1))->toBeGreaterThanOrEqual(0.3)
        ;

        $pool->release($conn);
        $pool->close();
    });

    it('cancels the acquire timeout timer when the waiter is satisfied before timeout', function (): void {
        $pool = makePool(maxSize: 1, acquireTimeout: 2.0);

        $conn = await($pool->get());

        $waiter = $pool->get();

        Loop::addTimer(0.1, fn () => $pool->release($conn));

        $conn2 = await($waiter);

        expect($conn2->isReady())->toBeTrue();

        $pool->release($conn2);
        $pool->close();
    });

    it('skips cancelled waiters and serves the next one in queue', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $waiter1 = $pool->get();
        $waiter2 = $pool->get();

        $waiter1->cancel();

        $pool->release($conn);

        $conn2 = await($waiter2);

        expect($waiter1->isCancelled())->toBeTrue()
            ->and($conn2->isReady())->toBeTrue()
        ;

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Concurrent Load', function (): void {

    it('handles N concurrent borrows and releases correctly', function (): void {
        $pool = makePool(maxSize: 5);
        $promises = [];
        $results = [];

        for ($i = 0; $i < 10; $i++) {
            $promises[] = async(function () use ($pool, &$results): void {
                $conn = await($pool->get());
                $result = await($conn->query('SELECT pg_backend_pid() AS pid'));
                $results[] = (int) $result->fetchOne()['pid'];
                $pool->release($conn);
            });
        }

        await(Hibla\Promise\Promise::all($promises));

        expect(\count($results))->toBe(10);

        $pool->close();
    });

    it('never exceeds maxSize active connections under concurrent load', function (): void {
        $pool = makePool(maxSize: 3);
        $maxObserved = 0;

        $promises = [];

        for ($i = 0; $i < 9; $i++) {
            $promises[] = async(function () use ($pool, &$maxObserved): void {
                $conn = await($pool->get());
                $active = $pool->stats['active_connections'];
                if ($active > $maxObserved) {
                    $maxObserved = $active;
                }
                await(delay(0.05));
                $pool->release($conn);
            });
        }

        await(Hibla\Promise\Promise::all($promises));

        expect($maxObserved)->toBeLessThanOrEqual(3);

        $pool->close();
    });
});

describe('Health Check', function (): void {

    it('reports all idle connections as healthy', function (): void {
        $pool = makePool(maxSize: 3);

        $conns = [await($pool->get()), await($pool->get())];
        foreach ($conns as $c) {
            $pool->release($c);
        }

        $stats = await($pool->healthCheck());

        expect($stats['healthy'])->toBe(2)
            ->and($stats['unhealthy'])->toBe(0)
            ->and($stats['total_checked'])->toBe(2)
        ;

        $pool->close();
    });

    it('reports a closed connection as unhealthy and removes it', function (): void {
        $pool = makePool(maxSize: 2);

        $conn = await($pool->get());
        $conn2 = await($pool->get());

        $pool->release($conn);
        $pool->release($conn2);

        $conn->close();

        $stats = await($pool->healthCheck());

        expect($stats['unhealthy'])->toBe(1)
            ->and($stats['healthy'])->toBe(1)
            ->and($stats['total_checked'])->toBe(2)
        ;

        $pool->close();
    });

    it('returns an empty stats array when the pool has no idle connections', function (): void {
        $pool = makePool(maxSize: 1);
        $conn = await($pool->get());

        $stats = await($pool->healthCheck());

        expect($stats['total_checked'])->toBe(0)
            ->and($stats['healthy'])->toBe(0)
            ->and($stats['unhealthy'])->toBe(0)
        ;

        $pool->release($conn);
        $pool->close();
    });
});

describe('onConnect Hook', function (): void {

    it('runs the onConnect hook exactly once per physical connection', function (): void {
        $callCount = 0;

        $pool = makePool(
            maxSize: 1,
            onConnect: function () use (&$callCount): void {
                $callCount++;
            }
        );

        $conn = await($pool->get());
        $pool->release($conn);

        $conn2 = await($pool->get());
        $pool->release($conn2);

        expect($callCount)->toBe(1);

        $pool->close();
    });

    it('drops the connection and rejects the waiter when the onConnect hook throws', function (): void {
        $pool = makePool(
            maxSize: 1,
            onConnect: function (): void {
                throw new RuntimeException('hook failure');
            }
        );

        $exception = null;

        try {
            await($pool->get());
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->not->toBeNull()
            ->and($pool->stats['active_connections'])->toBe(0)
        ;

        $pool->close();
    });

    it('supports async (promise-returning) onConnect hooks', function (): void {
        $hookRan = false;

        $pool = makePool(
            maxSize: 1,
            onConnect: function () use (&$hookRan): Hibla\Promise\Interfaces\PromiseInterface {
                return async(function () use (&$hookRan): void {
                    await(delay(0.05));
                    $hookRan = true;
                });
            }
        );

        $conn = await($pool->get());

        expect($hookRan)->toBeTrue();

        $pool->release($conn);
        $pool->close();
    });
});

describe('Connection Reset', function (): void {

    it('resets session state between borrows when resetConnection is enabled', function (): void {
        $pool = makePool(maxSize: 1, resetConnection: true);

        $conn = await($pool->get());
        await($conn->query("SET application_name = 'dirty_state'"));
        $pool->release($conn);

        $conn2 = await($pool->get());
        $result = await($conn2->query('SHOW application_name'));
        $appName = $result->fetchOne()['application_name'];

        expect($appName)->not->toBe('dirty_state');

        $pool->release($conn2);
        $pool->close();
    });

    it('reruns the onConnect hook after DISCARD ALL when both resetConnection and onConnect are set', function (): void {
        $hookCount = 0;

        $pool = makePool(
            maxSize: 1,
            resetConnection: true,
            onConnect: function () use (&$hookCount): void {
                $hookCount++;
            }
        );

        $conn = await($pool->get());
        $pool->release($conn);

        await(delay(0.1));

        expect($hookCount)->toBe(2);

        $pool->close();
    });
});

describe('Stats', function (): void {

    it('reflects correct counts during active borrows', function (): void {
        $pool = makePool(maxSize: 3);

        $conn1 = await($pool->get());
        $conn2 = await($pool->get());

        $stats = $pool->stats;

        expect($stats['active_connections'])->toBe(2)
            ->and($stats['pooled_connections'])->toBe(0)
            ->and($stats['max_size'])->toBe(3)
        ;

        $pool->release($conn1);
        $pool->release($conn2);
        $pool->close();
    });

    it('reflects pooled_connections after release', function (): void {
        $pool = makePool(maxSize: 2);

        $conn = await($pool->get());
        $pool->release($conn);

        expect($pool->stats['pooled_connections'])->toBe(1);

        $pool->close();
    });

    it('reflects waiting_requests when pool is at capacity', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $waiter = $pool->get();

        expect($pool->stats['waiting_requests'])->toBe(1);

        $pool->release($conn);
        await($waiter)->close();
        $pool->close();
    });
});

describe('Shutdown', function (): void {

    it('close() immediately releases all idle connections', function (): void {
        $pool = makePool(maxSize: 2);

        $conn1 = await($pool->get());
        $conn2 = await($pool->get());
        $pool->release($conn1);
        $pool->release($conn2);

        $pool->close();

        expect($pool->stats['active_connections'])->toBe(0)
            ->and($pool->stats['pooled_connections'])->toBe(0)
        ;
    });

    it('close() rejects all pending waiters', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $waiter = $pool->get();

        $exception = null;
        $waiter->then(null, function (Throwable $e) use (&$exception): void {
            $exception = $e;
        });

        $pool->close();

        await(delay(0));

        expect($exception)->not->toBeNull();
        $conn->close();
    });

    it('closeAsync() waits for active connections to finish before resolving', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $settled = false;

        $shutdown = $pool->closeAsync()->then(function () use (&$settled): void {
            $settled = true;
        });

        await(delay(0));
        expect($settled)->toBeFalse();

        $pool->release($conn);
        await($shutdown);

        expect($settled)->toBeTrue();
    });

    it('closeAsync() resolves immediately when pool is already idle', function (): void {
        $pool = makePool(maxSize: 1);
        $start = microtime(true);

        await($pool->closeAsync());

        expect(round(microtime(true) - $start, 2))->toBeLessThan(0.1);
    });

    it('closeAsync() falls back to force close when timeout expires', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());

        $start = microtime(true);
        await($pool->closeAsync(timeout: 0.3));

        expect(round(microtime(true) - $start, 1))->toBeGreaterThanOrEqual(0.3);

        expect($conn->isClosed())->toBeTrue();
    });

    it('close() while closeAsync() is pending resolves the shutdown promise immediately', function (): void {
        $pool = makePool(maxSize: 1);
        $conn = await($pool->get());
        $resolved = false;

        $shutdown = $pool->closeAsync()->then(function () use (&$resolved): void {
            $resolved = true;
        });

        $pool->close();

        await(delay(0));

        expect($resolved)->toBeTrue();
        $conn->close();
    });

    it('calling close() twice is a safe no-op', function (): void {
        $pool = makePool();
        $pool->close();

        $exception = null;

        try {
            $pool->close();
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->toBeNull();
    });

    it('calling closeAsync() twice returns the same in-progress shutdown promise', function (): void {
        $pool = makePool(maxSize: 1);
        $conn = await($pool->get());

        $p1 = $pool->closeAsync();
        $p2 = $pool->closeAsync();

        expect($p1)->toBe($p2);

        $pool->release($conn);
        await($p1);
    });
});

describe('PgSqlConfig Integration', function (): void {

    it('accepts a DSN string as config', function (): void {
        $dsn = sprintf(
            'postgresql://%s:%s@%s:%d/%s',
            $_ENV['POSTGRES_USERNAME'] ?? 'postgres',
            $_ENV['POSTGRES_PASSWORD'] ?? 'postgres',
            $_ENV['POSTGRES_HOST'] ?? '127.0.0.1',
            (int) ($_ENV['POSTGRES_PORT'] ?? 5443),
            $_ENV['POSTGRES_DATABASE'] ?? 'postgres',
        );

        $pool = new PoolManager(config: $dsn, maxSize: 1);
        $conn = await($pool->get());

        expect($conn->isReady())->toBeTrue();

        $pool->release($conn);
        $pool->close();
    });

    it('accepts a PgSqlConfig object directly', function (): void {
        $config = PgSqlConfig::fromArray(pgPoolConfig());
        $pool = new PoolManager(config: $config, maxSize: 1);
        $conn = await($pool->get());

        expect($conn->isReady())->toBeTrue();

        $pool->release($conn);
        $pool->close();
    });

    it('overrides enableServerSideCancellation from the config when passed explicitly', function (): void {
        $config = PgSqlConfig::fromArray(pgPoolConfig([
            'enable_server_side_cancellation' => false,
        ]));

        $pool = new PoolManager(
            config: $config,
            maxSize: 1,
            enableServerSideCancellation: true,
        );

        expect($pool->stats['query_cancellation_enabled'])->toBeTrue();

        $pool->close();
    });

    it('throws InvalidArgumentException for invalid constructor arguments', function (): void {
        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 0))
            ->toThrow(InvalidArgumentException::class)
        ;

        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 5, minSize: -1))
            ->toThrow(InvalidArgumentException::class)
        ;

        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 2, minSize: 5))
            ->toThrow(InvalidArgumentException::class)
        ;

        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 5, idleTimeout: 0))
            ->toThrow(InvalidArgumentException::class)
        ;

        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 5, maxLifetime: 0))
            ->toThrow(InvalidArgumentException::class)
        ;

        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 5, maxWaiters: -1))
            ->toThrow(InvalidArgumentException::class)
        ;

        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 5, acquireTimeout: -1.0))
            ->toThrow(InvalidArgumentException::class)
        ;
    });
});

describe('Waiter Queue Ordering', function (): void {

    it('serves waiters in strict FIFO order as connections are released', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $servedOrder = [];

        $waiter1 = $pool->get()->then(function ($c) use (&$servedOrder, $pool): void {
            $servedOrder[] = 1;
            $pool->release($c);
        });
        $waiter2 = $pool->get()->then(function ($c) use (&$servedOrder, $pool): void {
            $servedOrder[] = 2;
            $pool->release($c);
        });
        $waiter3 = $pool->get()->then(function ($c) use (&$servedOrder, $pool): void {
            $servedOrder[] = 3;
            $pool->release($c);
        });

        $pool->release($conn);

        await(Hibla\Promise\Promise::all([$waiter1, $waiter2, $waiter3]));

        expect($servedOrder)->toBe([1, 2, 3]);

        $pool->close();
    });
});

describe('Post-Close Behavior', function (): void {

    it('rejects get() immediately after force close()', function (): void {
        $pool = makePool();
        $pool->close();

        $exception = null;

        try {
            await($pool->get());
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->toBeInstanceOf(PoolException::class);
    });

    it('calling close() twice is safe and leaves stats at zero', function (): void {
        $pool = makePool(maxSize: 2);

        $conn = await($pool->get());
        $pool->release($conn);

        $pool->close();
        $pool->close();

        expect($pool->stats['active_connections'])->toBe(0)
            ->and($pool->stats['pooled_connections'])->toBe(0)
        ;
    });
});

describe('Max Lifetime Eviction on Release', function (): void {

    it('drops a connection that exceeded maxLifetime when parking it back in the idle pool', function (): void {
        $pool = makePool(maxSize: 1, maxLifetime: 1);

        $conn = await($pool->get());
        $pid = $conn->getProcessId();

        await(delay(1.5));

        $pool->release($conn);

        $conn2 = await($pool->get());

        expect($conn2->getProcessId())->not->toBe($pid);

        $pool->release($conn2);
        $pool->close();
    });
});

describe('Min Size Edge Cases', function (): void {

    it('pre-fills the pool completely when minSize equals maxSize', function (): void {
        $pool = makePool(maxSize: 3, minSize: 3);

        await(delay(0.1));

        expect($pool->stats['active_connections'])->toBe(3);

        $pool->close();
    });

    it('does not spawn replacement connections during graceful shutdown', function (): void {
        $pool = makePool(maxSize: 2, minSize: 2);

        await(delay(0.1));

        $conn = await($pool->get());

        $shutdown = $pool->closeAsync();

        $pool->release($conn);

        await($shutdown);

        expect($pool->stats['active_connections'])->toBe(0);
    });
});

describe('Stats Flags', function (): void {

    it('reports on_connect_hook as true when a hook is registered', function (): void {
        $pool = makePool(maxSize: 1, onConnect: static function (): void {
        });

        expect($pool->stats['on_connect_hook'])->toBeTrue();

        $pool->close();
    });

    it('reports on_connect_hook as false when no hook is registered', function (): void {
        $pool = makePool(maxSize: 1);

        expect($pool->stats['on_connect_hook'])->toBeFalse();

        $pool->close();
    });

    it('reports reset_connection_enabled correctly', function (): void {
        $poolWithReset = makePool(maxSize: 1, resetConnection: true);
        $poolWithout = makePool(maxSize: 1, resetConnection: false);

        expect($poolWithReset->stats['reset_connection_enabled'])->toBeTrue()
            ->and($poolWithout->stats['reset_connection_enabled'])->toBeFalse()
        ;

        $poolWithReset->close();
        $poolWithout->close();
    });

    it('reports draining_connections while a reset is in progress', function (): void {
        $pool = makePool(maxSize: 1, resetConnection: true);

        $conn = await($pool->get());

        $pool->release($conn);

        expect($pool->stats['draining_connections'])->toBeGreaterThanOrEqual(1);

        await(delay(0.1));

        expect($pool->stats['draining_connections'])->toBe(0);

        $pool->close();
    });
});

describe('Connection Reset Edge Cases', function (): void {

    it('rolls back an open transaction before issuing DISCARD ALL on reset', function (): void {
        $pool = makePool(maxSize: 1, resetConnection: true);

        $conn = await($pool->get());

        await($conn->query('BEGIN'));
        await($conn->query("SET LOCAL application_name = 'in_txn'"));

        $pool->release($conn);

        await(delay(0.1));

        $conn2 = await($pool->get());
        $result = await($conn2->query('SHOW application_name'));
        $appName = $result->fetchOne()['application_name'];

        expect($appName)->not->toBe('in_txn');

        $pool->release($conn2);
        $pool->close();
    });

    it('drops the connection and satisfies the next waiter when DISCARD ALL fails', function (): void {
        $pool = makePool(maxSize: 1, resetConnection: true);

        $conn = await($pool->get());

        $conn->close();
        $pool->release($conn);
        $conn2 = await($pool->get());

        expect($conn2->isReady())->toBeTrue();

        $pool->release($conn2);
        $pool->close();
    });
});

describe('onConnect Hook Edge Cases', function (): void {

    it('can execute queries inside the onConnect hook via ConnectionSetupInterface', function (): void {
        $pool = makePool(
            maxSize: 1,
            onConnect: function (Hibla\Postgres\Interfaces\ConnectionSetup $setup): Hibla\Promise\Interfaces\PromiseInterface {
                return async(function () use ($setup): void {
                    await($setup->query("SET application_name = 'hooked'"));
                });
            }
        );

        $conn = await($pool->get());
        $result = await($conn->query('SHOW application_name'));

        expect($result->fetchOne()['application_name'])->toBe('hooked');

        $pool->release($conn);
        $pool->close();
    });

    it('does not rerun the onConnect hook on release when resetConnection is disabled', function (): void {
        $callCount = 0;

        $pool = makePool(
            maxSize: 1,
            resetConnection: false,
            onConnect: function () use (&$callCount): void {
                $callCount++;
            }
        );

        $conn = await($pool->get());
        $pool->release($conn);

        await(delay(0.05));

        expect($callCount)->toBe(1);

        $pool->close();
    });

    it('drops the connection when the onConnect hook rejects during pool warm-up', function (): void {
        $pool = makePool(
            maxSize: 2,
            minSize: 2,
            onConnect: function (): void {
                throw new RuntimeException('warm-up hook failure');
            }
        );

        await(delay(0.1));

        expect($pool->stats['active_connections'])->toBe(0);

        $pool->close();
    });
});

describe('Graceful Shutdown Edge Cases', function (): void {

    it('rejects all pending waiters immediately when closeAsync() is called', function (): void {
        $pool = makePool(maxSize: 1);

        $conn = await($pool->get());
        $waiter = $pool->get();

        $exception = null;
        $waiter->then(null, function (Throwable $e) use (&$exception): void {
            $exception = $e;
        });

        $shutdown = $pool->closeAsync();

        await(delay(0));

        expect($exception)->toBeInstanceOf(PoolException::class);

        $pool->release($conn);
        await($shutdown);
    });

    it('drops idle connections immediately when closeAsync() is called', function (): void {
        $pool = makePool(maxSize: 2);

        $conn1 = await($pool->get());
        $conn2 = await($pool->get());
        $pool->release($conn1);
        $pool->release($conn2);

        $shutdown = $pool->closeAsync();

        expect($pool->stats['pooled_connections'])->toBe(0);

        await($shutdown);
    });

    it('health check during graceful shutdown destroys connections instead of re-pooling them', function (): void {
        $pool = makePool(maxSize: 2);

        $conn1 = await($pool->get());
        $conn2 = await($pool->get());
        $pool->release($conn1);
        $pool->release($conn2);

        $pool->closeAsync();
        $stats = await($pool->healthCheck());

        expect($stats['total_checked'])->toBe(0);
    });
});

describe('Invalid Constructor Arguments', function (): void {

    it('throws when minSize exceeds maxSize by exactly one', function (): void {
        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 3, minSize: 4))
            ->toThrow(InvalidArgumentException::class)
        ;
    });

    it('throws when acquireTimeout is negative', function (): void {
        expect(fn () => new PoolManager(pgPoolConfig(), maxSize: 5, acquireTimeout: -0.001))
            ->toThrow(InvalidArgumentException::class)
        ;
    });

    it('accepts acquireTimeout of exactly 0.0 (unlimited wait)', function (): void {
        $pool = null;
        $exception = null;

        try {
            $pool = new PoolManager(pgPoolConfig(), maxSize: 1, acquireTimeout: 0.0);
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->toBeNull();

        $pool?->close();
    });

    it('accepts minSize of exactly 0', function (): void {
        $pool = null;
        $exception = null;

        try {
            $pool = new PoolManager(pgPoolConfig(), maxSize: 3, minSize: 0);
        } catch (Throwable $e) {
            $exception = $e;
        }

        expect($exception)->toBeNull();

        $pool?->close();
    });
});
