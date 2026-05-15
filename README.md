# Hibla PostgreSQL Client

**A modern, async-first, high-performance PostgreSQL client for PHP with robust connection pooling, prepared statements, streaming, transactions, and asynchronous Pub/Sub leveraging native pgsql extension**

[![Latest Release](https://img.shields.io/github/release/hiblaphp/postgres.svg?style=flat-square)](https://github.com/hiblaphp/postgres/releases)
[![Total Downloads](https://img.shields.io/packagist/dt/hiblaphp/postgres.svg?style=flat-square)](https://packagist.org/packages/hiblaphp/postgres)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](./LICENSE)

---

## Features

| Feature | Status | Notes |
|---|---|---|
| Lazy connection pooling | Supported | No TCP connections opened until the first query |
| Parameterized queries | Supported | Binary protocol via prepared statements, SQL-injection safe |
| Named parameters (`:name`) | Supported | Parsed into positional `$n` at the client, works with `query()`, `prepare()`, and all transaction methods |
| Positional `?` parameters | Supported | Converted to `$1, $2, ...` format automatically |
| `$n` parameters | Supported | Passed through as-is when already in PostgreSQL format |
| Prepared statements | Supported | Explicit lifecycle control with `prepare()` and `close()` |
| Statement caching | Supported | Per-connection LRU cache, eliminates repeated `PREPARE` round-trips |
| Streaming results | Supported | Row-by-row delivery with backpressure, supports large result sets |
| Chunked streaming | Supported | Native `pg_set_chunked_rows_size` when available (PHP 8.4+), automatic cursor fallback otherwise |
| Server-side cursors | Supported | Automatic fallback for streaming when chunked mode is unavailable |
| Transactions | Supported | High-level `transaction()` with auto commit/rollback and retry, low-level `beginTransaction()` |
| Savepoints | Supported | `savepoint()`, `rollbackTo()`, `releaseSavepoint()` |
| Pub/Sub (LISTEN/NOTIFY) | Supported | Dedicated unpooled listener connection with auto-reconnect and exponential backoff |
| `NOTIFY` | Supported | Send notifications via `notify()` |
| SSL/TLS | Supported | Full `sslmode` support including `verify-ca` and `verify-full` |
| Server-side query cancellation | Supported | Opt-in `pg_cancel_backend` via side-channel TCP connection |
| Type casting | Supported | OID-based casting for integers, floats, booleans, and typed arrays from prepared statements |
| Health checks | Supported | `healthCheck()` pings idle connections and evicts stale ones |
| Pool stats | Supported | `$client->stats` for live pool introspection |
| `hiblaphp/sql` contracts | Supported | Fully implements `SqlClientInterface`, drivers are swappable |
| Multi-result sets | Supported | Traverse via `nextResult()` |

---

## Contents

**Getting started**
- [Installation](#installation)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [hiblaphp/sql contracts](#hiblaphpsql-contracts)

**Configuration**
- [PgSqlConfig](#pgsqlconfig)
  - [Construction](#construction)
  - [Properties](#properties)
  - [Methods](#methods)
  - [Sharing a config across multiple clients](#sharing-a-config-across-multiple-clients)

**Core API**
- [The PostgresClient](#the-postgresclient)
- [Making queries](#making-queries)
  - [Simple queries](#simple-queries-text-protocol)
  - [Queries with parameters](#queries-with-parameters-binary-prepared-statements)
  - [Named parameters](#named-parameters)
  - [Convenience methods](#convenience-methods)
- [Prepared statements](#prepared-statements)
- [Streaming results](#streaming-results)
- [Transactions](#transactions)
  - [High-level API: `transaction()`](#high-level-api-transaction)
  - [Automatic retry](#automatic-retry)
  - [TransactionOptions reference](#transactionoptions-reference)
  - [Low-level API: `beginTransaction()`](#low-level-api-begintransaction)
  - [Tainted state](#tainted-state)
  - [Cancellation behaviour](#cancellation-behaviour)
  - [Savepoints](#savepoints)
  - [Transaction lifecycle rules](#transaction-lifecycle-rules)

**PostgreSQL-specific features**
- [Pub/Sub: LISTEN and NOTIFY](#pubsub-listen-and-notify)
  - [Sending notifications](#sending-notifications)
  - [Receiving notifications: PostgresListener](#receiving-notifications-postgreslistener)
  - [Auto-reconnect behaviour](#auto-reconnect-behaviour)

**Advanced features**
- [Connection pooling](#connection-pooling)
  - [Check-on-borrow health strategy](#check-on-borrow-health-strategy)
  - [Shutdown strategies](#shutdown-strategies)
  - [resetConnection and statement cache interaction](#resetconnection-and-statement-cache-interaction)
- [Health checks and pool stats](#health-checks-and-pool-stats)
- [SSL/TLS](#ssltls)
- [Query cancellation](#query-cancellation)
- [onConnect hook](#onconnect-hook)
- [Statement caching](#statement-caching)
- [Type casting](#type-casting)

**Working with responses**
- [Result inspection](#result-inspection)
- [Multiple result sets](#multiple-result-sets)

**Development**
- [Development](#development)

**Reference**
- [API Reference](#api-reference-summary)
- [Exceptions](#exceptions)

**Meta**
- [License](#license)

---

## Installation

> This package is currently in **beta**. Before installing, ensure your `composer.json` allows beta releases:

```json
{
    "minimum-stability": "beta",
    "prefer-stable": true
}
```

```bash
composer require hiblaphp/postgres
```

**Requirements:**
- PHP 8.4+
- The `pgsql` PHP extension (`ext-pgsql`)

---

## Quick start

```php
use Hibla\Postgres\PostgresClient;
use function Hibla\await;

// The client is lazy by default, so no connections are opened until the first query.
$client = new PostgresClient('postgresql://test_user:test_password@127.0.0.1/mydb');

// Simple query
$users = await($client->query('SELECT * FROM users WHERE active = $1', [true]));
echo $users->rowCount;

// Named parameters
$user = await(
    $client->query(
        'SELECT * FROM users WHERE email = :email AND status = :status',
        ['email' => 'alice@example.com', 'status' => 'active']
    )
);

// Positional ? placeholders (converted to $n automatically)
$orders = await(
    $client->query(
        'SELECT * FROM orders WHERE user_id = ? AND status = ?',
        [$userId, 'pending']
    )
);

// Prepared statement (recommended for repeated execution)
$stmt = await(
    $client->prepare('SELECT * FROM users WHERE email = :email')
);
$result = await($stmt->execute(['email' => 'alice@example.com']));
await($stmt->close());

// Streaming large result sets
$stream = await($client->stream('SELECT * FROM logs ORDER BY id DESC'));
foreach ($stream as $row) {
    processLog($row);
}

// Pub/Sub
await($client->notify('user.events', json_encode(['type' => 'login', 'id' => 42])));
```

---

## How it works

`PostgresClient` manages a **lazy connection pool** of asynchronous PostgreSQL connections. By default, `minConnections` is `0`, meaning no TCP connections are opened until the first query actually arrives. Resources are created on demand and returned to the pool for reuse. This makes the client cheap to instantiate and well-suited to environments where database activity is bursty or infrequent.

All operations return `PromiseInterface` objects. You can use `await()` for linear code or `.then()` chaining.

Parameterized queries use the **PostgreSQL binary protocol** (prepared statements), which is more efficient and SQL-injection safe. The library accepts positional `?`, named `:name`, and native `$n` placeholders. Named and positional parameters are resolved entirely on the client side before the query is sent, so they work regardless of the PostgreSQL version.

Streaming results support two modes. When `pg_set_chunked_rows_size` is available (PHP 8.4+), rows are delivered in chunks directly from the server buffer. When it is not available, the library falls back to **server-side cursors** (`DECLARE ... CURSOR FOR`) automatically. The fallback is transparent and requires no changes in your application code.

```php
use function Hibla\await;
use Hibla\Promise\Promise;

// Three queries run concurrently. Connections are borrowed from the pool
// (and created on demand) only as each query starts.
[$users, $orders, $stats] = await(
    Promise::all([
        $client->query('SELECT * FROM users'),
        $client->query('SELECT * FROM orders'),
        $client->query('SELECT COUNT(*) FROM stats'),
    ])
);
```

---

## hiblaphp/sql contracts

`PostgresClient` fully implements the [`hiblaphp/sql`](https://github.com/hiblaphp/sql) contract package, which defines the common interfaces shared across all Hibla database drivers:

| Interface | Implemented by |
|---|---|
| `SqlClientInterface` | `PostgresClient` |
| `PostgresClientInterface` | `PostgresClient` |
| `PreparedStatement` | `ManagedPreparedStatement`, `TransactionPreparedStatement` |
| `Transaction` | `Transaction` |
| `Result` | `Result` |
| `RowStream` | `RowStream` |

This means you can type-hint against `SqlClientInterface` or `TransactionInterface` in your application code and swap the underlying driver without changing any business logic:

```php
use Hibla\Sql\SqlClientInterface;

class UserRepository
{
    public function __construct(private readonly SqlClientInterface $db) {}
}
```

---

## `PgSqlConfig`

`PgSqlConfig` is the canonical, immutable connection-level configuration object. All three config formats accepted by `PostgresClient` (DSN string, associative array, and `PgSqlConfig` directly) are normalised to this type internally. You can construct it explicitly when you want to share a single config object across multiple clients, derive variants from a base config, or keep all settings in one strongly-typed place.

```php
use Hibla\Postgres\ValueObjects\PgSqlConfig;

$config = new PgSqlConfig(
    host: '127.0.0.1',
    port: 5432,
    username: 'app_user',
    password: 'secret',
    database: 'production',
    sslmode: 'verify-full',
    sslCa: '/etc/ssl/certs/ca-bundle.crt',
    enableServerSideCancellation: true,
    resetConnection: true,
    castPreparedTypes: true,
);

$client = new PostgresClient($config, maxConnections: 20);
```

Because `PgSqlConfig` is `readonly`, every property is immutable after construction. Use the factory methods or `with*()` methods to derive variants.

---

### Construction

Three construction paths are available depending on where your config comes from.

**Direct constructor**

All properties are named and have defaults except `host`. Pass only what you need:

```php
$config = new PgSqlConfig(
    host: '127.0.0.1',
    username: 'app_user',
    password: 'secret',
    database: 'mydb',
);
```

**`PgSqlConfig::fromArray(array $config)`**

Accepts an associative array of options. Unknown keys are silently ignored. All keys are optional except `host`:

```php
$config = PgSqlConfig::fromArray([
    'host'                            => '127.0.0.1',
    'port'                            => 5432,
    'username'                        => 'app_user',
    'password'                        => 'secret',
    'database'                        => 'mydb',
    'sslmode'                         => 'prefer',
    'ssl_ca'                          => '/path/to/ca.pem',
    'ssl_cert'                        => '/path/to/client-cert.pem',
    'ssl_key'                         => '/path/to/client-key.pem',
    'connect_timeout'                 => 10,
    'application_name'                => 'my_app',
    'reset_connection'                => true,
    'enable_server_side_cancellation' => false,
    'kill_timeout_seconds'            => 3.0,
    'cast_prepared_types'             => true,
]);
```

**`PgSqlConfig::fromUri(string $uri)`**

Parses a PostgreSQL DSN string. The scheme must be `postgresql` or `postgres`. Most options are passed as query parameters:

```php
$config = PgSqlConfig::fromUri(
    'postgresql://app_user:secret@127.0.0.1:5432/mydb'
    . '?sslmode=verify-full'
    . '&ssl_ca=/path/to/ca.pem'
    . '&reset_connection=true'
    . '&enable_server_side_cancellation=true'
    . '&kill_timeout_seconds=5'
    . '&cast_prepared_types=true'
    . '&application_name=my_app'
);
```

If no scheme is present, `postgresql://` is prepended automatically. Characters in the username or password that are not URL-safe should be percent-encoded.

---

### Properties

| Property | Type | Default | Description |
|---|---|---|---|
| `host` | `string` | required | PostgreSQL server hostname or IP address. |
| `port` | `int` | `5432` | TCP port. |
| `username` | `string` | `'postgres'` | PostgreSQL username. |
| `password` | `string` | `''` | PostgreSQL password. |
| `database` | `string` | `''` | Default database to connect to. |
| `sslmode` | `string` | `'prefer'` | SSL mode. Accepted values: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`. |
| `sslCa` | `string\|null` | `null` | Path to the SSL CA certificate file (`sslrootcert`). Used when `sslmode` is `verify-ca` or `verify-full`. |
| `sslCert` | `string\|null` | `null` | Path to the SSL client certificate file (`sslcert`). Used for mutual TLS. |
| `sslKey` | `string\|null` | `null` | Path to the SSL client key file (`sslkey`). Used for mutual TLS. |
| `connectTimeout` | `int` | `10` | Seconds before a connect attempt is aborted. |
| `applicationName` | `string` | `'hibla_pgsql'` | The `application_name` reported to PostgreSQL, visible in `pg_stat_activity`. |
| `killTimeoutSeconds` | `float` | `3.0` | Maximum seconds to wait for a `pg_cancel_backend` side-channel connection to complete. Must be greater than zero. Only relevant when `enableServerSideCancellation` is `true`. |
| `enableServerSideCancellation` | `bool` | `false` | Whether cancelling a query promise dispatches `pg_cancel_backend` to the server via a dedicated side-channel TCP connection. When `false`, cancellation only transitions the promise state. See [Query cancellation](#query-cancellation). |
| `resetConnection` | `bool` | `false` | Whether to send `DISCARD ALL` when a connection is returned to the pool. Clears all session state including variables, temporary tables, and prepared statement handles. The `onConnect` hook is re-invoked after every reset. |
| `castPreparedTypes` | `bool` | `true` | Whether the OID-based decoder casts column values to native PHP types when executing prepared statements. When `false`, all values are returned as strings. See [Type casting](#type-casting). |

---

### Methods

**`withQueryCancellation(bool $enabled): self`**

Returns a new `PgSqlConfig` with `enableServerSideCancellation` changed to `$enabled`. All other properties are copied unchanged:

```php
$base = PgSqlConfig::fromArray([
    'host'     => '127.0.0.1',
    'username' => 'app_user',
    'password' => 'secret',
    'database' => 'mydb',
]);

// Regular client, cancellation off (default)
$readClient = new PostgresClient($base);

// Long-running report client, cancellation on so queries can be interrupted
$reportClient = new PostgresClient(
    $base->withQueryCancellation(true),
    maxConnections: 2,
);
```

**`toConnectionString(): string`**

Returns the `libpq` connection string format required by `pg_connect()`. Useful for debugging, but note that the password is included in plaintext. Do not log this value directly.

---

### Sharing a config across multiple clients

Because `PgSqlConfig` is immutable, one instance can be safely shared and derived from across as many clients as you need:

```php
$base = new PgSqlConfig(
    host: 'db.internal',
    username: 'app',
    password: 'secret',
    sslmode: 'verify-full',
    sslCa: '/etc/ssl/ca.pem',
);

$userDb   = new PostgresClient(
    PgSqlConfig::fromArray([...(array) $base, 'database' => 'users']),
    maxConnections: 10
);
$reportDb = new PostgresClient(
    $base->withQueryCancellation(true),
    maxConnections: 2
);
```

> `PgSqlConfig` covers only what is negotiated at the TCP and PostgreSQL handshake level: credentials, SSL, and per-connection protocol behaviour. Pool-level settings such as `maxConnections`, `idleTimeout`, and `statementCacheSize` are constructor parameters on `PostgresClient`.

---

## The `PostgresClient`

```php
use Hibla\Postgres\PostgresClient;

// From DSN string, lazy, no connections opened yet
$client = new PostgresClient('postgresql://user:pass@localhost:5432/mydb');

// From array
$client = new PostgresClient([
    'host'     => '127.0.0.1',
    'port'     => 5432,
    'username' => 'test_user',
    'password' => 'test_password',
    'database' => 'test',
]);

// With explicit pool settings
$client = new PostgresClient(
    config: 'postgresql://...',
    minConnections: 0,
    maxConnections: 20,
    idleTimeout: 300,
    maxLifetime: 3600,
    statementCacheSize: 512,
    enableStatementCache: true,
    maxWaiters: 100,
    acquireTimeout: 10.0,
    enableServerSideCancellation: true,
    resetConnection: true,
    castPreparedTypes: true,
    onConnect: function (ConnectionSetup $setup) {
        await($setup->execute("SET SESSION TIME ZONE 'UTC'"));
    },
);
```

### Constructor parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `$config` | `PgSqlConfig\|array\|string` | required | Database configuration. Accepts a DSN string, an associative array of options, or a `PgSqlConfig` object. |
| `$minConnections` | `int` | `0` | Minimum number of connections to keep open. Set to a value greater than `0` only if you need pre-warmed connections at startup. |
| `$maxConnections` | `int` | `10` | Hard cap on the number of open connections in the pool. |
| `$idleTimeout` | `int` | `60` | Seconds a connection can remain idle in the pool before it is evicted and closed. |
| `$maxLifetime` | `int` | `3600` | Maximum seconds a connection may live before it is rotated out, regardless of whether it is idle or active. |
| `$statementCacheSize` | `int` | `256` | Maximum number of prepared statements to cache per connection (LRU eviction). |
| `$enableStatementCache` | `bool` | `true` | Whether to cache prepared statements per connection. When enabled, `query($sql, $params)` reuses existing server-side statement handles instead of issuing a new `PREPARE` on every call. |
| `$maxWaiters` | `int` | `0` | Maximum number of callers that may queue waiting for a free connection before a `PoolException` is thrown immediately. `0` means unlimited. |
| `$acquireTimeout` | `float` | `10.0` | Maximum seconds to wait for a free connection before throwing a `PoolException`. |
| `$enableServerSideCancellation` | `bool\|null` | `null` | Controls whether cancelling a query promise dispatches `pg_cancel_backend`. `null` defers to the value in `$config`. |
| `$resetConnection` | `bool\|null` | `null` | Controls whether `DISCARD ALL` is sent when a connection is returned to the pool. `null` defers to the value in `$config`. |
| `$castPreparedTypes` | `bool\|null` | `null` | Controls whether OID-based type casting is applied to prepared statement results. `null` defers to the value in `$config`. |
| `$onConnect` | `callable\|null` | `null` | Optional hook invoked on every new physical connection immediately after the PostgreSQL handshake completes. Receives a `ConnectionSetup` instance. |

---

## Making queries

### Simple queries (text protocol)

```php
$result = await($client->query('SELECT * FROM users LIMIT 10'));
```

### Queries with parameters (binary prepared statements)

When `$params` are provided, the library automatically uses a prepared statement over the binary protocol. Three placeholder formats are supported and can be chosen freely per query.

**Native PostgreSQL `$n` placeholders:**

```php
$result = await(
    $client->query(
        'SELECT id, name FROM users WHERE created_at > $1 AND status = $2',
        [$since, 'active']
    )
);
```

**Positional `?` placeholders (converted to `$n` automatically):**

```php
$result = await(
    $client->query(
        'SELECT id, name FROM users WHERE created_at > ? AND status = ?',
        [$since, 'active']
    )
);
```

**Named `:name` placeholders:**

```php
$result = await(
    $client->query(
        'SELECT id, name FROM users WHERE created_at > :since AND status = :status',
        ['since' => $since, 'status' => 'active']
    )
);
```

> You cannot mix placeholder formats within the same query. Attempting to do so throws a `QueryException`.

### Named parameters

Named placeholders are resolved entirely on the client side before the query reaches PostgreSQL, so there is no server-side dependency and they work identically across all PostgreSQL versions.

```php
// Named params with execute()
$result = await(
    $client->query(
        'INSERT INTO orders (user_id, total, status) VALUES (:userId, :total, :status)',
        ['status' => 'pending', 'total' => 99.99, 'userId' => 42] // any key order
    )
);

// Named params via prepare(), most useful when executing the same statement repeatedly
$stmt = await(
    $client->prepare(
        'SELECT * FROM products WHERE category_id = :categoryId AND price > :minPrice'
    )
);

$electronics = await($stmt->execute(['categoryId' => 1, 'minPrice' => 50.00]));
$clothing    = await($stmt->execute(['categoryId' => 2, 'minPrice' => 25.00]));

await($stmt->close());
```

**Rules for named parameters:**

- Named and positional `?` placeholders cannot be mixed in the same query.
- Parameter names must start with a letter (`a-z`, `A-Z`) or underscore (`_`) and may contain letters, digits, and underscores.
- The PostgreSQL `::` cast operator (e.g. `value::int`) is correctly recognised and is never mistaken for a named placeholder.
- The `:=` assignment operator used in PL/pgSQL is also passed through untouched.
- Dollar-quoted strings (`$$...$$` and `$tag$...$tag$`) are fully parsed, so placeholders inside them are never substituted.

### Convenience methods

```php
// Returns affected row count
$count = await(
    $client->execute(
        'UPDATE users SET last_login = NOW() WHERE id = :id',
        ['id' => $userId]
    )
);

// Returns the first column of the first row as an integer (designed for use with RETURNING id)
$newId = await(
    $client->executeGetId(
        'INSERT INTO users (name, email) VALUES (:name, :email) RETURNING id',
        ['name' => 'Alice', 'email' => 'alice@example.com']
    )
);

// Returns first row as associative array, or null
$user = await(
    $client->fetchOne(
        'SELECT * FROM users WHERE id = :id',
        ['id' => $userId]
    )
);

// Returns value of first column (or named column) from first row
$name = await(
    $client->fetchValue(
        'SELECT name FROM users WHERE id = :id',
        ['id' => $userId]
    )
);
```

---

## Prepared statements

Use explicit prepared statements when you need to execute the same query many times and want direct control over the statement lifecycle. Both positional and named placeholders are supported.

```php
// Positional placeholders (? converted to $n)
$stmt = await(
    $client->prepare('SELECT * FROM products WHERE category_id = ? AND price > ?')
);
$result1 = await($stmt->execute([1, 50.00]));
$result2 = await($stmt->execute([2, 100.00]));
await($stmt->close());

// Named placeholders, order of keys in execute() does not matter
$stmt = await(
    $client->prepare(
        'SELECT * FROM products WHERE category_id = :categoryId AND price > :minPrice'
    )
);
$result1 = await($stmt->execute(['categoryId' => 1, 'minPrice' => 50.00]));
$result2 = await($stmt->execute(['minPrice' => 100.00, 'categoryId' => 2]));
await($stmt->close());
```

`close()` sends `DEALLOCATE` to the server and releases the statement handle. It is called automatically on object destruction if omitted, but explicit calls are strongly recommended to free server-side memory promptly.

> `PostgresClient::query()` with parameters handles statement preparation and caching for you transparently. Explicit `prepare()` is intended for cases where you hold the statement open yourself across many executions.

---

## Streaming results

Rows are yielded as they arrive from the server with **backpressure support**, so the socket is automatically paused when the internal buffer fills and resumed when it drains below half capacity.

The library chooses the most efficient streaming strategy automatically:

- **Chunked mode** (PHP 8.4+ with `pg_set_chunked_rows_size`): rows are delivered in chunks directly from the server's send buffer with no additional round-trips.
- **Server-side cursor** (fallback): the library issues `DECLARE ... NO SCROLL CURSOR FOR` and loops with `FETCH n FROM cursor`, managing `BEGIN`/`COMMIT` automatically when not already inside a transaction.

Both modes are completely transparent from the application's perspective.

```php
$stream = await(
    $client->stream('SELECT * FROM large_table ORDER BY id', bufferSize: 200)
);

// Inspect stream metadata before iterating
echo $stream->columnCount;
print_r($stream->columns);

foreach ($stream as $row) {
    processRow($row);
}
```

You can also stream prepared statement results:

```php
$stmt = await(
    $client->prepare('SELECT * FROM logs WHERE created_at > :since AND level = :level')
);

$stream = await(
    $stmt->executeStream(['since' => $since, 'level' => 'error'])
);

foreach ($stream as $row) {
    processRow($row);
}
```

To cancel a stream before it is fully consumed, call `$stream->cancel()`:

```php
$stream = await($client->stream('SELECT * FROM huge_table'));

foreach ($stream as $row) {
    if (shouldStop($row)) {
        $stream->cancel();
        break;
    }
    processRow($row);
}
```

> **Concurrent use:** If you are consuming a stream alongside other concurrent async work, wrap the `foreach` in `async()` to avoid blocking the event loop while waiting for the next buffer fill:
>
> ```php
> await(
>     async(function () use ($client) {
>         $stream = await($client->stream($sql));
>         foreach ($stream as $row) { ... }
>     })
> );
> ```

---

## Transactions

Transactions use `BEGIN`, which means **isolation levels are scoped strictly to the individual transaction**. They do not leak into the session or affect any other concurrent query on the same connection. Each transaction starts clean, and the connection is returned to the pool in its original session state when the transaction completes.

### High-level API: `transaction()`

The `transaction()` method is the recommended way to run a transaction. It handles `BEGIN`, commit, rollback, and automatic retry automatically so you only write the business logic.

**The callback is implicitly wrapped in a Fiber via `async()`.** This means `await()` is safe to call freely inside it without blocking the event loop. Concurrent async work, nested queries, and streaming all behave correctly inside the callback with no extra setup required.

```php
$result = await(
    $client->transaction(function (TransactionInterface $tx) use ($from, $to) {
        await(
            $tx->execute(
                'UPDATE accounts SET balance = balance - :amount WHERE id = :id',
                ['amount' => 100, 'id' => $from]
            )
        );
        await(
            $tx->execute(
                'UPDATE accounts SET balance = balance + :amount WHERE id = :id',
                ['amount' => 100, 'id' => $to]
            )
        );

        return 'Transfer completed';
    })
);
```

Partial failure is never silently committed. If any `await()` inside the callback throws, the client automatically rolls back the entire transaction and re-throws the exception.

---

### Automatic retry

`transaction()` automatically retries the entire callback on **deadlocks** (`DeadlockException`, SQLSTATE `40P01`) and **lock wait timeouts** (`LockWaitTimeoutException`, SQLSTATE `55P03`). These two exception types implement the `RetryableException` marker interface, which the retry engine recognises without any configuration.

The default `TransactionOptions` has `attempts: 1` (no retry). Pass `withAttempts()` to enable retry:

```php
use Hibla\Sql\TransactionOptions;
use Hibla\Postgres\Enums\IsolationLevel;

await(
    $client->transaction(
        function (TransactionInterface $tx) use ($from, $to) {
            await(
                $tx->execute(
                    'UPDATE accounts SET balance = balance - :amount WHERE id = :id',
                    ['amount' => 100, 'id' => $from]
                )
            );
            await(
                $tx->execute(
                    'UPDATE accounts SET balance = balance + :amount WHERE id = :id',
                    ['amount' => 100, 'id' => $to]
                )
            );
        },
        TransactionOptions::default()
            ->withAttempts(3)
            ->withIsolationLevel(IsolationLevel::REPEATABLE_READ)
    )
);
```

On each retry the callback runs again from scratch on a fresh `BEGIN`. The rollback from the failed attempt is issued automatically before the next attempt begins.

---

### `TransactionOptions` reference

`TransactionOptions` is an immutable value object. All `with*()` methods return a new instance.

```php
use Hibla\Sql\TransactionOptions;
use Hibla\Postgres\Enums\IsolationLevel;

$options = TransactionOptions::default()
    ->withAttempts(5)
    ->withIsolationLevel(IsolationLevel::SERIALIZABLE)
    ->withRetryableExceptions([MyOptimisticLockException::class]);
```

| Method | Description |
|---|---|
| `TransactionOptions::default()` | Returns a default instance: 1 attempt, no isolation level, no custom retryable exceptions. |
| `withAttempts(int $n)` | Maximum number of attempts (must be >= 1). |
| `withIsolationLevel(IsolationLevelInterface $level)` | Sets the isolation level for each attempt. Applied via `SET TRANSACTION ISOLATION LEVEL` scoped to the transaction, never to the session. |
| `withRetryableExceptions(callable\|array $exceptions)` | Extends retry logic for third-party exceptions you cannot modify. Accepts a class-string array or a `callable(\Throwable): bool` predicate. |
| `withoutRetryableExceptions()` | Removes any previously set custom retry predicate. |

**Retry decision hierarchy.** When a transaction attempt fails, `transaction()` calls `$options->shouldRetry($e)` to decide whether to try again. The check follows a strict three-tier order:

1. **`RetryableException` marker interface.** Any exception implementing this interface retries automatically. `DeadlockException` (SQLSTATE `40P01`) and `LockWaitTimeoutException` (SQLSTATE `55P03`) implement it out of the box. Your own exceptions can opt in the same way:

    ```php
    class MyOptimisticLockException extends \RuntimeException
        implements \Hibla\Sql\Exceptions\RetryableException {}
    ```

2. **Known permanent SQL failures.** Exceptions the SQL layer has classified as non-retryable are never retried, regardless of what any user predicate returns. This protects against accidentally retrying errors that will never resolve, such as a `UNIQUE` constraint violation. These include `ConstraintViolationException`, `QueryException`, `PreparedException`, `ConnectionException`, and `TransactionException`.

3. **User predicate.** For third-party exceptions that fail tiers 1 and 2, the predicate from `withRetryableExceptions()` is consulted:

    ```php
    // Retry by class list
    $options = TransactionOptions::default()
        ->withAttempts(3)
        ->withRetryableExceptions([ThirdPartyConflictException::class]);

    // Retry by predicate
    $options = TransactionOptions::default()
        ->withAttempts(3)
        ->withRetryableExceptions(
            fn(\Throwable $e) => $e instanceof ThirdPartyConflictException && $e->getCode() === 409
        );
    ```

---

### Low-level API: `beginTransaction()`

Use `beginTransaction()` when you need explicit control over the transaction lifecycle.

```php
$tx = await($client->beginTransaction());

try {
    await(
        $tx->execute(
            'UPDATE accounts SET balance = balance - :amount WHERE id = :id',
            ['amount' => 100, 'id' => $from]
        )
    );
    await(
        $tx->execute(
            'UPDATE accounts SET balance = balance + :amount WHERE id = :id',
            ['amount' => 100, 'id' => $to]
        )
    );
    await($tx->commit());
} catch (\Throwable $e) {
    await($tx->rollback());
    throw $e;
}
```

Unlike `transaction()`, the low-level API does not retry automatically and does not wrap the work in a fiber. Prefer `transaction()` in all cases where it is sufficient.

---

### Tainted state

If any query inside a transaction throws, the transaction is immediately marked **tainted**. All subsequent calls to `query()`, `execute()`, `stream()`, `prepare()`, and `savepoint()` on that transaction will throw a `TransactionException` until the taint is cleared.

The only two operations that accept a tainted transaction are `rollback()` (rolls back and releases the connection) and `rollbackTo(string $identifier)` (rolls back to a savepoint and clears the tainted state).

Attempting to `commit()` a tainted transaction throws `TransactionException` immediately without contacting the server.

```php
$tx = await($client->beginTransaction());

try {
    await($tx->savepoint('before_risky'));

    try {
        await(
            $tx->execute(
                'INSERT INTO external_refs (id) VALUES (:id)',
                ['id' => $externalId]
            )
        );
    } catch (\Throwable $e) {
        // Rolling back to the savepoint also clears the tainted state.
        await($tx->rollbackTo('before_risky'));
    }

    await($tx->releaseSavepoint('before_risky'));
    await($tx->commit());
} catch (\Throwable $e) {
    await($tx->rollback());
    throw $e;
}
```

---

### Cancellation behaviour

Promise cancellation inside a transaction follows the same `enableServerSideCancellation` setting as standalone queries. When enabled and a query promise is cancelled mid-execution, the connection dispatches `pg_cancel_backend` via a side-channel TCP connection. Unlike MySQL's `KILL QUERY`, PostgreSQL sends an `ErrorResponse` on the main wire, which `QueryResultHandler` consumes automatically. No scrub query is needed.

Cancelling the **outer `transaction()` promise** causes the client to interrupt any currently running query on the connection and then issue `ROLLBACK` before the cancellation propagates:

```php
$promise = $client->transaction(function (TransactionInterface $tx) {
    await($tx->execute('UPDATE ...'));
    await($tx->execute('UPDATE ...')); // still running when cancelled
});

Loop::addTimer(2.0, fn() => $promise->cancel());
// The running query is interrupted, ROLLBACK is issued, connection returned to pool.
```

`commit()` and `rollback()` are **never cancellable**, regardless of this setting. Both operations always run to completion so the server-side transaction state is always deterministic.

---

### Savepoints

Savepoints let you mark a point within a transaction and roll back to it selectively without abandoning the entire transaction.

```php
await(
    $client->transaction(function (TransactionInterface $tx) use ($externalId) {
        await(
            $tx->execute(
                'INSERT INTO audit_log (event) VALUES (:event)',
                ['event' => 'attempt']
            )
        );

        await($tx->savepoint('before_risky_op'));

        try {
            await(
                $tx->execute(
                    'INSERT INTO external_refs (id) VALUES (:id)',
                    ['id' => $externalId]
                )
            );
        } catch (\Throwable $e) {
            // Rolls back to the savepoint and clears the tainted state,
            // so subsequent queries are allowed to continue.
            await($tx->rollbackTo('before_risky_op'));
        }

        await($tx->releaseSavepoint('before_risky_op'));
    })
);
```

### Commit and rollback hooks

The `TransactionInterface` exposes `onCommit()` and `onRollback()` methods. These allow you to register callbacks that fire after the transaction has successfully committed or rolled back on the server.

These hooks are useful for triggering side-effects such as dispatching domain events, clearing caches, or enqueuing background jobs, only when you are guaranteed the database state has been durably persisted or completely aborted.

```php
await(
    $client->transaction(function (TransactionInterface $tx) use ($user) {
        await(
            $tx->execute(
                'INSERT INTO users (name, email) VALUES (:name, :email)',
                ['name' => $user->name, 'email' => $user->email]
            )
        );

        // Fires only if COMMIT succeeds
        $tx->onCommit(function () use ($user) {
            EventDispatcher::dispatch(new UserCreated($user));
        });

        // Fires if the transaction rolls back
        $tx->onRollback(function () use ($user) {
            Logger::warning("Failed to persist user: {$user->email}");
        });
    })
);
```

**Hook rules:**
- **Post-execution:** They execute after the `COMMIT` or `ROLLBACK` has been acknowledged by the PostgreSQL server.
- **Mutually exclusive:** A successful commit clears all rollback hooks, and a rollback clears all commit hooks.
- **FIFO order:** Multiple callbacks registered to the same hook are executed in the exact order they were added.
- **Active registration:** Hooks must be registered while the transaction is active. Attempting to call `onCommit()` or `onRollback()` after the transaction has closed will throw a `TransactionException`.

---

### Transaction lifecycle rules

**Isolation level scoping.** Isolation levels are applied via `SET TRANSACTION ISOLATION LEVEL` immediately before `BEGIN`, scoping them strictly to that transaction. The session isolation level is never mutated.

**`commit()` and `rollback()` are uninterruptible.** They are internally wrapped with `Promise::uninterruptible()` so a concurrent `cancel()` on the outer promise does not interrupt either operation mid-flight.

**`rollback()` is idempotent.** Calling it on an already-committed, already-rolled-back, or released transaction silently returns a resolved promise. It is safe to place in `finally` blocks.

**Automatic rollback on garbage collection.** If a `Transaction` object is garbage collected without an explicit `commit()` or `rollback()`, a fire-and-forget `ROLLBACK` is issued automatically. Always manage the lifecycle explicitly rather than relying on this safety net.

**`commit()` is rejected while tainted.** Attempting to commit a tainted transaction throws `TransactionException` immediately without contacting the server.

---

## Pub/Sub: LISTEN and NOTIFY

PostgreSQL's `LISTEN`/`NOTIFY` mechanism provides lightweight asynchronous messaging between database clients. `PostgresClient` exposes two complementary APIs: `notify()` for sending, and `createListener()` for receiving.

### Sending notifications

```php
// Send a notification to a channel with an optional payload
await(
    $client->notify('user.events', json_encode(['type' => 'login', 'userId' => 42]))
);

// Send without a payload
await($client->notify('cache.invalidate'));
```

`notify()` is a thin wrapper around `SELECT pg_notify(channel, payload)` and shares a connection from the pool like any other query.

### Receiving notifications: PostgresListener

`createListener()` creates a **dedicated, unpooled TCP connection** to PostgreSQL that is completely isolated from the connection pool. This separation is necessary because a connection in `LISTEN` mode must maintain an idle read watcher at all times, which is incompatible with the pool's checkout/release lifecycle.

```php
$listener = await($client->createListener());

// Subscribe to one or more channels
await(
    $listener->listen('user.events', function (string $channel, string $payload, int $pid) {
        $event = json_decode($payload, true);
        echo "Received on {$channel} from PID {$pid}: " . print_r($event, true);
    })
);

await(
    $listener->listen('cache.invalidate', function (string $channel, string $payload, int $pid) {
        CacheManager::flush();
    })
);

// Multiple callbacks can be registered to the same channel
await(
    $listener->listen('user.events', function (string $channel, string $payload, int $pid) {
        AuditLogger::log($channel, $payload);
    })
);
```

To stop listening on a specific channel:

```php
await($listener->unlisten('user.events'));
```

To close the listener connection entirely and drop all subscriptions:

```php
await($listener->close());
```

### Auto-reconnect behaviour

The `PostgresListener` includes **transparent auto-reconnection with exponential backoff**. If the underlying TCP connection drops (due to a network partition, server restart, or proxy timeout), the listener detects the disconnection and begins reconnecting automatically. All channel subscriptions are restored on reconnect without any intervention from your application code.

```php
// Customize the reconnect backoff window
$listener = await(
    $client->createListener(
        minReconnectInterval: 0.5,  // first retry after 0.5 seconds
        maxReconnectInterval: 60.0, // cap at 60 seconds
    )
);
```

The backoff interval doubles on each failed attempt, from `minReconnectInterval` up to `maxReconnectInterval`. Once reconnected, all channels are re-subscribed via fresh `LISTEN` commands before the listener is considered ready again.

> **Note on payload size.** PostgreSQL limits `NOTIFY` payloads to 8000 bytes. Larger payloads will cause the server to reject the `pg_notify` call with an error. Store large data in a table and pass only an identifier in the payload if you need to signal larger datasets.

---

## Connection pooling

The pool manages the full connection lifecycle automatically. By default it is **fully lazy** (`minConnections: 0`).

```php
$client = new PostgresClient(
    config: $config,
    minConnections: 0,
    maxConnections: 50,
    idleTimeout: 600,
    maxLifetime: 3600,
    acquireTimeout: 10.0,
    resetConnection: true,
);
```

### Check-on-borrow health strategy

Before a connection is checked out of the pool, the client verifies it is still alive by checking connection state, idle timeout, and max lifetime. Connections that fail any of these checks are discarded and replaced transparently. A more thorough check is also available via `healthCheck()`, which sends a round-trip ping to every idle connection.

### Shutdown strategies

```php
// Graceful: stops new work, waits for active queries to finish, then closes
await($client->closeAsync(timeout: 30.0));

// Force: closes everything immediately, rejects pending waiters
$client->close();
```

The two modes are safe to combine. Calling `close()` while `closeAsync()` is pending will force-resolve the shutdown promise before tearing everything down, so the caller awaiting `closeAsync()` is never left hanging.

The destructor issues a force-close automatically when the object is garbage collected.

### `resetConnection` and statement cache interaction

When `resetConnection` is enabled, `DISCARD ALL` wipes all server-side prepared statement handles and session state. The client automatically clears the per-connection statement cache on checkout to prevent executing stale statement names. The `onConnect` hook is also **re-invoked after every reset** to restore session state.

---

## Health checks and pool stats

### Health check

```php
$result = await($client->healthCheck());
// e.g. ['total_checked' => 5, 'healthy' => 4, 'unhealthy' => 1]
```

### Pool stats

```php
$stats = $client->stats;
// Returns an associative array with keys like:
// 'active_connections', 'total_connections', 'pooled_connections',
// 'waiting_requests', 'draining_connections', 'max_size', ...
```

---

## SSL/TLS

PostgreSQL's `sslmode` option controls both whether SSL is used and how strictly the server's certificate is verified.

```php
// Prefer SSL but allow plaintext fallback (the default)
$client = new PostgresClient([
    'host'    => 'db.example.com',
    'sslmode' => 'prefer',
    // ...
]);

// Require SSL with full server certificate verification
$client = new PostgresClient([
    'host'    => 'db.example.com',
    'sslmode' => 'verify-full',
    'ssl_ca'  => '/etc/ssl/certs/ca-bundle.crt',
    // ...
]);

// Mutual TLS (client certificate and key)
$client = new PostgresClient([
    'host'     => 'db.example.com',
    'sslmode'  => 'verify-full',
    'ssl_ca'   => '/path/to/ca.pem',
    'ssl_cert' => '/path/to/client-cert.pem',
    'ssl_key'  => '/path/to/client-key.pem',
    // ...
]);
```

**`sslmode` values:**

| Value | Description |
|---|---|
| `disable` | Never use SSL. |
| `allow` | Try without SSL first, then retry with SSL if refused. |
| `prefer` | Try with SSL first, then retry without SSL if refused. This is the default. |
| `require` | Always use SSL but do not verify the server certificate. |
| `verify-ca` | Use SSL and verify the server certificate was signed by a trusted CA. |
| `verify-full` | Use SSL, verify the server certificate, and verify the hostname matches. |

SSL negotiation is handled by `libpq` during the connection handshake. The `sslCa`, `sslCert`, and `sslKey` properties map to `libpq`'s `sslrootcert`, `sslcert`, and `sslkey` parameters.

---

## Query cancellation

Server-side query cancellation is **disabled by default**. When disabled, `$promise->cancel()` transitions the promise to the cancelled state on the client side only. The PostgreSQL server continues executing the query to completion.

Enable it explicitly for long-running queries where stopping server execution and releasing locks immediately has meaningful value:

```php
$client = new PostgresClient(
    config: $config,
    enableServerSideCancellation: true,
);

$promise = $client->query('SELECT * FROM huge_table');
Loop::addTimer(5.0, fn() => $promise->cancel()); // pg_cancel_backend dispatched
```

When enabled, cancelling a query promise dispatches `pg_cancel_backend(<pid>)` via a **dedicated side-channel TCP connection** to avoid blocking the main wire. The PostgreSQL server then sends an `ErrorResponse` on the main connection, which `QueryResultHandler` consumes automatically, resetting the connection state to `READY` with no scrub query needed.

**Key implementation details:**

The cancel dispatch is **idempotent**: if a cancel is already in-flight for the same backend PID, a second dispatch is suppressed to prevent orphaned promises. The promise is also registered synchronously before the next event loop tick, so `close()` always sees the pending cancel correctly even under race conditions.

When the pool receives a connection back after a cancellation, it uses a `ping()` as a synchronization barrier to confirm the `ErrorResponse` has been fully drained before returning the connection to the idle pool.

> `commit()` and `rollback()` are never cancellable regardless of this setting.

---

## `onConnect` hook

```php
$client = new PostgresClient(
    config: $config,
    onConnect: function (ConnectionSetup $setup) {
        await($setup->execute("SET SESSION TIME ZONE 'UTC'"));
        await($setup->execute("SET search_path TO myschema, public"));
    }
);
```

The hook receives a `ConnectionSetup` instance, which exposes a minimal query surface (`query()` and `execute()`) without leaking the internal `Connection` object. Both synchronous and async (promise-returning) hooks are supported. If the hook rejects or throws, the connection is dropped entirely rather than returned to the pool in an unknown session state.

> If `resetConnection` is enabled, `DISCARD ALL` wipes all session variables. The `onConnect` hook is therefore **re-invoked after every reset** to restore session state.

---

## Statement caching

Prepared statements are cached **per connection** with LRU eviction (default: 256 slots).

```php
$client = new PostgresClient(
    config: $config,
    enableStatementCache: true,
    statementCacheSize: 512
);

// Invalidate all caches, for example after a schema change
$client->clearStatementCache();
```

When a cached statement is evicted from the LRU cache, `DEALLOCATE` is sent to the server automatically to free the server-side handle. When `resetConnection` is enabled, `DISCARD ALL` drops all server-side statement handles, so the per-connection cache is cleared on checkout to prevent executing stale statement names.

---

## Type casting

Type casting applies **only when `castPreparedTypes` is `true`** (the default) and the query is executed via a prepared statement, meaning any call to `query()` or `execute()` with `$params`, or explicit `prepare()`.

When `castPreparedTypes` is `false`, every column value is returned as a PHP `string` regardless of the PostgreSQL column type, matching the behaviour of the text protocol.

The casting is **OID-based**: the library reads the field type OID from the result metadata and applies the appropriate conversion. This is more precise than name-based heuristics because it works correctly with custom domains and type aliases.

### Scalar types

| PostgreSQL OID | PostgreSQL type | PHP type returned |
|---|---|---|
| `16` | `bool` | `bool` |
| `21` | `int2` | `int` |
| `23` | `int4` | `int` |
| `20` | `int8` | `int` |
| `700` | `float4` | `float` |
| `701` | `float8` | `float` |

### Array types

Array column values are parsed from PostgreSQL's wire literal format (e.g. `{1,2,3}`) into native PHP arrays by `PgArrayParser`.

| PostgreSQL OIDs | PostgreSQL types | PHP type returned |
|---|---|---|
| `1005`, `1007`, `1016` | `_int2`, `_int4`, `_int8` | `array<int, int>` |
| `1021`, `1022` | `_float4`, `_float8` | `array<int, float>` |
| `1000` | `_bool` | `array<int, bool>` |
| `1009`, `1015`, `1014`, `2951`, `199`, `3807`, `1001`, `1231` | `_text`, `_varchar`, `_char`, `_uuid`, `_json`, `_jsonb`, `_bytea`, `_numeric` | `array<int, string>` |

`PgArrayParser` handles nested arrays, quoted elements with escaped characters, and `NULL` values inside arrays.

### Types returned as strings

All other types (including `NUMERIC`/`DECIMAL`, `TEXT`, `VARCHAR`, `UUID`, `JSON`, `JSONB`, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, `BYTEA`, and custom types) are returned as `string`. This is intentional for precision-sensitive types like `NUMERIC`, where casting to `float` would silently lose precision.

```php
$result = await(
    $client->query(
        'SELECT price, quantity FROM products WHERE id = ?',
        [1]
    )
);

$row = $result->fetchOne();
// $row['price']    => string("19.99")   (NUMERIC, preserved as string)
// $row['quantity'] => int(5)            (int4, cast to int)
```

For arithmetic on `NUMERIC` columns, use `bcmath`:

```php
$tax   = bcmul($row['price'], '0.20', 2); // "4.00"
$total = bcadd($row['price'], $tax, 2);   // "23.99"
```

---

## Result inspection

```php
$result = await($client->query('SELECT * FROM users'));

echo $result->rowCount;      // int, rows in result set
echo $result->affectedRows;  // int, rows affected by INSERT/UPDATE/DELETE
echo $result->connectionId;  // int, backend PID from pg_get_pid()
echo $result->insertedOid;   // int|null, OID of the inserted row if applicable
echo $result->columnCount;   // int, number of columns
echo $result->columns;       // list<string> of column names

foreach ($result as $row) {
    echo $row['name'];
}

$row = $result->fetchOne();           // first row as associative array, or null
$all = $result->fetchAll();           // all rows as array of associative arrays
$col = $result->fetchColumn('name');  // all values from a named column
$col = $result->fetchColumn(0);       // all values from column index 0
```

---

## Multiple result sets

Queries that return multiple result sets can be traversed via `nextResult()`:

```php
$result = await($client->query('SELECT * FROM users; SELECT * FROM orders'));

foreach ($result as $row) {
    echo $row['name']; // first result set: users
}

$next = $result->nextResult();
if ($next !== null) {
    foreach ($next as $row) {
        echo $row['total']; // second result set: orders
    }
}
```

---

## API Reference Summary

### `PostgresClient`

Implements `Hibla\Postgres\Interfaces\PostgresClientInterface`.

| Method / Property | Returns | Description |
|---|---|---|
| `$stats` | `array<string, int\|bool>` | Snapshot of pool state. No database round-trip. |
| `query(string $sql, array $params = [])` | `Promise<PostgresResult>` | Execute a query. Uses binary protocol when params are given. Supports all placeholder formats. |
| `execute(string $sql, array $params = [])` | `Promise<int>` | Execute and return affected row count. |
| `executeGetId(string $sql, array $params = [])` | `Promise<int>` | Execute and return the first column of the first row as an integer. Designed for use with `RETURNING id`. |
| `fetchOne(string $sql, array $params = [])` | `Promise<array\|null>` | First row as associative array, or null. |
| `fetchValue(string $sql, $column = null, array $params = [])` | `Promise<mixed>` | Single scalar value from the first row. |
| `prepare(string $sql)` | `Promise<ManagedPreparedStatement>` | Prepare a reusable statement. Supports all placeholder formats. |
| `stream(string $sql, array $params = [], int $bufferSize = 100)` | `Promise<RowStream>` | Stream rows with backpressure. |
| `notify(string $channel, string $payload = '')` | `Promise<void>` | Send an asynchronous notification to a PostgreSQL channel. |
| `createListener(float $minReconnectInterval = 1.0, float $maxReconnectInterval = 30.0)` | `Promise<PostgresListener>` | Create a dedicated listener connection for receiving notifications. |
| `beginTransaction(?IsolationLevelInterface $level = null)` | `Promise<TransactionInterface>` | Begin a transaction manually. |
| `transaction(callable $callback, ?TransactionOptions $options = null)` | `Promise<mixed>` | Run a transaction with automatic commit/rollback and optional retry. |
| `healthCheck()` | `Promise<array<string, int>>` | Pings all idle connections and returns a health summary. |
| `clearStatementCache()` | `void` | Invalidate all per-connection statement caches. |
| `close()` | `void` | Force-close all connections immediately. |
| `closeAsync(float $timeout = 0.0)` | `Promise<void>` | Graceful shutdown. Waits for active queries to finish. |

### `PostgresListener`

| Method | Returns | Description |
|---|---|---|
| `listen(string $channel, callable $callback)` | `Promise<void>` | Subscribe to a channel and register a callback. Multiple callbacks can be registered per channel. |
| `unlisten(string $channel)` | `Promise<void>` | Unsubscribe from a channel and remove all associated callbacks. |
| `close()` | `Promise<void>` | Close the listener connection, dropping all subscriptions immediately. |

The callback signature is `function(string $channel, string $payload, int $pid): void`.

### `PreparedStatementInterface` (`ManagedPreparedStatement`)

| Method | Returns | Description |
|---|---|---|
| `execute(array $params = [])` | `Promise<PostgresResult>` | Execute with given parameters. Supports named params. |
| `executeStream(array $params = [], int $bufferSize = 100)` | `Promise<RowStream>` | Execute and stream results. Supports named params. |
| `close()` | `Promise<void>` | Send `DEALLOCATE` and release the connection to the pool. |

### `TransactionInterface`

| Method | Returns | Description |
|---|---|---|
| `query(string $sql, array $params = [])` | `Promise<PostgresResult>` | Execute a query inside the transaction. |
| `execute(string $sql, array $params = [])` | `Promise<int>` | Execute and return affected rows. |
| `executeGetId(string $sql, array $params = [])` | `Promise<int>` | Execute and return the first column of the first row as an integer. |
| `fetchOne(string $sql, array $params = [])` | `Promise<array\|null>` | First row or null. |
| `fetchValue(string $sql, $column = null, array $params = [])` | `Promise<mixed>` | Scalar value from first row. |
| `stream(string $sql, array $params = [], int $bufferSize = 100)` | `Promise<RowStream>` | Stream rows inside the transaction. |
| `prepare(string $sql)` | `Promise<PreparedStatementInterface>` | Prepare a statement scoped to this transaction. |
| `commit()` | `Promise<void>` | Commit and release connection. |
| `rollback()` | `Promise<void>` | Roll back and release connection. Idempotent. |
| `savepoint(string $identifier)` | `Promise<void>` | Create a savepoint. |
| `rollbackTo(string $identifier)` | `Promise<void>` | Roll back to savepoint and clear tainted state. |
| `releaseSavepoint(string $identifier)` | `Promise<void>` | Release a savepoint. |
| `onCommit(callable $callback)` | `void` | Register a callback to run after a successful commit. |
| `onRollback(callable $callback)` | `void` | Register a callback to run after a rollback. |

---

## Exceptions

All database exceptions extend `Hibla\Sql\Exceptions\SqlException`.

| Exception | SQLSTATE | Thrown when |
|---|---|---|
| `QueryException` | various | General query execution error |
| `PreparedException` | n/a | Statement is used after `close()` |
| `ConnectionException` | n/a | TCP connection fails, drops unexpectedly, or is closed |
| `ConstraintViolationException` | `23xxx` | UNIQUE, FOREIGN KEY, NOT NULL, or CHECK constraint violated |
| `DeadlockException` | `40P01` | PostgreSQL deadlock detected |
| `LockWaitTimeoutException` | `55P03` | Lock wait timeout exceeded |
| `PoolException` | n/a | Pool exhausted, shutting down, or max waiters exceeded |
| `NotInitializedException` | n/a | `PostgresClient` method called after `close()` |
| `ConfigurationException` | n/a | Invalid configuration passed to `PostgresClient` constructor |

---

## Development

### Requirements

- Docker and Docker Compose
- PHP 8.4+
- Composer
- The `pgsql` PHP extension

### Setup

```bash
git clone https://github.com/hiblaphp/postgres.git
cd postgres
composer install
```

### Running tests

The test suite requires a running PostgreSQL instance. Each supported version has a dedicated Docker Compose service pair: one plain TCP and one SSL.

**Start the database services you want to test against:**

```bash
# PostgreSQL 15 (plain + SSL)
docker compose up -d postgres15 postgres15_ssl

# PostgreSQL 16
docker compose up -d postgres16 postgres16_ssl

# PostgreSQL 17
docker compose up -d postgres17 postgres17_ssl

# PostgreSQL 18
docker compose up -d postgres18 postgres18_ssl
```

Wait for the containers to report healthy before running tests:

```bash
docker ps  # all target containers should show (healthy)
```

**Run the tests for a specific version:**

```bash
composer test:pg15
composer test:pg16
composer test:pg17
composer test:pg18

# All versions sequentially
composer test:all
```

**Tear down services when done:**

```bash
docker compose down -v
```

### Static analysis

```bash
composer analyze
```

### Code formatting

```bash
composer format
```

### Port reference

| Service | Plain port | SSL port |
|---|---|---|
| PostgreSQL 15 | 5443 | 5444 |
| PostgreSQL 16 | 5445 | 5446 |
| PostgreSQL 17 | 5447 | 5448 |
| PostgreSQL 18 | 5449 | 5450 |

All ports are defined in `docker-compose.yml`. The Composer test scripts set `POSTGRES_PORT` and `POSTGRES_SSL_PORT` automatically and you do not need to export them manually unless you want to point the suite at an external server.

---

## License

MIT License. See [LICENSE](./LICENSE) for more information.