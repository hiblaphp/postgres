<?php

declare(strict_types=1);

namespace Hibla\Postgres\ValueObjects;

final readonly class PgSqlConfig
{
    public const float DEFAULT_KILL_TIMEOUT_SECONDS = 3.0;

    /**
     * @param string $host Hostname or IP of the PostgreSQL server.
     * @param int $port TCP port (default 5432).
     * @param string $username PostgreSQL username.
     * @param string $password PostgreSQL password.
     * @param string $database Database name.
     * @param string $sslmode SSL mode (disable, allow, prefer, require, verify-ca, verify-full).
     * @param string|null $sslCa Path to the SSL CA certificate file (maps to sslrootcert).
     * @param string|null $sslCert Path to the SSL client certificate file (maps to sslcert).
     * @param string|null $sslKey Path to the SSL client key file (maps to sslkey).
     * @param int $connectTimeout Seconds before a connect attempt is aborted.
     * @param string $applicationName The application_name reported to Postgres.
     * @param float $killTimeoutSeconds How long to wait for a pg_cancel_backend() side-channel.
     * @param bool $enableServerSideCancellation Whether to dispatch pg_cancel_backend() on promise cancellation.
     * @param bool $resetConnection Whether to send `DISCARD ALL` on release to clear session state.
     */
    public function __construct(
        public string $host,
        public int $port = 5432,
        public string $username = 'postgres',
        public string $password = '',
        public string $database = '',
        public string $sslmode = 'prefer',
        public ?string $sslCa = null,
        public ?string $sslCert = null,
        public ?string $sslKey = null,
        public int $connectTimeout = 10,
        public string $applicationName = 'hibla_pgsql',
        public float $killTimeoutSeconds = self::DEFAULT_KILL_TIMEOUT_SECONDS,
        public bool $enableServerSideCancellation = false,
        public bool $resetConnection = false,
    ) {
        if ($this->killTimeoutSeconds <= 0) {
            throw new \InvalidArgumentException(
                \sprintf('killTimeoutSeconds must be greater than zero, %f given.', $this->killTimeoutSeconds)
            );
        }
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function fromArray(array $config): self
    {
        $host = $config['host'] ?? throw new \InvalidArgumentException('Host is required');
        if (! \is_string($host)) {
            throw new \InvalidArgumentException('Host must be a string');
        }

        $port = $config['port'] ?? 5432;
        $port = is_numeric($port) ? (int) $port : 5432;

        $sslCa = $config['ssl_ca'] ?? null;
        if (! \is_string($sslCa)) {
            $sslCa = null;
        }

        $sslCert = $config['ssl_cert'] ?? null;
        if (! \is_string($sslCert)) {
            $sslCert = null;
        }

        $sslKey = $config['ssl_key'] ?? null;
        if (! \is_string($sslKey)) {
            $sslKey = null;
        }

        return new self(
            host: $host,
            port: $port,
            username: self::getString($config['username'] ?? null, 'postgres'),
            password: self::getString($config['password'] ?? null, ''),
            database: self::getString($config['database'] ?? null, ''),
            sslmode: self::getString($config['sslmode'] ?? null, 'prefer'),
            sslCa: $sslCa,
            sslCert: $sslCert,
            sslKey: $sslKey,
            connectTimeout: is_numeric($config['connect_timeout'] ?? 10) ? (int) ($config['connect_timeout'] ?? 10) : 10,
            applicationName: self::getString($config['application_name'] ?? null, 'hibla_pgsql'),
            killTimeoutSeconds: is_numeric($config['kill_timeout_seconds'] ?? self::DEFAULT_KILL_TIMEOUT_SECONDS)
                ? (float) ($config['kill_timeout_seconds'] ?? self::DEFAULT_KILL_TIMEOUT_SECONDS)
                : self::DEFAULT_KILL_TIMEOUT_SECONDS,
            enableServerSideCancellation: \is_scalar($config['enable_server_side_cancellation'] ?? false)
                ? (bool) ($config['enable_server_side_cancellation'] ?? false)
                : false,
            resetConnection: \is_scalar($config['reset_connection'] ?? false)
                ? (bool) ($config['reset_connection'] ?? false)
                : false,
        );
    }

    public static function fromUri(string $uri): self
    {
        if (! str_contains($uri, '://')) {
            $uri = 'postgresql://' . $uri;
        }

        $parts = parse_url($uri);

        if ($parts === false || ! isset($parts['host'])) {
            throw new \InvalidArgumentException('Invalid PostgreSQL URI: ' . $uri);
        }

        if (isset($parts['scheme']) && ! \in_array($parts['scheme'], ['postgres', 'postgresql'], true)) {
            throw new \InvalidArgumentException('Invalid URI scheme "' . $parts['scheme'] . '", expected "postgresql"');
        }

        $query = [];
        if (isset($parts['query']) && \is_string($parts['query'])) {
            parse_str($parts['query'], $query);
        }

        return new self(
            host: (string) $parts['host'],
            port: isset($parts['port']) ? (int) $parts['port'] : 5432,
            username: isset($parts['user']) ? rawurldecode((string) $parts['user']) : 'postgres',
            password: isset($parts['pass']) ? rawurldecode((string) $parts['pass']) : '',
            database: isset($parts['path']) ? rawurldecode(ltrim((string) $parts['path'], '/')) : '',
            sslmode: isset($query['sslmode']) && \is_string($query['sslmode']) ? $query['sslmode'] : 'prefer',
            sslCa: isset($query['ssl_ca']) && \is_string($query['ssl_ca']) ? $query['ssl_ca'] : null,
            sslCert: isset($query['ssl_cert']) && \is_string($query['ssl_cert']) ? $query['ssl_cert'] : null,
            sslKey: isset($query['ssl_key']) && \is_string($query['ssl_key']) ? $query['ssl_key'] : null,
            connectTimeout: isset($query['connect_timeout']) ? (int) $query['connect_timeout'] : 10,
            applicationName: isset($query['application_name']) && \is_string($query['application_name']) ? $query['application_name'] : 'hibla_pgsql',
            killTimeoutSeconds: isset($query['kill_timeout_seconds']) ? (float) $query['kill_timeout_seconds'] : self::DEFAULT_KILL_TIMEOUT_SECONDS,
            enableServerSideCancellation: isset($query['enable_server_side_cancellation'])
                ? filter_var($query['enable_server_side_cancellation'], FILTER_VALIDATE_BOOLEAN)
                : false,
            resetConnection: isset($query['reset_connection']) ? filter_var($query['reset_connection'], FILTER_VALIDATE_BOOLEAN) : false,
        );
    }

    public function withQueryCancellation(bool $enabled): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            username: $this->username,
            password: $this->password,
            database: $this->database,
            sslmode: $this->sslmode,
            sslCa: $this->sslCa,
            sslCert: $this->sslCert,
            sslKey: $this->sslKey,
            connectTimeout: $this->connectTimeout,
            applicationName: $this->applicationName,
            killTimeoutSeconds: $this->killTimeoutSeconds,
            enableServerSideCancellation: $enabled,
            resetConnection: $this->resetConnection,
        );
    }

    /**
     * Builds the connection string format required by pg_connect()
     */
    public function toConnectionString(): string
    {
        $parts = [
            "host='{$this->host}'",
            "port='{$this->port}'",
            "dbname='{$this->database}'",
            "user='{$this->username}'",
            "sslmode='{$this->sslmode}'",
            "connect_timeout='{$this->connectTimeout}'",
            "application_name='{$this->applicationName}'",
        ];

        if ($this->password !== '') {
            $escapedPassword = str_replace("'", "\\'", $this->password);
            $parts[] = "password='{$escapedPassword}'";
        }

        if ($this->sslCa !== null) {
            $parts[] = "sslrootcert='{$this->sslCa}'";
        }

        if ($this->sslCert !== null) {
            $parts[] = "sslcert='{$this->sslCert}'";
        }

        if ($this->sslKey !== null) {
            $parts[] = "sslkey='{$this->sslKey}'";
        }

        return implode(' ', $parts);
    }

    private static function getString(mixed $value, string $default): string
    {
        return \is_scalar($value) ? (string) $value : $default;
    }
}
