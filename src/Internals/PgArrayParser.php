<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

/**
 * @internal Parses PostgreSQL array literals into native PHP arrays.
 */
final class PgArrayParser
{
    /**
     * Parses a PostgreSQL array literal string (e.g., '{1,2,3}') into a native PHP array.
     *
     * @param string $data The raw array string from PostgreSQL.
     * @param string $castType The type to cast individual elements to ('int', 'float', 'bool', 'string').
     *
     * @return array<int, mixed>
     */
    public static function parse(string $data, string $castType = 'string'): array
    {
        $data = trim($data);

        if ($data === '' || $data[0] !== '{') {
            return [];
        }

        $position = 0;

        return self::parseRecursive($data, $position, $castType);
    }

    /**
     * @return array<int, mixed>
     */
    private static function parseRecursive(string $data, int &$position, string $castType): array
    {
        $result = [];
        $length = \strlen($data);
        $position++;

        $buffer = '';
        $inQuotes = false;
        $inEscape = false;
        $wasQuoted = false;

        while ($position < $length) {
            $char = $data[$position];

            // Handle escaped characters (e.g., \")
            if ($inEscape) {
                $buffer .= $char;
                $inEscape = false;
                $position++;

                continue;
            }

            if ($char === '\\') {
                $inEscape = true;
                $position++;

                continue;
            }

            // Handle quotes
            if ($inQuotes) {
                if ($char === '"') {
                    $inQuotes = false;
                    $wasQuoted = true;
                } else {
                    $buffer .= $char;
                }
                $position++;

                continue;
            }

            if ($char === '"') {
                $inQuotes = true;
                $wasQuoted = true;
                $position++;

                continue;
            }

            // Handle nested arrays
            if ($char === '{') {
                $result[] = self::parseRecursive($data, $position, $castType);

                continue;
            }

            // Handle element boundaries
            if ($char === '}' || $char === ',') {
                if (! $wasQuoted) {
                    $buffer = trim($buffer);
                }

                // Only push if there's actual data, or if it was explicitly quoted (""),
                // or if it's a null between commas (e.g., {1,,3})
                if ($buffer !== '' || $wasQuoted || (isset($data[$position - 1]) && $data[$position - 1] === ',')) {
                    if (! $wasQuoted && ($buffer === '' || strtoupper($buffer) === 'NULL')) {
                        $result[] = null;
                    } else {
                        $result[] = self::cast($buffer, $castType);
                    }
                }

                $buffer = '';
                $wasQuoted = false;

                if ($char === '}') {
                    $position++;

                    break;
                }

                $position++;

                continue;
            }

            $buffer .= $char;
            $position++;
        }

        return $result;
    }

    private static function cast(string $value, string $type): int|float|bool|string
    {
        return match ($type) {
            'int' => (int) $value,
            'float' => match (strtoupper($value)) {
                'NAN' => NAN,
                'INFINITY', 'INF' => INF,
                '-INFINITY', '-INF' => -INF,
                default => (float) $value,
            },
            'bool' => $value === 't',
            default => $value,
        };
    }
}
