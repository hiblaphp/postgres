<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Sql\Exceptions\QueryException;

/**
 * @internal
 */
final class QueryParser
{
    /**
     * Converts `?` placeholders to PostgreSQL `$1, $2` format.
     * Skips question marks inside string literals (single/double quotes).
     *
     * @return array{0: string, 1: array<int, mixed>} [parsedSql, indexedParams]
     */
    public static function parse(string $sql, array $params): array
    {
        if ($params === []) {
            return[$sql, []];
        }

        $indexedParams = array_values($params);
        $hasDollar = preg_match('/\$\d+/', $sql) === 1;
        $hasQuestion = str_contains($sql, '?');

        if ($hasDollar && $hasQuestion) {
            throw new QueryException('Cannot mix ? and $n placeholder formats in the same query');
        }

        if ($hasDollar || ! $hasQuestion) {
            return [$sql, $indexedParams];
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
                $count++;
                $result .= '$' . $count;

                continue;
            }

            $result .= $char;
        }

        return[$result, $indexedParams];
    }
}
