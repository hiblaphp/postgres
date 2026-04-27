<?php

declare(strict_types=1);

namespace Hibla\Postgres\Internals;

use Hibla\Sql\Exceptions\QueryException;

/**
 * @internal
 */
final class ParamParser
{
    /**
     * Converts `?` or `:name` placeholders to PostgreSQL `$1, $2, …` format
     * and returns the number of parameters, their ordered names (if named),
     * and whether a bare statement-separator (`;`) was seen outside any literal
     * or comment context.
     *
     * @return array{0: string, 1: int, 2: list<string>, 3: bool}
     *                                                            [convertedSql, paramCount, paramNames, hasBareStatement]
     */
    public static function parsePlaceholders(string $sql): array
    {
        $hasQuestion = str_contains($sql, '?');
        $hasColon = str_contains($sql, ':');

        if (! $hasQuestion && ! $hasColon) {
            return [$sql, 0, [], false];
        }

        $count = 0;
        $paramNames = [];
        $paramMap = [];
        $result = '';
        $length = \strlen($sql);
        $state = 'NORMAL';
        $blockDepth = 0;       // tracks nesting level for /* ... */ comments
        $dollarTag = null;    // the opening $tag$ we are waiting to close
        $hasBareStatement = false;

        for ($i = 0; $i < $length; $i++) {
            $char = $sql[$i];
            $next = $sql[$i + 1] ?? '';

            if ($state === 'NORMAL') {

                // Bare semicolon = multiple statements
                if ($char === ';') {
                    $hasBareStatement = true;
                    $result .= $char;

                    continue;
                }

                // Single / double quoted string literals
                if ($char === "'" || $char === '"') {
                    $state = $char;
                    $result .= $char;

                    continue;
                }

                // Line comment (--)
                if ($char === '-' && $next === '-') {
                    $state = '--';
                    $result .= $char;

                    continue;
                }

                // Block comment (/*)
                if ($char === '/' && $next === '*') {
                    $state = '/*';
                    $blockDepth = 1;
                    $result .= $char;

                    continue;
                }

                // Dollar-quoted string: $$ or $tag$
                // A valid dollar-quote tag is: $[A-Za-z_][A-Za-z0-9_]*$ or just $$
                if ($char === '$') {
                    $j = $i + 1;
                    $tag = '';
                    $validTag = true;

                    while ($j < $length && $sql[$j] !== '$') {
                        $c = $sql[$j];
                        $code = \ord($c);

                        if ($j === $i + 1) {
                            // First character of tag: must be letter or underscore
                            if (
                                ($code >= 97 && $code <= 122)   // a-z
                                || ($code >= 65 && $code <= 90) // A-Z
                                || $code === 95                 // _
                            ) {
                                $tag .= $c;
                                $j++;
                            } else {
                                $validTag = false;

                                break;
                            }
                        } else {
                            // Subsequent tag chars: letter, digit, or underscore
                            if (
                                ($code >= 97 && $code <= 122)
                                || ($code >= 65 && $code <= 90)
                                || ($code >= 48 && $code <= 57)
                                || $code === 95
                            ) {
                                $tag .= $c;
                                $j++;
                            } else {
                                $validTag = false;

                                break;
                            }
                        }
                    }

                    if ($validTag && $j < $length && $sql[$j] === '$') {
                        // Consume the full opening $tag$ and enter dollar-quote state
                        $dollarTag = '$' . $tag . '$';
                        $result .= substr($sql, $i, $j - $i + 1);
                        $i = $j;
                        $state = '$$';

                        continue;
                    }

                    // Not a dollar-quote opener — treat $ as a literal character
                    $result .= $char;

                    continue;
                }

                // Positional ? placeholder
                if ($char === '?' && $hasQuestion) {
                    if ($paramNames !== []) {
                        throw new QueryException('Cannot mix ? and :name placeholder formats in the same query');
                    }

                    $result .= '$' . (++$count);

                    continue;
                }

                // PostgreSQL :: cast operator — consume both colons as one unit
                if ($char === ':' && $next === ':') {
                    $result .= '::';
                    $i++;

                    continue;
                }

                // := assignment operator — pass through untouched
                if ($char === ':' && $next === '=') {
                    $result .= $char;

                    continue;
                }

                // Named :name placeholder
                if ($char === ':' && $hasColon) {
                    $nameStart = $i + 1;

                    if ($nameStart < $length) {
                        $firstCode = \ord($sql[$nameStart]);
                        $validFirst = ($firstCode >= 97 && $firstCode <= 122)  // a-z
                            || ($firstCode >= 65 && $firstCode <= 90)          // A-Z
                            || $firstCode === 95;                              // _

                        if ($validFirst) {
                            if ($count > 0 && $paramNames === []) {
                                throw new QueryException('Cannot mix ? and :name placeholder formats in the same query');
                            }

                            $j = $nameStart;
                            $name = '';

                            while ($j < $length) {
                                $c = $sql[$j];
                                $code = \ord($c);

                                if (
                                    ($code >= 97 && $code <= 122)
                                    || ($code >= 65 && $code <= 90)
                                    || ($code >= 48 && $code <= 57)
                                    || $code === 95
                                ) {
                                    $name .= $c;
                                    $j++;
                                } else {
                                    break;
                                }
                            }

                            if ($name !== '') {
                                if (! isset($paramMap[$name])) {
                                    $paramMap[$name] = ++$count;
                                    $paramNames[] = $name;
                                }

                                $result .= '$' . $paramMap[$name];
                                $i = $j - 1;

                                continue;
                            }
                        }
                    }
                }

                $result .= $char;

                // ── QUOTED STRINGS (single or double) ─────────────────────────────
            } elseif ($state === "'" || $state === '"') {
                $result .= $char;

                if ($char === '\\' && $next !== '') {
                    // Consume escaped character (e.g. E'\n', E'\'')
                    $result .= $next;
                    $i++;
                } elseif ($char === $state) {
                    // Doubled-quote escape: 'O''Reilly' or ""ident""
                    if ($next === $state) {
                        $result .= $next;
                        $i++;
                    } else {
                        $state = 'NORMAL';
                    }
                }

                // ── LINE COMMENT (--) ─────────────────────────────────────────────
            } elseif ($state === '--') {
                $result .= $char;

                if ($char === "\n") {
                    $state = 'NORMAL';
                }

                // ── BLOCK COMMENT (/* ... */) with nesting support ────────────────
            } elseif ($state === '/*') {
                $result .= $char;

                if ($char === '/' && $next === '*') {
                    // Nested block-comment open: increment depth and consume '*'
                    $blockDepth++;
                    $result .= $next;
                    $i++;
                } elseif ($char === '*' && $next === '/') {
                    // Block-comment close: consume '/' then decrement
                    $result .= $next;
                    $i++;
                    $blockDepth--;

                    if ($blockDepth === 0) {
                        $state = 'NORMAL';
                    }
                }

                // ── DOLLAR-QUOTED STRING ($tag$ ... $tag$) ────────────────────────
            } elseif ($state === '$$') {
                $tagLen = \strlen($dollarTag);

                if (substr($sql, $i, $tagLen) === $dollarTag) {
                    // Closing tag found — consume it all at once and return to NORMAL
                    $result .= $dollarTag;
                    $i += $tagLen - 1;  // -1 because the for-loop will add 1
                    $state = 'NORMAL';
                    $dollarTag = null;
                } else {
                    $result .= $char;
                }
            }
        }

        return [$result, $count, $paramNames, $hasBareStatement];
    }

    /**
     * Parses SQL with `?`, `:name`, or `$n` placeholders into the format
     * ext-pgsql expects: `$n` placeholders with a flat indexed params array.
     *
     * @param array<string|int, mixed> $params
     *
     * @return array{0: string, 1: array<int, mixed>} [parsedSql, indexedParams]
     */
    public static function parse(string $sql, array $params): array
    {
        if ($params === []) {
            return [$sql, []];
        }

        $isNamed = \is_string(array_key_first($params));

        if ($isNamed) {
            return self::parseNamed($sql, $params);
        }

        $indexedParams = array_values($params);
        $hasDollar = preg_match('/\$\d+/', $sql) === 1;
        $hasQuestion = str_contains($sql, '?');

        if ($hasDollar && $hasQuestion) {
            throw new QueryException('Cannot mix ? and $n placeholder formats in the same query');
        }

        // Detect named placeholders when an indexed array was supplied — must throw
        // rather than silently returning unconverted SQL.
        if (! $hasDollar && ! $hasQuestion && str_contains($sql, ':')) {
            [,, $names] = self::parsePlaceholders($sql);

            if ($names !== []) {
                throw new QueryException(
                    'Named placeholders found in query but an indexed params array was provided'
                );
            }
        }

        // $n-style or plain SQL — pass through as-is
        if ($hasDollar || ! $hasQuestion) {
            return [$sql, $indexedParams];
        }

        // ? placeholders — convert to $n
        [$converted,,, $hasBareStatement] = self::parsePlaceholders($sql);

        if ($hasBareStatement) {
            throw new QueryException('Multiple statements in a single query are not allowed');
        }

        return [$converted, $indexedParams];
    }

    /**
     * Resolves a named associative params array against the ordered param names
     * returned by parsePlaceholders().
     *
     * @param list<string> $paramNames
     * @param array<string, mixed> $params
     *
     * @return array<int, mixed>
     */
    public static function resolveNamed(array $paramNames, array $params): array
    {
        $out = [];

        foreach ($paramNames as $name) {
            if (! \array_key_exists($name, $params)) {
                throw new QueryException("Missing value for named parameter ':$name'");
            }

            $out[] = $params[$name];
        }

        return $out;
    }

    /**
     * @param array<string, mixed> $params
     *
     * @return array{0: string, 1: array<int, mixed>}
     */
    private static function parseNamed(string $sql, array $params): array
    {
        [$convertedSql,, $paramNames, $hasBareStatement] = self::parsePlaceholders($sql);

        if ($hasBareStatement) {
            throw new QueryException('Multiple statements in a single query are not allowed');
        }

        if ($paramNames === []) {
            throw new QueryException('Named params array provided but no :name placeholders found in query');
        }

        return [$convertedSql, self::resolveNamed($paramNames, $params)];
    }
}
