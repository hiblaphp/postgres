<?php

declare(strict_types=1);

namespace Hibla\Postgres\Tests\Internals;

use Hibla\Postgres\Internals\ParamParser;
use Hibla\Sql\Exceptions\QueryException;

describe('ParamParser', function (): void {
    describe('parsePlaceholders()', function (): void {
        it('leaves a plain query untouched', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM users');
            expect($sql)->toBe('SELECT * FROM users')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles an empty string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('');
            expect($sql)->toBe('')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles a whitespace-only query', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('   ');
            expect($sql)->toBe('   ')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('converts a single ? to $1', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM users WHERE id = ?');
            expect($sql)->toBe('SELECT * FROM users WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe([])
            ;
        });

        it('converts multiple ? placeholders in order', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE a = ? AND b = ? AND c = ?');
            expect($sql)->toBe('SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3')
                ->and($count)->toBe(3)
                ->and($names)->toBe([])
            ;
        });

        it('handles a single ? at the very end of the string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT ?');
            expect($sql)->toBe('SELECT $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe([])
            ;
        });

        it('handles a query that is only a positional placeholder', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('?');
            expect($sql)->toBe('$1')
                ->and($count)->toBe(1)
                ->and($names)->toBe([])
            ;
        });

        it('converts a single :name to $1', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM users WHERE id = :id');
            expect($sql)->toBe('SELECT * FROM users WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('converts multiple distinct :name placeholders in order', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM users WHERE id = :id AND status = :status'
            );
            expect($sql)->toBe('SELECT * FROM users WHERE id = $1 AND status = $2')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['id', 'status'])
            ;
        });

        it('parses named parameters with underscores', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('INSERT INTO t VALUES (:first_name, :last_name)');
            expect($sql)->toBe('INSERT INTO t VALUES ($1, $2)')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['first_name', 'last_name'])
            ;
        });

        it('parses a named parameter leading with an underscore', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :_private_id');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['_private_id'])
            ;
        });

        it('parses named parameters with digits in the name', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM t WHERE col1 = :val1 AND col2 = :val2'
            );
            expect($sql)->toBe('SELECT * FROM t WHERE col1 = $1 AND col2 = $2')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['val1', 'val2'])
            ;
        });

        it('parses a single-character parameter name', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :i');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['i'])
            ;
        });

        it('parses a parameter name that is only underscores', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :___');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['___'])
            ;
        });

        it('parses a named parameter at the very end of the string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :id');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a query that is only a named parameter', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(':id');
            expect($sql)->toBe('$1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('parses a named parameter immediately followed by a comma', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('INSERT INTO t (a, b) VALUES (:a, :b)');
            expect($sql)->toBe('INSERT INTO t (a, b) VALUES ($1, $2)')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['a', 'b'])
            ;
        });

        it('parses a named parameter immediately followed by a closing parenthesis', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE (id = :id)');
            expect($sql)->toBe('SELECT * FROM t WHERE (id = $1)')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('parses a named parameter immediately followed by a newline', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t\nWHERE id = :id\nAND active = :active");
            expect($sql)->toBe("SELECT * FROM t\nWHERE id = \$1\nAND active = \$2")
                ->and($count)->toBe(2)
                ->and($names)->toBe(['id', 'active'])
            ;
        });

        it('parses two named parameters with no space between them', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT :a:b');
            expect($sql)->toBe('SELECT $1$2')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['a', 'b'])
            ;
        });

        it('handles tab characters between tokens', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT\t*\tFROM\tt\tWHERE\tid\t=\t:id");
            expect($sql)->toBe("SELECT\t*\tFROM\tt\tWHERE\tid\t=\t\$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('deduplicates repeated :name to the same $n placeholder', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM users WHERE (first = :name OR last = :name)'
            );
            expect($sql)->toBe('SELECT * FROM users WHERE (first = $1 OR last = $1)')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['name'])
            ;
        });

        it('deduplicates across a UNION', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT id FROM t WHERE role = :role UNION SELECT id FROM u WHERE role = :role'
            );
            expect($sql)->toBe('SELECT id FROM t WHERE role = $1 UNION SELECT id FROM u WHERE role = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['role'])
            ;
        });

        it('assigns distinct $n when different names are interleaved with a repeated name', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'WHERE a = :x AND b = :y AND c = :x'
            );
            expect($sql)->toBe('WHERE a = $1 AND b = $2 AND c = $1')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['x', 'y'])
            ;
        });

        it('passes through the :: cast operator untouched', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT col::text FROM t WHERE id = :id');
            expect($sql)->toBe('SELECT col::text FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles multiple consecutive :: casts', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT col::text::varchar FROM t WHERE id = :id');
            expect($sql)->toBe('SELECT col::text::varchar FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a :: cast on a string literal', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT 'x'::text, :param");
            expect($sql)->toBe("SELECT 'x'::text, \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['param'])
            ;
        });

        it('handles a named parameter cast to a type after parsing', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT :val::text::varchar(255) FROM t');
            expect($sql)->toBe('SELECT $1::text::varchar(255) FROM t')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['val'])
            ;
        });

        it('handles :: at the very end of the string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT col::');
            expect($sql)->toBe('SELECT col::')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('passes through the := assignment operator untouched', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT @row := @row + 1, :param');
            expect($sql)->toBe('SELECT @row := @row + 1, $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['param'])
            ;
        });

        it('ignores placeholders inside single-quoted strings', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM logs WHERE message = 'Error: :not_param' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM logs WHERE message = 'Error: :not_param' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('ignores placeholders inside double-quoted identifiers', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM logs WHERE message = "Error: :not_param" AND id = :id'
            );
            expect($sql)->toBe('SELECT * FROM logs WHERE message = "Error: :not_param" AND id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles escaped backslash in a string literal', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE path = '\\\\' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE path = '\\\\' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles doubled single-quote escape (O\'\'Reilly)', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM logs WHERE msg = 'O''Reilly :trap' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM logs WHERE msg = 'O''Reilly :trap' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles adjacent string literals', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE a = ':trap' AND b = ':trap' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE a = ':trap' AND b = ':trap' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles an empty string literal', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE name = '' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE name = '' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles string-heavy query without false positives', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE label = 'hello:world' AND code = 'foo::bar' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE label = 'hello:world' AND code = 'foo::bar' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a very long string literal without false positives', function (): void {
            $longLiteral = str_repeat(':trap ', 1000);
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE msg = '{$longLiteral}' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE msg = '{$longLiteral}' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('ignores placeholders inside an uppercase E-string literal', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE name = E':trap' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE name = E':trap' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('ignores placeholders inside a lowercase e-string literal', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE name = e':trap' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE name = e':trap' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a backslash-escaped single-quote inside an E-string without leaving string mode', function (): void {
            // E'it\'s :trap' — the \' is an escape, not a string terminator
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE name = E'it\\'s :trap' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE name = E'it\\'s :trap' AND id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('ignores placeholders inside an untagged dollar-quoted string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT $$ :not_a_param $$ FROM t WHERE id = :id'
            );
            expect($sql)->toBe('SELECT $$ :not_a_param $$ FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('ignores placeholders inside a tagged dollar-quoted string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT $body$ :trap $body$ WHERE id = :id'
            );
            expect($sql)->toBe('SELECT $body$ :trap $body$ WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('does not close a dollar-quoted string on a mismatched inner tag', function (): void {
            // $outer$ ... $inner$ ... $inner$ ... $outer$ — inner tags are not closers
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT $outer$ :trap $inner$ :still_trap $inner$ :also_trap $outer$ WHERE id = :id'
            );
            expect($sql)->toBe('SELECT $outer$ :trap $inner$ :still_trap $inner$ :also_trap $outer$ WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('does not treat a dollar sign followed by a digit inside a dollar-quoted string as a placeholder', function (): void {
            // Inside $$...$$ backslashes and dollar signs are literal
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT $$ cost is $9.99 $$ WHERE id = :id'
            );
            expect($sql)->toBe('SELECT $$ cost is $9.99 $$ WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles two adjacent dollar-quoted strings without false positives', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT $$ :a $$, $$ :b $$ WHERE id = :id'
            );
            expect($sql)->toBe('SELECT $$ :a $$, $$ :b $$ WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles an unterminated untagged dollar-quoted string gracefully', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT $$ :trap');
            expect($sql)->toBe('SELECT $$ :trap')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles an unterminated tagged dollar-quoted string gracefully', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT $tag$ :trap');
            expect($sql)->toBe('SELECT $tag$ :trap')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('ignores placeholders inside line comments (--)', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM users -- comment with :param\n WHERE id = :id"
            );
            expect($sql)->toBe("SELECT * FROM users -- comment with :param\n WHERE id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('ignores placeholders inside block comments', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM users /* comment with :param \n multiline :param2 */ WHERE id = :id"
            );
            expect($sql)->toBe("SELECT * FROM users /* comment with :param \n multiline :param2 */ WHERE id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('resumes parsing correctly after a block comment', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT /* skip :x */ :a, :b FROM t');
            expect($sql)->toBe('SELECT /* skip :x */ $1, $2 FROM t')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['a', 'b'])
            ;
        });

        it('resumes parsing correctly after a line comment', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT -- skip :x\n :a FROM t");
            expect($sql)->toBe("SELECT -- skip :x\n \$1 FROM t")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['a'])
            ;
        });

        it('treats a line comment at the end of the string as consuming the rest', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT 1 -- :trap');
            expect($sql)->toBe('SELECT 1 -- :trap')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles an empty block comment', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT /**/ :id FROM t');
            expect($sql)->toBe('SELECT /**/ $1 FROM t')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a named parameter immediately after a -- opener with no space', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t --:trap\nWHERE id = :id");
            expect($sql)->toBe("SELECT * FROM t --:trap\nWHERE id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a very long block comment without false positives', function (): void {
            $longComment = str_repeat(':trap ', 1000);
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t /* {$longComment} */ WHERE id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t /* {$longComment} */ WHERE id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('exits a line comment on Windows-style CRLF line endings', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t -- :trap\r\nWHERE id = :id");
            expect($sql)->toBe("SELECT * FROM t -- :trap\r\nWHERE id = \$1")
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles one level of nested block comment', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT /* outer /* :inner */ still_comment :also */ :real'
            );
            expect($sql)->toBe('SELECT /* outer /* :inner */ still_comment :also */ $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['real'])
            ;
        });

        it('handles deeply nested block comments', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT /* a /* b /* :deep */ :mid */ :outer_still */ :real'
            );
            expect($sql)->toBe('SELECT /* a /* b /* :deep */ :mid */ :outer_still */ $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['real'])
            ;
        });

        it('handles an unterminated nested block comment gracefully', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('/* /* :trap */');
            expect($sql)->toBe('/* /* :trap */')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('resumes parsing correctly after a nested block comment', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT /* /* skip */ :also_skip */ :real FROM t'
            );
            expect($sql)->toBe('SELECT /* /* skip */ :also_skip */ $1 FROM t')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['real'])
            ;
        });

        it('treats a lone colon with no valid identifier as literal', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT : FROM t WHERE id = :id');
            expect($sql)->toBe('SELECT : FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a colon at the very end of the string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE x =:');
            expect($sql)->toBe('SELECT * FROM t WHERE x =:')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('does not treat a colon followed by a space as a named parameter', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT : param FROM t WHERE id = :id');
            expect($sql)->toBe('SELECT : param FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('does not treat a colon followed by a digit as a named parameter', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :id LIMIT :1');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1 LIMIT :1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('does not treat a high-byte character after a colon as a valid identifier start', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t WHERE id = :\xc3\xa9dition AND name = :name");
            expect($names)->toBe(['name']);
        });

        it('treats array slice colon syntax as literal', function (): void {
            // arr[1:3] — colon between two digits, not a named param
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT arr[1:3] FROM t WHERE id = :id'
            );
            expect($sql)->toBe('SELECT arr[1:3] FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('handles a :: cast inside array brackets without confusion', function (): void {
            // arr[1::integer] — double colon cast, not a named param
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT arr[1::integer] FROM t WHERE id = :id'
            );
            expect($sql)->toBe('SELECT arr[1::integer] FROM t WHERE id = $1')
                ->and($count)->toBe(1)
                ->and($names)->toBe(['id'])
            ;
        });

        it('throws when mixing ? after :name', function (): void {
            expect(fn () => ParamParser::parsePlaceholders('WHERE a = :name AND b = ?'))
                ->toThrow(QueryException::class, 'Cannot mix')
            ;
        });

        it('throws when mixing :name after ?', function (): void {
            expect(fn () => ParamParser::parsePlaceholders('WHERE a = ? AND b = :name'))
                ->toThrow(QueryException::class, 'Cannot mix')
            ;
        });

        it('handles an unterminated single-quoted string gracefully', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t WHERE msg = 'unterminated :trap");
            expect($sql)->toBe("SELECT * FROM t WHERE msg = 'unterminated :trap")
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles an unterminated double-quoted identifier gracefully', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE msg = "unterminated :trap');
            expect($sql)->toBe('SELECT * FROM t WHERE msg = "unterminated :trap')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles an unterminated block comment gracefully', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t /* unterminated :trap');
            expect($sql)->toBe('SELECT * FROM t /* unterminated :trap')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles a query that is only an unterminated string', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("'");
            expect($sql)->toBe("'")
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles a query that is only an unterminated block comment', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('/*');
            expect($sql)->toBe('/*')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles a query that is only a bare colon', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(':');
            expect($sql)->toBe(':')
                ->and($count)->toBe(0)
                ->and($names)->toBe([])
            ;
        });

        it('handles an UPDATE statement', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'UPDATE users SET name = :name, email = :email WHERE id = :id'
            );
            expect($sql)->toBe('UPDATE users SET name = $1, email = $2 WHERE id = $3')
                ->and($count)->toBe(3)
                ->and($names)->toBe(['name', 'email', 'id'])
            ;
        });

        it('handles an INSERT with many columns', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'INSERT INTO users (name, email, age, role) VALUES (:name, :email, :age, :role)'
            );
            expect($sql)->toBe('INSERT INTO users (name, email, age, role) VALUES ($1, $2, $3, $4)')
                ->and($count)->toBe(4)
                ->and($names)->toBe(['name', 'email', 'age', 'role'])
            ;
        });

        it('handles a DELETE statement', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'DELETE FROM t WHERE id = :id AND tenant = :tenant'
            );
            expect($sql)->toBe('DELETE FROM t WHERE id = $1 AND tenant = $2')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['id', 'tenant'])
            ;
        });

        it('handles a LIMIT / OFFSET pattern', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM t WHERE status = :status LIMIT :limit OFFSET :offset'
            );
            expect($sql)->toBe('SELECT * FROM t WHERE status = $1 LIMIT $2 OFFSET $3')
                ->and($count)->toBe(3)
                ->and($names)->toBe(['status', 'limit', 'offset'])
            ;
        });

        it('handles a CASE expression with named parameters', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT CASE WHEN status = :active THEN 1 WHEN status = :inactive THEN 0 END FROM t'
            );
            expect($sql)->toBe('SELECT CASE WHEN status = $1 THEN 1 WHEN status = $2 THEN 0 END FROM t')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['active', 'inactive'])
            ;
        });

        it('handles an IN clause with named parameters', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM t WHERE id IN (:id1, :id2, :id3)'
            );
            expect($sql)->toBe('SELECT * FROM t WHERE id IN ($1, $2, $3)')
                ->and($count)->toBe(3)
                ->and($names)->toBe(['id1', 'id2', 'id3'])
            ;
        });

        it('handles a subquery containing named parameters', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE role = :role) AND active = :active'
            );
            expect($sql)->toBe('SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE role = $1) AND active = $2')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['role', 'active'])
            ;
        });

        it('handles a WITH (CTE) containing named parameters', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                'WITH active AS (SELECT * FROM t WHERE status = :status) SELECT * FROM active WHERE id = :id'
            );
            expect($sql)->toBe('WITH active AS (SELECT * FROM t WHERE status = $1) SELECT * FROM active WHERE id = $2')
                ->and($count)->toBe(2)
                ->and($names)->toBe(['status', 'id'])
            ;
        });

        it('handles a large number of named parameters', function (): void {
            $parts = array_map(fn (int $n) => "col{$n} = :param{$n}", range(1, 200));
            $query = 'SELECT * FROM t WHERE ' . implode(' AND ', $parts);
            [$sql, $count, $names] = ParamParser::parsePlaceholders($query);
            expect($count)->toBe(200)
                ->and(count($names))->toBe(200)
                ->and($names[0])->toBe('param1')
                ->and($names[199])->toBe('param200')
                ->and(substr_count($sql, '$'))->toBe(200)
            ;
        });

        it('returns the same result on repeated calls (no side effects)', function (): void {
            $query = 'SELECT * FROM t WHERE id = :id AND status = :status';
            [$sql1, $count1, $names1] = ParamParser::parsePlaceholders($query);
            [$sql2, $count2, $names2] = ParamParser::parsePlaceholders($query);
            expect($sql1)->toBe($sql2)
                ->and($count1)->toBe($count2)
                ->and($names1)->toBe($names2)
            ;
        });

        it('does not treat SQL keywords after a colon as anything other than a safe placeholder', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :SELECT');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($names)->toBe(['SELECT'])
            ;
        });

        it('does not allow a semicolon to break out of a parameter name', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :id; DROP TABLE t--');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1; DROP TABLE t--')
                ->and($names)->toBe(['id'])
            ;
        });

        it('stops a parameter name at a dash', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :user-id');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1-id')
                ->and($names)->toBe(['user'])
            ;
        });

        it('stops a parameter name at a dot', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders('SELECT * FROM t WHERE id = :table.column');
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1.column')
                ->and($names)->toBe(['table'])
            ;
        });

        it('stops a parameter name at a quote character', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t WHERE id = :user'injection");
            expect($sql)->toBe("SELECT * FROM t WHERE id = \$1'injection")
                ->and($names)->toBe(['user'])
            ;
        });

        it('stops a parameter name at a null byte', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders("SELECT * FROM t WHERE id = :user\x00injection");
            expect($sql)->toBe("SELECT * FROM t WHERE id = \$1\x00injection")
                ->and($names)->toBe(['user'])
            ;
        });

        it('treats an injection attempt inside a string literal as inert', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE msg = 'x'' OR ''1''=''1' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE msg = 'x'' OR ''1''=''1' AND id = \$1")
                ->and($names)->toBe(['id'])
            ;
        });

        it('treats a comment-based injection inside a string literal as inert', function (): void {
            [$sql, $count, $names] = ParamParser::parsePlaceholders(
                "SELECT * FROM t WHERE msg = 'hello -- :trap' AND id = :id"
            );
            expect($sql)->toBe("SELECT * FROM t WHERE msg = 'hello -- :trap' AND id = \$1")
                ->and($names)->toBe(['id'])
            ;
        });
    });

    describe('resolveNamed()', function (): void {

        it('maps param names to indexed values in order', function (): void {
            $result = ParamParser::resolveNamed(['id', 'status'], ['id' => 42, 'status' => 'active']);
            expect($result)->toBe([42, 'active']);
        });

        it('resolves params in the order of $paramNames, not the order of $params', function (): void {
            $result = ParamParser::resolveNamed(['b', 'a'], ['a' => 1, 'b' => 2]);
            expect($result)->toBe([2, 1]);
        });

        it('produces a single value for a deduplicated param name', function (): void {
            $result = ParamParser::resolveNamed(['name'], ['name' => 'Alice']);
            expect($result)->toBe(['Alice']);
        });

        it('ignores extra keys in $params that have no matching placeholder', function (): void {
            $result = ParamParser::resolveNamed(['id'], ['id' => 1, 'extra' => 'ignored']);
            expect($result)->toBe([1]);
        });

        it('passes null values through', function (): void {
            $result = ParamParser::resolveNamed(['id', 'deleted_at'], ['id' => 5, 'deleted_at' => null]);
            expect($result)->toBe([5, null]);
        });

        it('throws when a required param name is missing from the array', function (): void {
            expect(fn () => ParamParser::resolveNamed(['id', 'status'], ['id' => 1]))
                ->toThrow(QueryException::class, ':status')
            ;
        });

        it('returns an empty array for empty paramNames', function (): void {
            expect(ParamParser::resolveNamed([], ['id' => 1]))->toBe([]);
        });
    });

    describe('parse()', function (): void {
        it('returns the query unchanged when params is empty', function (): void {
            [$sql, $params] = ParamParser::parse('SELECT * FROM users', []);
            expect($sql)->toBe('SELECT * FROM users')
                ->and($params)->toBe([])
            ;
        });

        it('converts ? placeholders and returns indexed params', function (): void {
            [$sql, $params] = ParamParser::parse('SELECT * FROM t WHERE id = ?', [42]);
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($params)->toBe([42])
            ;
        });

        it('preserves $n-style SQL with indexed params as-is', function (): void {
            [$sql, $params] = ParamParser::parse('SELECT * FROM t WHERE id = $1', [99]);
            expect($sql)->toBe('SELECT * FROM t WHERE id = $1')
                ->and($params)->toBe([99])
            ;
        });

        it('throws when mixing ? and $n in the same query', function (): void {
            expect(fn () => ParamParser::parse('WHERE a = ? AND b = $1', [1, 2]))
                ->toThrow(QueryException::class)
            ;
        });

        it('normalises a non-zero-indexed array to a flat indexed array', function (): void {
            [$sql, $params] = ParamParser::parse('SELECT * FROM t WHERE id = ?', [5 => 42]);
            expect($params)->toBe([42]);
        });

        it('converts :name placeholders and resolves associative params', function (): void {
            [$sql, $params] = ParamParser::parse(
                'SELECT * FROM users WHERE id = :id AND status = :status',
                ['id' => 7, 'status' => 'active']
            );
            expect($sql)->toBe('SELECT * FROM users WHERE id = $1 AND status = $2')
                ->and($params)->toBe([7, 'active'])
            ;
        });

        it('resolves params in placeholder order, not array key order', function (): void {
            [$sql, $params] = ParamParser::parse(
                'WHERE a = :first AND b = :second',
                ['second' => 'B', 'first' => 'A']
            );
            expect($params)->toBe(['A', 'B']);
        });

        it('deduplicates repeated :name and emits one value', function (): void {
            [$sql, $params] = ParamParser::parse(
                'WHERE first = :name OR last = :name',
                ['name' => 'Alice']
            );
            expect($sql)->toBe('WHERE first = $1 OR last = $1')
                ->and($params)->toBe(['Alice'])
            ;
        });

        it('throws when a named param is missing from the associative array', function (): void {
            expect(fn () => ParamParser::parse(
                'WHERE id = :id AND status = :status',
                ['id' => 1]
            ))->toThrow(QueryException::class, ':status');
        });

        it('throws when no :name placeholders are found but an associative array is provided', function (): void {
            expect(fn () => ParamParser::parse('SELECT * FROM t WHERE id = $1', ['id' => 1]))
                ->toThrow(QueryException::class)
            ;
        });

        it('passes non-boolean values through unchanged', function (): void {
            [$sql, $params] = ParamParser::parse('WHERE id = ?', ['hello']);
            expect($params)->toBe(['hello']);
        });

        it('throws when an indexed array is passed for a named-placeholder query', function (): void {
            // parse() must detect the SQL contains :name placeholders and reject
            // the indexed array rather than silently returning unconverted SQL
            expect(fn () => ParamParser::parse('SELECT * FROM t WHERE id = :id', [42]))
                ->toThrow(QueryException::class)
            ;
        });

        it('throws when an associative array is passed for a ? query', function (): void {
            expect(fn () => ParamParser::parse('SELECT * FROM t WHERE id = ?', ['id' => 42]))
                ->toThrow(QueryException::class)
            ;
        });

        it('throws when the query contains multiple statements', function (): void {
            expect(fn () => ParamParser::parse('SELECT :a; DROP TABLE users', ['a' => 1]))
                ->toThrow(QueryException::class);
        });

        it('does not throw for a semicolon inside a string literal', function (): void {
            [$sql, $params] = ParamParser::parse(
                "SELECT * FROM t WHERE note = ';not a statement' AND id = :id",
                ['id' => 1]
            );
            expect($sql)->toBe("SELECT * FROM t WHERE note = ';not a statement' AND id = \$1")
                ->and($params)->toBe([1]);
        });

        it('does not throw for a semicolon inside a line comment', function (): void {
            [$sql, $params] = ParamParser::parse(
                "SELECT * FROM t -- semicolon here; ignored\nWHERE id = :id",
                ['id' => 1]
            );
            expect($sql)->toBe("SELECT * FROM t -- semicolon here; ignored\nWHERE id = \$1")
                ->and($params)->toBe([1]);
        });
    });
});
