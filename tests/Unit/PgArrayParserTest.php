<?php

declare(strict_types=1);

use Hibla\Postgres\Internals\PgArrayParser;

it(
    'returns empty array for empty string',
    fn () => expect(PgArrayParser::parse(''))->toBe([])
);

it(
    'returns empty array for whitespace-only input',
    fn () => expect(PgArrayParser::parse('   '))->toBe([])
);

it('returns empty array for non-array input', function () {
    expect(PgArrayParser::parse('hello'))->toBe([])
        ->and(PgArrayParser::parse('123'))->toBe([])
    ;
});

it(
    'returns empty array for empty array literal',
    fn () => expect(PgArrayParser::parse('{}'))->toBe([])
);

dataset('int arrays', [
    'single element' => ['{1}',          [1]],
    'multiple elements' => ['{1,2,3}',      [1, 2, 3]],
    'negative values' => ['{-1,-2,3}',    [-1, -2, 3]],
    'with whitespace' => ['  {1,2,3}  ',  [1, 2, 3]],
    'zeros' => ['{0,0,0}',      [0, 0, 0]],
    'large integer' => ['{2147483647}', [2147483647]],
]);

it('parses int arrays', function (string $input, array $expected) {
    expect(PgArrayParser::parse($input, 'int'))->toBe($expected);
})->with('int arrays');

dataset('float arrays', [
    'basic floats' => ['{1.1,2.2,3.3}', [1.1, 2.2, 3.3]],
    'negative floats' => ['{-1.5,2.5}',    [-1.5, 2.5]],
    'scientific notation' => ['{1.5e2}',        [150.0]],
    'integer-like floats' => ['{1,2}',          [1.0, 2.0]],
]);

it('parses float arrays', function (string $input, array $expected) {
    expect(PgArrayParser::parse($input, 'float'))->toBe($expected);
})->with('float arrays');

dataset('bool arrays', [
    'all true' => ['{t,t,t}', [true, true, true]],
    'all false' => ['{f,f,f}', [false, false, false]],
    'mixed' => ['{t,f,t}', [true, false, true]],
    'single t' => ['{t}',     [true]],
    'single f' => ['{f}',     [false]],
]);

it('parses bool arrays', function (string $input, array $expected) {
    expect(PgArrayParser::parse($input, 'bool'))->toBe($expected);
})->with('bool arrays');

dataset('string arrays', [
    'basic strings' => ['{foo,bar,baz}',            ['foo', 'bar', 'baz']],
    'quoted strings' => ['{"foo","bar"}',             ['foo', 'bar']],
    'quoted with spaces' => ['{"hello world","foo bar"}', ['hello world', 'foo bar']],
    'quoted with comma inside' => ['{"a,b","c,d"}',             ['a,b', 'c,d']],
    'quoted with curly braces' => ['{"a{b}c","d{e}f"}',        ['a{b}c', 'd{e}f']],
    'mixed quoted/unquoted' => ['{foo,"bar baz",qux}',       ['foo', 'bar baz', 'qux']],
    'single element' => ['{hello}',                   ['hello']],
    'numbers as strings' => ['{1,2,3}',                   ['1', '2', '3']],
]);

it('parses string arrays', function (string $input, array $expected) {
    expect(PgArrayParser::parse($input, 'string'))->toBe($expected);
})->with('string arrays');

it('parses NULL elements as null (case-insensitive)', function () {
    expect(PgArrayParser::parse('{NULL,1,2}', 'int'))->toBe([null, 1, 2])
        ->and(PgArrayParser::parse('{null,1,2}', 'int'))->toBe([null, 1, 2])
        ->and(PgArrayParser::parse('{Null,1,2}', 'int'))->toBe([null, 1, 2])
    ;
});

it(
    'treats quoted NULL as a literal string, not null',
    fn () => expect(PgArrayParser::parse('{"NULL"}', 'string'))->toBe(['NULL'])
);

it(
    'parses array of all NULLs',
    fn () => expect(PgArrayParser::parse('{NULL,NULL,NULL}', 'string'))->toBe([null, null, null])
);

it(
    'parses NULL mixed with strings',
    fn () => expect(PgArrayParser::parse('{foo,NULL,bar}', 'string'))->toBe(['foo', null, 'bar'])
);

it(
    'parses NULL mixed with ints',
    fn () => expect(PgArrayParser::parse('{1,NULL,3}', 'int'))->toBe([1, null, 3])
);

it(
    'parses NULL at the start and end',
    fn () => expect(PgArrayParser::parse('{NULL,1,NULL}', 'int'))->toBe([null, 1, null])
);

it(
    'parses NULL mixed with booleans',
    fn () => expect(PgArrayParser::parse('{t,NULL,f}', 'bool'))->toBe([true, null, false])
);

it(
    'handles escaped double quotes inside quoted strings',
    fn () => expect(PgArrayParser::parse('{"say \\"hello\\""}', 'string'))->toBe(['say "hello"'])
);

it(
    'handles escaped backslashes',
    fn () => expect(PgArrayParser::parse('{"back\\\\slash"}', 'string'))->toBe(['back\\slash'])
);

it(
    'handles empty quoted string as empty string, not null',
    fn () => expect(PgArrayParser::parse('{""}', 'string'))->toBe([''])
);

it(
    'handles multiple empty quoted strings',
    fn () => expect(PgArrayParser::parse('{"","",""}', 'string'))->toBe(['', '', ''])
);

it(
    'handles empty quoted string mixed with real values',
    fn () => expect(PgArrayParser::parse('{foo,"",bar}', 'string'))->toBe(['foo', '', 'bar'])
);

it(
    'handles escaped quote at the start of a quoted string',
    fn () => expect(PgArrayParser::parse('{"\\"starts with quote"}', 'string'))->toBe(['"starts with quote'])
);

it(
    'handles escaped quote at the end of a quoted string',
    fn () => expect(PgArrayParser::parse('{"ends with quote\\""}', 'string'))->toBe(['ends with quote"'])
);

it(
    'handles tab character inside quoted strings',
    fn () => expect(PgArrayParser::parse("{\"foo\tbar\"}", 'string'))->toBe(["foo\tbar"])
);

it(
    'handles newline character inside quoted strings',
    fn () => expect(PgArrayParser::parse("{\"foo\nbar\"}", 'string'))->toBe(["foo\nbar"])
);

it(
    'handles quoted string that is only whitespace',
    fn () => expect(PgArrayParser::parse('{"   "}', 'string'))->toBe(['   '])
);

it(
    'parses 2D int array',
    fn () => expect(PgArrayParser::parse('{{1,2},{3,4}}', 'int'))->toBe([[1, 2], [3, 4]])
);

it(
    'parses 3D int array',
    fn () => expect(PgArrayParser::parse('{{{1,2},{3,4}},{{5,6},{7,8}}}', 'int'))
        ->toBe([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
);

it(
    'parses 4D int array',
    fn () => expect(PgArrayParser::parse('{{{{1,2}}}}', 'int'))->toBe([[[[1, 2]]]])
);

it(
    'parses 2D string array',
    fn () => expect(PgArrayParser::parse('{{foo,bar},{baz,qux}}', 'string'))
        ->toBe([['foo', 'bar'], ['baz', 'qux']])
);

it(
    'parses 2D array with NULLs',
    fn () => expect(PgArrayParser::parse('{{1,NULL},{NULL,4}}', 'int'))
        ->toBe([[1, null], [null, 4]])
);

it(
    'parses nested array with quoted strings',
    fn () => expect(PgArrayParser::parse('{{"hello world","foo,bar"},{"baz"}}', 'string'))
        ->toBe([['hello world', 'foo,bar'], ['baz']])
);

it(
    'parses empty nested array',
    fn () => expect(PgArrayParser::parse('{{}}', 'int'))->toBe([[]])
);

it(
    'parses multiple empty nested arrays',
    fn () => expect(PgArrayParser::parse('{{},{}}', 'int'))->toBe([[], []])
);

it(
    'parses nested array with all NULLs',
    fn () => expect(PgArrayParser::parse('{{NULL,NULL},{NULL,NULL}}', 'int'))
        ->toBe([[null, null], [null, null]])
);

it(
    'trims whitespace around unquoted string elements',
    fn () => expect(PgArrayParser::parse('{ foo , bar , baz }', 'string'))->toBe(['foo', 'bar', 'baz'])
);

it(
    'trims whitespace around unquoted int elements',
    fn () => expect(PgArrayParser::parse('{ 1 , 2 , 3 }', 'int'))->toBe([1, 2, 3])
);

it(
    'does not trim whitespace inside quoted strings',
    fn () => expect(PgArrayParser::parse('{"  foo  "," bar "}', 'string'))->toBe(['  foo  ', ' bar '])
);

it('only treats literal t as true for bool cast', function () {
    // Postgres outputs 't'/'f' — anything else is not 't', so it's false
    expect(PgArrayParser::parse('{true,false,yes,no,1,0}', 'bool'))
        ->toBe([false, false, false, false, false, false])
    ;
});

it(
    'parses Infinity as INF for float',
    fn () => expect(PgArrayParser::parse('{Infinity}', 'float'))->toBe([INF])
);

it(
    'parses negative Infinity as -INF for float',
    fn () => expect(PgArrayParser::parse('{-Infinity}', 'float'))->toBe([-INF])
);

it('parses NaN for float', function () {
    $result = PgArrayParser::parse('{NaN}', 'float');

    expect($result)->toHaveCount(1);
    expect(is_nan($result[0]))->toBeTrue();
});

it('returns raw strings for any unrecognised cast type', function () {
    // Callers are responsible for further casting (e.g. JSON decoding).
    // The parser always falls back to raw strings for unknown types.
    expect(PgArrayParser::parse('{1,2,3}', 'unknown'))->toBe(['1', '2', '3'])
        ->and(PgArrayParser::parse('{1,2,3}', 'numeric'))->toBe(['1', '2', '3']);
});

it('returns numeric and decimal values as raw strings to preserve precision', function () {
    // numeric/decimal are intentionally not cast to float or int.
    // PHP floats are IEEE 754 and cannot represent arbitrary-precision values
    // exactly. Callers that need precision arithmetic should use BCMath,
    // brick/math, or moneyphp/money on the raw string value.
    expect(PgArrayParser::parse('{0.1,0.2,0.3}', 'string'))
        ->toBe(['0.1', '0.2', '0.3'])
        ->and(PgArrayParser::parse('{99999999999999999.99}', 'string'))
        ->toBe(['99999999999999999.99'])
        ->and(PgArrayParser::parse('{0.30000000000000004}', 'string'))
        ->toBe(['0.30000000000000004']);
});

it(
    'defaults to string cast when no type is specified',
    fn () => expect(PgArrayParser::parse('{1,2,3}'))->toBe(['1', '2', '3'])
);

it(
    'returns empty array for a bare closing brace',
    fn () => expect(PgArrayParser::parse('}'))->toBe([])
);

it(
    'returns empty array for a bare opening brace with no closing',
    fn () =>
    expect(PgArrayParser::parse('{'))->toBe([])
);

it(
    'ignores trailing content after the closing brace',
    fn () =>
    expect(PgArrayParser::parse('{1,2,3}garbage', 'int'))->toBe([1, 2, 3])
);

it(
    'handles extra closing brace gracefully',
    fn () => expect(PgArrayParser::parse('{1,2,3}}', 'int'))->toBe([1, 2, 3])
);

it(
    'handles unicode characters in unquoted strings',
    fn () => expect(PgArrayParser::parse('{héllo,wörld}', 'string'))->toBe(['héllo', 'wörld'])
);

it(
    'handles unicode characters in quoted strings',
    fn () => expect(PgArrayParser::parse('{"héllo wörld","日本語"}', 'string'))
        ->toBe(['héllo wörld', '日本語'])
);

it(
    'handles emoji in quoted strings',
    fn () => expect(PgArrayParser::parse('{"hello 👋","world 🌍"}', 'string'))
        ->toBe(['hello 👋', 'world 🌍'])
);

it('parses a large flat int array correctly', function () {
    $values = range(1, 1000);
    $literal = '{' . implode(',', $values) . '}';

    expect(PgArrayParser::parse($literal, 'int'))->toBe($values);
});

it('parses a large flat string array correctly', function () {
    $values = array_map(fn ($i) => "item$i", range(1, 500));
    $literal = '{' . implode(',', $values) . '}';

    expect(PgArrayParser::parse($literal, 'string'))->toBe($values);
});

it('handles adjacent commas by treating them as null', function () {
    expect(PgArrayParser::parse('{1,,3}', 'int'))->toBe([1, null, 3]);
});

it('handles mismatched nested braces gracefully', function () {
    expect(PgArrayParser::parse('{{1,2,3}', 'int'))->toBe([[1, 2, 3]]);
});

it('returns empty array for mid-word quote (invalid postgres syntax)', function () {
    // A quote opened inside an unquoted token is invalid; the parser
    // enters quote mode and never finds a closing boundary.
    expect(PgArrayParser::parse('{part"ially,quoted}', 'string'))->toBe([]);
});

it('handles bytea escaped data inside an array', function () {
    // Postgres doubles backslashes in array literals: {\\xDEADBEEF} → \xDEADBEEF
    expect(PgArrayParser::parse('{\\\\xDEADBEEF}', 'string'))->toBe(['\\xDEADBEEF']);
});

it('parses an array of empty nested arrays', function () {
    expect(PgArrayParser::parse('{{},{},{}}'))->toBe([[], [], []]);
});

it('maintains precision for large numbers', function () {
    $largeInt = '9223372036854775807';
    expect(PgArrayParser::parse('{' . $largeInt . '}', 'int')[0])
        ->toBe(PHP_INT_MAX);

    // PHP float (IEEE 754 double) gives ~14-15 significant digits;
    // precision beyond that is a PHP limitation, not a parser concern.
    $float = '3.14159265358979';
    expect(PgArrayParser::parse('{' . $float . '}', 'float')[0])
        ->toBe((float) $float);
});

it('handles reasonably deep nesting without crashing', function () {
    $deep = str_repeat('{', 100) . '1' . str_repeat('}', 100);
    $result = PgArrayParser::parse($deep, 'int');
    for ($i = 0; $i < 100; $i++) {
        $result = $result[0];
    }
    expect($result)->toBe(1);
});
