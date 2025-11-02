<?php

declare(strict_types=1);

use Hibla\Postgres\Exceptions\QueryException;
use Hibla\Postgres\Utilities\QueryExecutor;

/**
 * Helper function to invoke private convertPlaceholders method
 */
function invokeConvertPlaceholders(string $sql): string
{
    $executor = new QueryExecutor();
    $reflection = new ReflectionClass(QueryExecutor::class);
    $method = $reflection->getMethod('convertPlaceholders');
    $method->setAccessible(true);

    return $method->invoke($executor, $sql);
}

describe('convertPlaceholders', function () {
    it('converts question mark placeholders to dollar notation', function () {
        $sql = 'SELECT * FROM users WHERE id = ? AND name = ?';
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe('SELECT * FROM users WHERE id = $1 AND name = $2');
    });

    it('leaves dollar notation placeholders unchanged', function () {
        $sql = 'SELECT * FROM users WHERE id = $1 AND name = $2';
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe('SELECT * FROM users WHERE id = $1 AND name = $2');
    });

    it('returns SQL unchanged when no placeholders exist', function () {
        $sql = 'SELECT * FROM users';
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe('SELECT * FROM users');
    });

    it('handles multiple question marks in complex queries', function () {
        $sql = 'SELECT * FROM users WHERE id = ? AND status = ? AND created_at > ?';
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe('SELECT * FROM users WHERE id = $1 AND status = $2 AND created_at > $3');
    });

    it('throws exception when mixing placeholder formats', function () {
        $sql = 'SELECT * FROM users WHERE id = ? AND name = $2';

        expect(fn () => invokeConvertPlaceholders($sql))
            ->toThrow(QueryException::class, 'Cannot mix ? and $n placeholder formats in the same query')
        ;
    });

    it('handles question marks in single quotes (string literals)', function () {
        $sql = "SELECT * FROM users WHERE message = 'What?' AND id = ?";
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe("SELECT * FROM users WHERE message = 'What?' AND id = \$1");
    });

    it('handles escaped quotes in string literals', function () {
        $sql = "SELECT * FROM users WHERE name = 'O''Brien' AND id = ?";
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe("SELECT * FROM users WHERE name = 'O''Brien' AND id = \$1");
    });

    it('handles multiple strings with question marks', function () {
        $sql = "SELECT * FROM posts WHERE title = 'Why?' AND body = 'How?' AND user_id = ?";
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe("SELECT * FROM posts WHERE title = 'Why?' AND body = 'How?' AND user_id = \$1");
    });

    it('handles mixed placeholders and string literals', function () {
        $sql = "INSERT INTO messages (text, user_id, status) VALUES ('Hello?', ?, ?)";
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe("INSERT INTO messages (text, user_id, status) VALUES ('Hello?', \$1, \$2)");
    });

    it('converts single question mark placeholder', function () {
        $sql = 'SELECT * FROM users WHERE id = ?';
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe('SELECT * FROM users WHERE id = $1');
    });

    it('handles empty SQL string', function () {
        $sql = '';
        $result = invokeConvertPlaceholders($sql);

        expect($result)->toBe('');
    });
});
