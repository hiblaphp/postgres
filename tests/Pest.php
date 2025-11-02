<?php

declare(strict_types=1);

afterEach(function () {
    Mockery::close();

    foreach (array_keys($GLOBALS) as $key) {
        if (str_starts_with($key, 'mock_')) {
            unset($GLOBALS[$key]);
        }
    }
});
