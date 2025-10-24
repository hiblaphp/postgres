<?php

use Hibla\Postgres\PG;

require 'vendor/autoload.php';

PG::init([
    'host' => 'localhost',
    'port' => 5432,
    'database' => 'test',
    'username' => 'postgres',
    'password' => 'root',
]);

$res = PG::query('SELECT * FROM users')->await();
var_dump($res);
