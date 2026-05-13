<?php

declare(strict_types=1);

use function Hibla\await;
use function Hibla\delay;

describe('Pub/Sub test using postgres sql', function () {
    it('can intercept raw notifications on a low-level Connection', function () {
        $conn1 = pgConn();
        $conn2 = pgConn();

        $received = [];

        $conn1->setNotificationCallback(function (string $channel, string $payload, int $pid) use (&$received) {
            $received[] = [
                'channel' => $channel,
                'payload' => $payload,
                'pid' => $pid,
            ];
        });

        $conn1->setIsListening(true);

        await($conn1->query('LISTEN "raw_test_channel"'));

        await($conn2->query("NOTIFY \"raw_test_channel\", 'hello from raw'"));

        await(delay(0.05));

        expect($received)->toHaveCount(1)
            ->and($received[0]['channel'])->toBe('raw_test_channel')
            ->and($received[0]['payload'])->toBe('hello from raw')
            ->and($received[0]['pid'])->toBeGreaterThan(0)
        ;

        $conn1->close();
        $conn2->close();
    });

    it('can subscribe to a channel and receive notifications via PostgresClient', function () {
        $client = makeClient();

        $listener = await($client->createListener());

        $events = [];

        await($listener->listen('client_events', function (string $channel, string $payload) use (&$events) {
            $events[] = $payload;
        }));

        await($client->notify('client_events', 'payload 1'));
        await($client->notify('client_events', 'payload 2'));

        await(delay(0.05));

        expect($events)->toHaveCount(2)
            ->and($events[0])->toBe('payload 1')
            ->and($events[1])->toBe('payload 2')
        ;

        await($listener->close());
        $client->close();
    });

    it('can multiplex multiple channels on a single PostgresListener', function () {
        $client = makeClient();
        $listener = await($client->createListener());

        $received = [];

        await($listener->listen('channel_a', function (string $channel, string $payload) use (&$received) {
            $received[$channel] = $payload;
        }));

        await($listener->listen('channel_b', function (string $channel, string $payload) use (&$received) {
            $received[$channel] = $payload;
        }));

        await($client->notify('channel_a', 'message a'));
        await($client->notify('channel_b', 'message b'));

        await(delay(0.05));

        expect($received)->toHaveCount(2)
            ->and($received)->toHaveKey('channel_a', 'message a')
            ->and($received)->toHaveKey('channel_b', 'message b')
        ;

        await($listener->close());
        $client->close();
    });

    it('stops receiving notifications after unlisten is called', function () {
        $client = makeClient();
        $listener = await($client->createListener());

        $callCount = 0;

        await($listener->listen('toggle_channel', function () use (&$callCount) {
            $callCount++;
        }));

        await($client->notify('toggle_channel'));
        await(delay(0.05));
        expect($callCount)->toBe(1);

        await($listener->unlisten('toggle_channel'));

        await($client->notify('toggle_channel'));
        await(delay(0.05));

        expect($callCount)->toBe(1);

        await($listener->close());
        $client->close();
    });

    it('safely handles special characters and escapes channel names', function () {
        $client = makeClient();
        $listener = await($client->createListener());

        $receivedChannel = null;
        $weirdChannelName = 'my "weird" channel; DROP TABLE users;';

        await($listener->listen($weirdChannelName, function (string $channel) use (&$receivedChannel) {
            $receivedChannel = $channel;
        }));

        await($client->notify($weirdChannelName, 'safe payload'));
        await(delay(0.05));

        expect($receivedChannel)->toBe($weirdChannelName);

        await($listener->close());
        $client->close();
    });

    it('auto-cleans up resources when the listener is closed', function () {
        $client = makeClient();
        $listener = await($client->createListener());

        $callCount = 0;
        await($listener->listen('closing_channel', function () use (&$callCount) {
            $callCount++;
        }));

        await($listener->close());

        await($client->notify('closing_channel'));
        await(delay(0.05));

        expect($callCount)->toBe(0);

        $client->close();
    });
});
