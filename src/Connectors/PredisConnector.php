<?php

namespace Monospice\LaravelRedisSentinel\Connectors;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Arr;
use Monospice\SpicyIdentifiers\DynamicMethod;
use Predis\Client;

/**
 * Initializes Predis Client instances for Redis Sentinel connections
 *
 * @category Package
 * @package  Monospice\LaravelRedisSentinel
 * @author   Cy Rossignol <cy@rossignols.me>
 * @license  See LICENSE file
 * @link     http://github.com/monospice/laravel-redis-sentinel-drivers
 */
class PredisConnector
{
    /**
     * Configuration options specific to Sentinel connection operation
     *
     * We cannot pass these options as an array to the Predis client.
     * Instead, we'll set them on the connection directly using methods
     * provided by the SentinelReplication class of the Predis package.
     *
     * @var array
     */
    protected $sentinelConnectionOptionKeys = [
        'sentinel_timeout',
        'retry_wait',
        'retry_limit',
        'update_sentinels',
    ];

    protected $specialOptionKeys = [
        'master',
        'slaves'
    ];

    /**
     * Create a new Redis Sentinel connection from the provided configuration
     * options
     *
     * @param array $server  The client configuration for the connection
     * @param array $options The global client options shared by all Sentinel
     * connections
     *
     * @return PredisConnection The Sentinel connection containing a configured
     * Predis Client
     */
    public function connect(array $server, array $options = [ ])
    {
        // Merge the global options shared by all Sentinel connections with
        // connection-specific options
        $clientOpts = array_merge($options, Arr::pull($server, 'options', [ ]));

        // Automatically set "replication" to "sentinel". This is the Sentinel
        // driver, after all.
        $clientOpts['replication'] = 'sentinel';

        // Extract the array of Sentinel connection options from the rest of
        // the client options
        $sentinelKeys = array_flip($this->sentinelConnectionOptionKeys);
        $sentinelOpts = array_intersect_key($clientOpts, $sentinelKeys);

        // Extract the array of special options from the rest of
        // the client options
        $specialKeys = array_flip($this->specialOptionKeys);
        $specialOpts = array_intersect_key($clientOpts, $specialKeys);

        // Filter the Sentinel connection options elements from the client
        // options array
        $clientOpts = array_diff_key($clientOpts, $sentinelKeys, $specialKeys);

        return new PredisConnection(
            $this->makePredisClient($server, $clientOpts, $sentinelOpts, $specialOpts)
        );
    }

    /**
     * Create a Predis Client instance configured with the provided options
     *
     * @param array $server       The client configuration for the connection
     * @param array $clientOpts   Non-sentinel client options
     * @param array $sentinelOpts Sentinel-specific options
     * @param array $specialOpts  Special options
     *
     * @return Client The Predis Client configured for Sentinel connections
     */
    protected function makePredisClient(
        array $server,
        array $clientOpts,
        array $sentinelOpts,
        array $specialOpts
    ) {
        $client = new Client($server, $clientOpts);
        $connection = $client->getConnection();

        // Set the Sentinel-specific connection options on the Predis Client
        // connection
        foreach ($sentinelOpts as $option => $value) {
            DynamicMethod::parseFromUnderscore($option)
                ->prepend('set')
                ->callOn($connection, [ $value ]);
        }

        // allow to reload master / slave configuration, useful with remote sentinel cluster
        if(isset($specialOpts['master']) || isset($specialOpts['slaves'])){
            $reflection = new \ReflectionObject($connection);
            $connectionFactoryProperty = $reflection->getProperty('connectionFactory');
            $connectionFactoryProperty->setAccessible(true);
            $connectionFactory = $connectionFactoryProperty->getValue($connection);
        }

        if(isset($specialOpts['master'])){
            $masterProperty = $reflection->getProperty('master');
            $masterProperty->setAccessible(true);
            $masterProperty->setValue($connection, null);
            $connection->add($connectionFactory->create(array_merge($specialOpts['master'],['alias' => 'master'])));
        }

        if(isset($specialOpts['slaves'])){
            $masterProperty = $reflection->getProperty('slaves');
            $masterProperty->setAccessible(true);
            $masterProperty->setValue($connection, []);
            if(!is_array($specialOpts['slaves']))
                $specialOpts['slaves'] = [$specialOpts['slaves']];
            foreach($specialOpts['slaves'] as $index => $slave){
                $connection->add($connectionFactory->create(array_merge($slave,['alias' => 'slave-'.$index])));
            }
        }

        return $client;
    }
}
