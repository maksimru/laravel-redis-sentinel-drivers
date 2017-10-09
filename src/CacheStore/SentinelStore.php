<?php

namespace Monospice\LaravelRedisSentinel\CacheStore;

use Illuminate\Cache\RedisStore;

class SentinelStore extends RedisStore
{
    /**
     * Store an item in the cache if the key doesn't exist.
     *
     * @param  string $key
     * @param  mixed $value
     * @param  float|int $minutes
     * @return bool
     */
    public function add($key, $value, $minutes)
    {
        $lua = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

        if ($this->connection()->getOptions()->defined('replication'))
            $this->connection()->getOptions()->replication->setScriptReadOnly($lua, false);

        return (bool)$this->connection()->eval(
            $lua, 1, $this->prefix . $key, $this->serialize($value), (int)max(1, $minutes * 60)
        );
    }
}
