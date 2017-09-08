<?php

namespace Monospice\LaravelRedisSentinel\ReplicationDrivers;

use Predis\Connection\Aggregate\SentinelReplication;

class ManualReplication extends SentinelReplication
{

    private function retryCommandOnFailure(CommandInterface $command, $method)
    {
        $retries = 0;

        SENTINEL_RETRY: {
            try {
                $response = $this->getConnection($command)->$method($command);
            } catch (CommunicationException $exception) {
                $exception->getConnection()->disconnect();

                if ($retries == $this->retryLimit) {
                    throw $exception;
                }

                usleep($this->retryWait * 1000);

                ++$retries;
                goto SENTINEL_RETRY;
            }
        }

        return $response;
    }

    public function setMaster($master){
        $this->master = null;
        $this->add($this->connectionFactory->create(array_merge($master,['alias' => 'master'])));
    }

    public function setSlaves($slaves){
        $this->slaves = [];
        if(!is_array($slaves))
            $slaves = [$slaves];
        foreach($slaves as $index => $slave){
            $this->add($this->connectionFactory->create(array_merge($slave,['alias' => 'slave-'.$index])));
        }
    }

    public function getMaster()
    {
        return $this->master;
    }

    public function getSlaves()
    {
        if ($this->slaves) {
            return array_values($this->slaves);
        }
    }

}