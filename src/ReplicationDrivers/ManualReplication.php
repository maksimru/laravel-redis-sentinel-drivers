<?php

namespace Monospice\LaravelRedisSentinel\ReplicationDrivers;

use Predis\Command\CommandInterface;
use Predis\Connection\Aggregate\SentinelReplication;
use Predis\Connection\NodeConnectionInterface;

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

    /**
     * {@inheritdoc}
     */
    public function writeRequest(CommandInterface $command)
    {
        $this->retryCommandOnFailure($command, __FUNCTION__);
    }

    /**
     * {@inheritdoc}
     */
    public function readResponse(CommandInterface $command)
    {
        return $this->retryCommandOnFailure($command, __FUNCTION__);
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        return $this->retryCommandOnFailure($command, __FUNCTION__);
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

    /**
     * Returns the connection instance in charge for the given command.
     *
     * @param CommandInterface $command Command instance.
     *
     * @return NodeConnectionInterface
     */
    private function getConnectionInternal(CommandInterface $command)
    {
        if (!$this->current) {
            if ($this->strategy->isReadOperation($command) && $slave = $this->pickSlave()) {
                $this->current = $slave;
            } else {
                $this->current = $this->getMaster();
            }

            return $this->current;
        }

        if ($this->current === $this->master) {
            return $this->current;
        }

        if (!$this->strategy->isReadOperation($command)) {
            $this->current = $this->getMaster();
        }

        return $this->current;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnection(CommandInterface $command)
    {
        return $this->getConnectionInternal($command);
    }

    public function setScriptReadOnly($script, $readonly = true)
    {
        $this->strategy->setScriptReadOnly($script, $readonly);
    }
}