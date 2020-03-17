<?php
namespace Groowth\Worker;

use Commando\Command;
use PhpAmqpLib\Message\AMQPMessage as Message;
use PhpAmqpLib\Connection\AMQPStreamConnection as Connection;


abstract class AbstractWorker
{
    private $channel;
    private $connection;
    private $queueName;
    private $exchangeName;
    private $outboundRoutingKey;
    private $bindingKeys = array();
    
    abstract public function process(Message $message);
    
    static public function console(Connection $connection)
    {
        $cli = new Command;
        
        $cli->option('queue')->aka('q')
            ->describedAs('The name of the queue to consume from')
            ->required();
            
        $cli->option('key')->aka('k')
            ->describedAs('The routing key to use for outbound messages')
            ->required();
            
        $cli->option('bindings')->aka('b')
            ->describedAs('Binding keys to use (separate with pipe for multiple)')
            ->map(function($val) {
                return array_map('trim', explode('|', $val));
            })
            ->defaultsTo('#');
            
        $cli->option('exchange')->aka('e')
            ->describedAs('The name of the exchange to use')
            ->required();
        
        $class = get_called_class();
        $worker = new $class($connection);
        
        foreach($cli['bindings'] as $bindingKey) {
            $worker->addBindingKey($bindingKey);
        }
        
        $worker->setQueueName($cli['queue']);
        $worker->setExchangeName($cli['exchange']);
        $worker->setOutboundRoutingKey($cli['key']);
        
        $worker->work();
    }
    
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        
        if(method_exists($this, 'boot'))
            $this->boot();
    }
    
    public function addBindingKey($key)
    {
        $this->bindingKeys[] = $key;
        return $this;
    }
    
    public function setOutboundRoutingKey($key)
    {
        $this->outboundRoutingKey = $key;
        return $this;
    }
    
    public function setQueueName($name)
    {
        $this->queueName = $name;
        return $this;
    }
    
    public function setExchangeName($name)
    {
        $this->exchangeName = $name;
        return $this;
    }
    
    public function work()
    {
        $this->connect();
        
        while($this->channel->is_consuming())
        {
            $this->channel->wait();
        }
    }

    public function handler(Message $message)
    {
        if($result = $this->process($message))
        {
            $this->channel->basic_ack($message->delivery_info['delivery_tag']);
            
            if(is_object($result) || is_array($result))
                $result = serialize($result);
            
            $message = new Message($result);
            $this->channel->basic_publish($message, $this->exchangeName, $this->outboundRoutingKey);
        }
        else
        {
            $this->channel->basic_nack($message->delivery_info['delivery_tag']);
        }
    }
    
    private function connect()
    {
        $exchangeName = $this->exchangeName;
        
        $this->channel = $this->connection->channel();
        $this->channel->exchange_declare($exchangeName, 'topic', false, false, false);
        
        //list($this->queueName, ,) = $this->channel->queue_declare("", false, true, true, false);
        $this->channel->queue_declare($this->queueName, false, true, true, false);
        
        foreach($this->bindingKeys as $bindingKey)
        {
            $this->channel->queue_bind($this->queueName, $exchangeName, $bindingKey);
        }
        
        $this->channel->basic_consume($this->queueName, '', false, false, false, false, array($this, 'handler'));
    }
}

