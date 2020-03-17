<?php
namespace Groowth\Worker;

use PhpAmqpLib\Message\AMQPMessage as Message;


class ExampleWorker extends AbstractWorker
{
    public function boot()
    {
        
    }
    
    public function process(Message $message)
    {
        $id = (int) trim($message->getBody());
        
        if($id <= 1000)
        {
            var_dump($id);
            return $id;
        }
        
        return false;
    }
}
