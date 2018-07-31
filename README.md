# redis-queue
一个Redis queue工具, 封装常用的队列相关操作, 简化队列的使用. 

### 特性(FEATURES)

* 封装数据的序列化与反序列化
* 封装信号量处理, 便于代码更新时平滑重启
* 重试机制

### 代码

守护进程端

```php

$redis = new Redis();
$redis->connect("127.0.0.1", 6379, 30);
$redis->auth("xxx");

$queue = new RedisQueue($redis, "testQueue");
$queue->consume(function($data){
    var_dump($data);
});

```

以上代码中, consume调用平时不会退出, 除非有信号量进入. 有消息进入时, 回调函数将被自动调用. 

投递一个消息. 

```

$redis = new Redis();
$redis->connect("127.0.0.1", 6379, 30);
$redis->auth("xxx");

$queue = new RedisQueue($redis, "testQueue");
$queue->publish("hello");


```
