<?php
/**
 * User: jiaozi<jiaozi@iyenei.com>
 * Date: 2018/7/31
 * Time: 23:17
 */

use redisq\RedisQueue;

require_once __DIR__ . "/vendor/autoload.php";

$redis = new Redis();
$redis->connect("121.199.182.2", 30005, 10);
$redis->auth("y2Xs0fAZ8WHWQj0V");

$queue = new RedisQueue($redis, "testQueue");
$queue->publish("hello");


