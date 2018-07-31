<?php
/**
 * User: jiaozi<jiaozi@iyenei.com>
 * Date: 2018/7/31
 * Time: 22:50
 */

namespace redisq;

use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LoggerInterface;

class RedisQueue
{
    const CALLBACK_RESULT_REQUEUE = 1;
    const CALLBACK_RESULT_STOP = 2;

    private $logger;
    private $timeout = 10;
    private $queueKey;
    private $stop = false;
    private $maxDelivery = 1;

    /**
     * @var \Redis
     */
    private $redis;

    /**
     * RedisQueue constructor.
     * @param \Redis $redis 一个连接好的Redis实例
     * @param string $queueKey 队列Key
     */
    public function __construct(\Redis $redis, $queueKey)
    {
        $this->redis = $redis;
        $this->queueKey = $queueKey;
    }

    /**
     * 设置一个Logger用于接收日志输出
     *
     * @param LoggerInterface $logger
     * @return $this
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
        return $this;
    }

    /**
     * 创建一个默认Logger, 默认输出到标准输出
     *
     * @return Logger
     * @throws \Exception
     */
    protected function createDefaultLogger()
    {
        $logger = new Logger('redisq');
        $logger->pushHandler(new StreamHandler("php://stdout", Logger::INFO));
        return $logger;
    }

    /**
     * 取得一个Logger对象
     *
     * @return AbstractLogger
     * @throws \Exception
     */
    public function getLogger()
    {
        if ($this->logger == null) {
            $this->setLogger($this->createDefaultLogger());
        }

        return $this->logger;
    }

    /**
     * 入队
     *
     * @param mixed $data 需要入队的数据
     * @param string $taskName 任务名称, 如果没有设置任务名, 将自动生成一个
     * @return $this
     */
    public function publish($data, $taskName = '')
    {
        static $taskNameIndex = 0;

        // 如果没有指定一个任务编号，生成一个自增编号作为任务编号
        if (empty($taskName)) {
            $taskName = date('YmdHis') . '_' . ($taskNameIndex ++);
        }

        $queueItem = [
            'name' => $taskName,
            'body' => $data,
            'meta' => [
                'publishedAt' => date('Y-m-d H:i:s'),
                'retry' => 0,
                'delivery' => 0,            // 投递次数
            ]
        ];

        $this->push($queueItem);
        return $this;
    }

    private function push($queueItem)
    {
        $this->redis->rPush($this->queueKey, serialize($queueItem));
    }

    /**
     * 消费
     *
     * @param callable $callback
     * @return mixed
     * @throws \Exception
     */
    public function consume(callable $callback)
    {
        $onSignal = function($sig){
            $this->getLogger()->info("收到信号: {$sig}, 准备退出!");
            $this->stop = true;
        };

        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGTERM, $onSignal);
            pcntl_signal(SIGHUP, $onSignal);
            pcntl_signal(SIGQUIT, $onSignal);
            pcntl_signal(SIGINT, $onSignal);
        }

        $signalDispatch = function_exists('pcntl_signal_dispatch') ? function(){pcntl_signal_dispatch();}: function(){};

        $this->getLogger()->info("开始消费队列, queueName={$this->queueKey}");
        while (!$this->stop) {
            $pop = $this->redis->blPop([$this->queueKey], $this->timeout);
            if (!empty($pop)) {
                $data = unserialize($pop[1]);

                $this->getLogger()->info("取得任务: {$data['name']}, 进入处理");
                $callbackRet = (int)call_user_func($callback, $data['body'], $data['meta'], $data['name']);
                $this->getLogger()->info("任务回调结束, Ret={$callbackRet}");

                $data['meta']['delivery'] += 1;

                switch ($callbackRet) {
                    case self::CALLBACK_RESULT_REQUEUE:
                        // 回调希望重新入队
                        if ($data['meta']['delivery'] < $this->maxDelivery) {
                            $data['meta']['retry'] += 1;
                            $this->getLogger()->info("重新入队, TaskName={$data['name']}");
                            $this->push($data);
                        } else {
                            $this->getLogger()->warning("任务达到最大投递次数{$this->maxDelivery}, 放弃重试. ", $data);
                        }

                        break;
                    case self::CALLBACK_RESULT_STOP:
                        // 回调希望中止队列运行
                        $this->getLogger()->info("中止队列循环, TaskName={$data['name']}");
                        $this->stop = true;
                        break;
                    default:
                        // 其它返回值, 忽略
                        break;
                }

                // 回收内存
                gc_collect_cycles();
                if (function_exists('gc_mem_caches')) {
                    gc_mem_caches();
                }
            }

            $signalDispatch();
        }

        $this->getLogger()->info("队列退出");
    }

    /**
     * 设置任务的最大重投次数
     * @return int
     */
    public function getMaxDelivery(): int
    {
        return $this->maxDelivery;
    }

    /**
     * 设置任务的最大重投次数.
     * @param int $maxDelivery
     */
    public function setMaxDelivery(int $maxDelivery)
    {
        $this->maxDelivery = $maxDelivery;
    }
}