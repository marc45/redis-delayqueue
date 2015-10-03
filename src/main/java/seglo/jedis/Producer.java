package seglo.jedis;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;
import seglo.jesque.TestAction;

import java.util.Arrays;

public class Producer {
    static final String QueueName = "delay-queue";

    public static void main(String[] args) throws InterruptedException {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 2000);
        Jedis jedis = pool.getResource();

        try {
            int count = 0;
            while(true) {
                String message = "Message #" + count;
                String key = "foobar:" + count;
                System.out.println("Queueing message: "+ message);
                queueMessage(jedis, QueueName, key, message, 5);

                // delete every 5th Action
                if (count != 0 && count%5==0) {
                    System.out.println("Deleting msg with id " + count);
                    jedis.del(key);
                }

                count += 1;
                Thread.sleep(3000L);
            }
        } finally {
            jedis.close();
            pool.destroy();
        }
    }

    private static void queueMessage(Jedis jedis, String queue, String key, String message, Integer delay) {
        long time = System.currentTimeMillis()/1000 + delay;

        Transaction t = jedis.multi();
        t.zadd(queue, time, key);
        t.set(key, message);
        t.exec();
    }

//    def queueMessage(String queue, String message, Integer delay) {
//        def time = System.currentTimeMillis()/1000 + delay
//
//        redisService.withRedis { Jedis redis ->
//            redis.zadd(queue, time, message)
//        }
//    }


}

