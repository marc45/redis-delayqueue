package seglo.jedis;

import redis.clients.jedis.*;

import java.util.Set;

public class Consumer {
    public static void main(String[] args) throws InterruptedException {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 2000);
        Jedis jedis = pool.getResource();

        try {
            while(true) {
                getMessages(jedis, Producer.QueueName);
                Thread.sleep(1000L);
            }
        } finally {
            jedis.close();
            pool.destroy();
        }
    }

    private static void getMessages(Jedis jedis, String queue) {
        int startTime = 0;
        long endTime = System.currentTimeMillis() / 1000;

        Transaction t = jedis.multi();
        Response<Set<String>> setResponse = t.zrangeByScore(queue, startTime, endTime);
        t.zremrangeByScore(queue, startTime, endTime);
        t.exec();

        Set<String> response = setResponse.get();
        for (String k : response) {
            System.out.print("Received key: " + k + ". ");

            Transaction tMessage = jedis.multi();
            Response<String> messageResponse = tMessage.get(k);
            tMessage.del(k);
            tMessage.exec();

            String message = messageResponse.get();
            if (message == null) {
                System.out.println("Message for key " + k + " is gone!");
            } else {
                System.out.println("Message for key " + k + " is " + message);
            }
        }
    }
//    def getMessages(String queue) {
//        def startTime = 0
//        def endTime = System.currentTimeMillis() / 1000
//
//        redisService.withRedis { Jedis redis ->
//            def t = redis.multi()
//            def response = t.zrangeByScore(queue, startTime, endTime)
//            t.zremrangeByScore(queue, startTime, endTime)
//            t.exec()
//            response.get()
//        }
//    }

}
