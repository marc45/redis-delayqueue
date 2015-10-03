package seglo.jedis;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;
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

        List<String> keys = new ArrayList<String>();
        keys.addAll(setResponse.get());
        String[] keyArray = keys.toArray(new String[keys.size()]);

        if (keyArray.length > 0) {
            Transaction tMessage = jedis.multi();
            Response<List<String>> messageResponse = tMessage.mget(keyArray);
            tMessage.del(keyArray);
            tMessage.exec();

            List<String> messages = messageResponse.get();

            for (int i = 0; i < messages.size(); i++) {
                String key = keys.get(i);
                String message = messages.get(i);

                System.out.print("Received key: " + key + ". ");

                if (message == null) {
                    System.out.println("Message for key " + key + " is gone!");
                } else {
                    System.out.println("Message for key " + key + " is " + message);
                }
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
