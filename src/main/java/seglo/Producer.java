package seglo;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;

import java.util.Arrays;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        // Configuration
        final Config config = new ConfigBuilder().withHost("ubu-vm").build();

        final Client client = new ClientImpl(config);

        try {

            int count = 0;
            while (true) {

                final long delay = 5; // in seconds
                final long future = System.currentTimeMillis() + (delay * 1000); // timestamp

                int id = count;

                System.out.println("Queuing msg " + id + " for " + delay + " seconds");

                final Job job = new Job(TestAction.Name,
                        new Object[]{id, 2.3, true, "test", Arrays.asList("inner", 4.5)});
                client.delayedEnqueue(TestAction.Name, job, future);

                // delete every 5th Action
                if (count != 0 && count%5==0) {
                    System.out.println("Deleting msg with id " + id);
                    Job jobToRemove = new Job(TestAction.Name, new Object[]{id, 2.3, true, "test", Arrays.asList("inner", 4.5)});
                    client.removeDelayedEnqueue(TestAction.Name, jobToRemove);
                }

                count += 1;

                Thread.sleep(3000L);
            }
        } finally {
            client.end();
        }
    }
}

