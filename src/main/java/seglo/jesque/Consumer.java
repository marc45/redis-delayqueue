package seglo.jesque;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import java.util.Arrays;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

public class Consumer {
    public static void main(String[] args) {
        // Configuration
        final Config config = new ConfigBuilder().withHost("ubu-vm").withNamespace("delay-queue").build();

        // Start a worker to run jobs from the "TestAction" delayed queue
        final Worker worker = new WorkerImpl(config,
                Arrays.asList(TestAction.Name), new MapBasedJobFactory(map(entry(TestAction.Name, TestAction.class))));

        final Thread workerThread = new Thread(worker);
        workerThread.start();

        try {
            while (true) { }
        } finally {
            worker.end(true);
            try { workerThread.join(); } catch (Exception e){ e.printStackTrace(); }
        }
    }
}

