import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates generating Snowflake IDs concurrently from multiple
 * simulated nodes, then reports throughput/latency and checks for
 * collisions across the whole batch.
 */
public class SnowflakeDemo {

    public static void main(String[] args) throws InterruptedException {
        int nodeCount = 4;
        int idsPerNode = 250_000;

        SnowflakeIdGenerator[] generators = new SnowflakeIdGenerator[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            generators[i] = new SnowflakeIdGenerator(i);
        }

        int totalIds = nodeCount * idsPerNode;
        Set<Long> allIds = ConcurrentHashMap.newKeySet(totalIds);
        ExecutorService pool = Executors.newFixedThreadPool(nodeCount);

        long start = System.nanoTime();

        for (int i = 0; i < nodeCount; i++) {
            SnowflakeIdGenerator generator = generators[i];
            pool.submit(() -> {
                for (int j = 0; j < idsPerNode; j++) {
                    allIds.add(generator.nextId());
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);

        long elapsedNanos = System.nanoTime() - start;

        System.out.printf("Generated %,d IDs across %d nodes in %.2f ms%n",
                totalIds, nodeCount, elapsedNanos / 1_000_000.0);
        System.out.printf("Average latency per ID: %.1f ns%n", (double) elapsedNanos / totalIds);
        System.out.printf("Unique IDs: %,d (collisions: %d)%n", allIds.size(), totalIds - allIds.size());

        long sampleId = generators[0].nextId();
        System.out.println("Sample ID: " + sampleId + " -> " + SnowflakeIdGenerator.decompose(sampleId));
    }
}
