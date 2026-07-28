/**
 * Twitter Snowflake-style 64-bit unique ID generator.
 *
 * Layout (MSB to LSB):
 *   1 bit   unused (sign bit, always 0)
 *   41 bits timestamp (ms since CUSTOM_EPOCH, ~69 years of range)
 *   10 bits node ID    (up to 1024 distributed nodes/workers)
 *   12 bits sequence   (up to 4096 IDs per node per millisecond)
 *
 * Latency is near-zero because each call is pure local bit arithmetic
 * under a single lock -- no network round-trip or shared coordinator
 * is involved. Uniqueness across a distributed fleet comes from
 * partitioning the node ID space so no two nodes ever share one.
 */
public class SnowflakeIdGenerator {

    // Custom epoch: 2024-01-01T00:00:00Z, in milliseconds since Unix epoch.
    private static final long EPOCH = 1704067200000L;

    private static final long NODE_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_NODE_ID = (1L << NODE_ID_BITS) - 1;
    private static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;

    private static final long NODE_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + NODE_ID_BITS;

    private final long nodeId;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long nodeId) {
        if (nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException(
                    "Node ID must be between 0 and " + MAX_NODE_ID);
        }
        this.nodeId = nodeId;
    }

    public synchronized long nextId() {
        long timestamp = currentTimeMillis();

        if (timestamp < lastTimestamp) {
            // Clock rolled backwards (NTP correction, VM migration, etc).
            // Refuse rather than risk handing out a duplicate ID.
            throw new IllegalStateException(
                    "Clock moved backwards by " + (lastTimestamp - timestamp)
                            + "ms. Refusing to generate ID.");
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                // Exhausted this millisecond's sequence space; spin to the next one.
                timestamp = waitForNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (nodeId << NODE_ID_SHIFT)
                | sequence;
    }

    private long waitForNextMillis(long lastTimestamp) {
        long timestamp = currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = currentTimeMillis();
        }
        return timestamp;
    }

    private long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /** Splits an ID back into its component parts, mainly for debugging/demo output. */
    public static String decompose(long id) {
        long sequence = id & MAX_SEQUENCE;
        long nodeId = (id >> NODE_ID_SHIFT) & MAX_NODE_ID;
        long timestamp = (id >> TIMESTAMP_SHIFT) + EPOCH;
        return String.format("timestamp=%d, nodeId=%d, sequence=%d", timestamp, nodeId, sequence);
    }
}
