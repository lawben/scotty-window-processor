package de.tub.dima.scotty.distributed;

import java.util.Random;
import org.zeromq.ZMQ;

/**
 * Uses the random function to advance the timestamp of the events but not actual time. This is deterministic in the
 * creation of the events given rand with the same seed. Network-related code is not deterministic.
 */
public class ThroughputInputStream<T> extends SleepInputStream<T> {
    public ThroughputInputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort) {
        super(streamId, config, nodeIp, nodePort);
    }

    @Override
    final protected void doSleep(int minSleep, int maxSleep, Random rand) {
        // Do nothing, as we do not want to sleep.
    }
}
