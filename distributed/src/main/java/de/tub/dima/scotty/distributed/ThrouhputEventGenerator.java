package de.tub.dima.scotty.distributed;

import java.util.Random;
import org.zeromq.ZMQ;

/**
 * Creates as many events as possible with natural time.
 */
public class ThrouhputEventGenerator<T> extends SleepEventGenerator<T> {

    public ThrouhputEventGenerator(int streamId, InputStreamConfig<T> config) {
        super(streamId, config);
    }

    @Override
    protected void doSleep(int minSleep, int maxSleep, Random rand) {
        // Do nothing
    }
}

