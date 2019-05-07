package de.tub.dima.scotty.distributed;

import java.util.Random;
import org.zeromq.ZMQ;

/**
 * Uses the random function to advance the timestamp of the events but not actual time. This is deterministic in the
 * creation of the events given rand with the same seed. Network-related code is not deterministic.
 */
public class FakeTimeEventGenerator<T> implements EventGenerator<T> {
    private final int streamId;
    private final InputStreamConfig<T> config;

    public FakeTimeEventGenerator(int streamId, InputStreamConfig<T> config) {
        this.streamId = streamId;
        this.config = config;
    }

    @Override
    public long generateAndSendEvents(Random rand, ZMQ.Socket eventSender) {
        int numRecordsProcessed = 0;
        long lastEventTimestamp = 0;
        while (numRecordsProcessed < config.numEventsToSend) {
            int max = config.maxWaitTimeMillis;
            int min = config.minWaitTimeMillis;
            int fakeSleepTime = rand.nextInt((max - min) + 1) + min;
            long eventTimestamp = lastEventTimestamp + fakeSleepTime;

            T eventValue = config.generatorFunction.apply(rand);

            eventSender.sendMore(String.valueOf(this.streamId));
            eventSender.sendMore(String.valueOf(eventTimestamp));
            eventSender.send(DistributedUtils.objectToBytes(eventValue));

            numRecordsProcessed++;
            lastEventTimestamp = eventTimestamp;
        }

        return lastEventTimestamp;
    }
}

