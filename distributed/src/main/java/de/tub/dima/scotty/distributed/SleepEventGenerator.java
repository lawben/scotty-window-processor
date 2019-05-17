package de.tub.dima.scotty.distributed;

import java.util.Random;
import java.util.function.Function;
import org.zeromq.ZMQ;

/**
 * Uses the random function to sleep. This causes the event time to progress "normally". The sleep times are
 * deterministic under the same `rand` condition but the sleep is not.
 */
public class SleepEventGenerator<T> implements EventGenerator<T> {
    private final int streamId;
    private final InputStreamConfig<T> config;

    public SleepEventGenerator(int streamId, InputStreamConfig<T> config) {
        this.streamId = streamId;
        this.config = config;
    }

    @Override
    public long generateAndSendEvents(Random rand, ZMQ.Socket eventSender) throws Exception {
        int numRecordsProcessed = 0;
        long lastEventTimestamp = 0;
        long startTime = config.startTimestamp;
        final Function<Random, T> eventGenerator = config.generatorFunction;
        while (numRecordsProcessed < config.numEventsToSend) {
            this.doSleep(config.minWaitTimeMillis, config.maxWaitTimeMillis, rand);

            long eventTimestamp = System.currentTimeMillis() - startTime;
            Integer eventValue = (Integer) eventGenerator.apply(rand);

            String msg = String.valueOf(this.streamId) + ',' + eventTimestamp + ',' + eventValue;
            eventSender.send(msg, ZMQ.DONTWAIT);

            numRecordsProcessed++;
            lastEventTimestamp = eventTimestamp;
        }

        return lastEventTimestamp;
    }

    protected void doSleep(int minSleep, int maxSleep, Random rand) throws InterruptedException {
        int sleepTime = rand.nextInt((maxSleep - minSleep) + 1) + minSleep;
        Thread.sleep(sleepTime);
    }
}

