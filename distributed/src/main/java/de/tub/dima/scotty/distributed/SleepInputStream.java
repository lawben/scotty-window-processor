package de.tub.dima.scotty.distributed;

import java.util.Random;
import org.zeromq.ZMQ.Socket;

/**
 * Uses the random function to sleep. This causes the event time to progress "normally". The sleep times are
 * deterministic under the same `rand` condition but the sleep is not.
 */
public class SleepInputStream<T> extends InputStream<T> {
    public SleepInputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort) {
        super(streamId, config, nodeIp, nodePort);
    }

    @Override
    final protected long generateAndSendEvents(InputStreamConfig<T> config, Random rand, Socket eventSender) throws Exception {
        int numRecordsProcessed = 0;
        long lastEventTimestamp = 0;
        long startTime = config.startTimestamp;
        while (numRecordsProcessed < config.numEventsToSend) {
            this.doSleep(config.minWaitTimeMillis, config.maxWaitTimeMillis, rand);

            long eventTimestamp = System.currentTimeMillis() - startTime;
            T eventValue = config.generatorFunction.apply(rand);

            eventSender.sendMore(String.valueOf(this.streamId));
            eventSender.sendMore(String.valueOf(eventTimestamp));
            boolean handedOver = eventSender.send(DistributedUtils.objectToBytes(eventValue));

            if (!handedOver) {
                System.err.println("Did not hand over");
            }


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
