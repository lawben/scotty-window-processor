package de.tub.dima.scotty.distributed;

import java.util.Random;
import java.util.function.Function;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

class InputStreamConfig<T> {
    final long numEventsToSend;
    final int minWaitTimeMillis;
    final int maxWaitTimeMillis;
    final long startTimestamp;

    final Function<Random, T> generatorFunction;
    final long randomSeed;

    InputStreamConfig(long numEventsToSend, int minWaitTimeMillis, int maxWaitTimeMillis, long startTimestamp,
            Function<Random, T> generatorFunction, long randomSeed) {
        this.numEventsToSend = numEventsToSend;
        this.minWaitTimeMillis = minWaitTimeMillis;
        this.maxWaitTimeMillis = maxWaitTimeMillis;
        this.startTimestamp = startTimestamp;
        this.generatorFunction = generatorFunction;
        this.randomSeed = randomSeed;
    }
}

public class InputStream<T> implements Runnable {

    private final int streamId;
    private final InputStreamConfig<T> config;
    private final String nodeIp;
    private final int nodePort;

    public InputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort) {
        this.streamId = streamId;
        this.config = config;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    @Override
    public void run() {
        System.out.println(this.streamIdString("Starting stream of " + this.config.numEventsToSend + " events to node "
                + this.nodeIp + ":" + this.nodePort));

        System.out.println(this.streamIdString("Using seed: " + this.config.randomSeed));
        Random rand = new Random(this.config.randomSeed);

        try (ZContext context = new ZContext()) {
            ZMQ.Socket eventSender = context.createSocket(SocketType.PUSH);
            eventSender.connect(DistributedUtils.buildTcpUrl(this.nodeIp, this.nodePort));

            int numRecordsProcessed = 0;
            long lastEventTimestamp = -1;
            while (numRecordsProcessed < this.config.numEventsToSend) {

                T eventValue = this.config.generatorFunction.apply(rand);
                long eventTimestamp = System.currentTimeMillis() - this.config.startTimestamp;

                eventSender.sendMore(String.valueOf(this.streamId));
                eventSender.sendMore(String.valueOf(eventTimestamp));
                eventSender.send(DistributedUtils.objectToBytes(eventValue));

                numRecordsProcessed++;
                lastEventTimestamp = eventTimestamp;

//                int max = this.config.maxWaitTimeMillis;
//                int min = this.config.minWaitTimeMillis;
//                int sleepTime = rand.nextInt((max - min) + 1) + min;
//                try {
//                    Thread.sleep(sleepTime);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
            System.out.println(this.streamIdString("Last event timestamp: " + lastEventTimestamp));
        }

        System.out.println(this.streamIdString("Finished sending events. Shutting down..."));
    }

    private String streamIdString(String msg) {
        return "[STREAM-" + this.streamId + "] " + msg;
    }


}
