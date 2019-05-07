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

    @Override
    public String toString() {
        return "InputStreamConfig{" +
                "numEventsToSend=" + numEventsToSend +
                ", minWaitTimeMillis=" + minWaitTimeMillis +
                ", maxWaitTimeMillis=" + maxWaitTimeMillis +
                ", startTimestamp=" + startTimestamp +
                ", generatorFunction=" + generatorFunction +
                ", randomSeed=" + randomSeed +
                '}';
    }
}

abstract public class InputStream<T> implements Runnable {

    protected final int streamId;
    private final InputStreamConfig<T> config;
    private final String nodeIp;
    private final int nodePort;

    public InputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort) {
        this.streamId = streamId;
        this.config = config;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    /**
     * Implement this to generate data in the wanted manner.
     * Example: generate a value an then sleep for x ms or just increase the timestmpa by x ms.
     */
    protected abstract long generateAndSendEvents(InputStreamConfig<T> config, Random rand, ZMQ.Socket eventSender) throws Exception;

    protected String streamName() { return this.getClass().getSimpleName(); }

    @Override
    public void run() {
        System.out.println(this.streamIdString("Starting " + this.streamName() + " with " + this.config.numEventsToSend
                + " events to node " + this.nodeIp + ":" + this.nodePort + " with " + this.config));

        System.out.println(this.streamIdString("Using seed: " + this.config.randomSeed));
        Random rand = new Random(this.config.randomSeed);

        try (ZContext context = new ZContext()) {
            this.registerAtNode(context);

            ZMQ.Socket eventSender = context.createSocket(SocketType.PUSH);
            eventSender.connect(DistributedUtils.buildTcpUrl(this.nodeIp, this.nodePort));

            Thread.sleep(DistributedChild.STREAM_REGISTER_TIMEOUT_MS * 2);
            System.out.println(this.streamIdString("Start sending data"));

            long lastEventTimestamp = this.generateAndSendEvents(this.config, rand, eventSender);

            System.out.println(this.streamIdString("Last event timestamp: " + lastEventTimestamp));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println(this.streamIdString("Finished sending events. Shutting down..."));
    }


    private void registerAtNode(ZContext context) {
        final ZMQ.Socket nodeRegistrar = context.createSocket(SocketType.REQ);
        nodeRegistrar.connect(DistributedUtils.buildTcpUrl(this.nodeIp, this.nodePort + DistributedChild.STREAM_REGISTER_PORT_OFFSET));

        nodeRegistrar.send(String.valueOf(this.streamId));
        nodeRegistrar.recv();
        System.out.println(this.streamIdString("Registered at node."));
    }

    private String streamIdString(String msg) {
        return "[STREAM-" + this.streamId + "] " + msg;
    }


}
