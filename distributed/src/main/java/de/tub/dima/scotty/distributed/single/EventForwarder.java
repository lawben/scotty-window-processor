package de.tub.dima.scotty.distributed.single;

import de.tub.dima.scotty.distributed.DistributedUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class EventForwarder implements Runnable {

    private final String rootIp;
    private final int rootPort;
    private final int streamInputPort;
    private final int childId;

    private final static long TIMEOUT_MS = 10 * 1000;

    public EventForwarder(String rootIp, int rootPort, int streamInputPort, int childId) {
        this.rootIp = rootIp;
        this.rootPort = rootPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;
    }

    @Override
    public void run() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket streamInput = context.createSocket(SocketType.PULL);
            streamInput.bind(DistributedUtils.buildLocalTcpUrl(this.streamInputPort));

            ZMQ.Socket streamOutput = context.createSocket(SocketType.PUSH);
            streamOutput.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootPort));

            ZMQ.Poller inputPoller = context.createPoller(1);
            inputPoller.register(streamInput);

            System.out.println(this.forwardIdString("Starting proxy"));

            while (!Thread.currentThread().isInterrupted()) {
                if (inputPoller.poll(TIMEOUT_MS) == 0) {
                    // Timed out --> quit
                    System.out.println(this.forwardIdString("No more data. Shutting down..."));
                    return;
                }

                // Receive
                String streamId = streamInput.recvStr();
                String eventTimestamp = streamInput.recvStr();
                byte[] eventValueBytes = streamInput.recv();

                // Forward
                streamOutput.sendMore(String.valueOf(streamId));
                streamOutput.sendMore(String.valueOf(eventTimestamp));
                streamOutput.send(eventValueBytes);
            }

        }
    }

    protected String forwardIdString(String msg) {
        return "[FW-" + this.childId + "] " + msg;
    }
}
