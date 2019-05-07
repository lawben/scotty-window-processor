package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.WindowAggregateId;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

public class ResultListener implements Runnable {
    String resultPath;

    private final static long TIMEOUT_MS = 10 * 1000;

    public ResultListener(String resultPath) {
        this.resultPath = resultPath;
    }

    @Override
    public void run() {
        System.out.println(this.resultString("Starting on path " + this.resultPath));

        try (ZContext context = new ZContext()) {
            ZMQ.Socket resultListener = context.createSocket(SocketType.PULL);
            resultListener.bind(DistributedUtils.buildIpcUrl(this.resultPath));

            ZMQ.Poller results = context.createPoller(1);
            results.register(resultListener, Poller.POLLIN);

            while (!Thread.currentThread().isInterrupted()) {
                if (results.poll(TIMEOUT_MS) == 0) {
                    System.out.println(this.resultString("No more results coming. Shutting down..."));
                    return;
                }

                String rawAggregateWindowId = resultListener.recvStr(ZMQ.DONTWAIT);
                byte[] rawAggregatedResult = resultListener.recv(ZMQ.DONTWAIT);

                WindowAggregateId windowId = DistributedUtils.stringToWindowId(rawAggregateWindowId);
                Object aggregateObject = DistributedUtils.bytesToObject(rawAggregatedResult);
                Integer finalAggregate = (Integer) aggregateObject;
                System.out.println(this.resultString("FINAL WINDOW: " + windowId + " --> " + finalAggregate));
            }
        }
    }

    private String resultString(String msg) {
        return "[RESULT] " + msg;
    }
}
