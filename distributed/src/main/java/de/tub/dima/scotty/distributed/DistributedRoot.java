package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

public class DistributedRoot implements Runnable {
    // Network related
    private final ZContext context;
    private final int controllerPort;
    private final int windowPort;
    private ZMQ.Socket windowPuller;
    private final int numChildren;


    // Slicing related
    private final DistributedRootSlicer<Integer> slicer;

    public DistributedRoot(int controllerPort, int windowPort, int numChildren) {
        this.controllerPort = controllerPort;
        this.windowPort = windowPort;
        this.numChildren = numChildren;

        this.context = new ZContext();

        StateFactory stateFactory = new MemoryStateFactory();
        this.slicer = new DistributedRootSlicer<>(stateFactory);
    }

    public void run() {
        System.out.println("Starting root worker with controller port " + this.controllerPort +
                " and window port " + this.windowPort);

        // 1. Wait for all children to register (hard-coded for now)
        this.waitForChildren(this.numChildren);

        // 2. wait for pre-aggregated windows
        this.waitForPreAggregatedWindows();
    }

    private void waitForPreAggregatedWindows() {
        if (this.windowPuller == null) {
            this.windowPuller = this.context.createSocket(SocketType.PULL);
            this.windowPuller.bind(DistributedUtils.buildTcpUrl("0.0.0.0", this.windowPort));
        }

        ZMQ.Poller preAggregatedWindows = this.context.createPoller(1);
        preAggregatedWindows.register(this.windowPuller, Poller.POLLIN);

        final long pollTimeout = 10 * 1000;
        while (!Thread.currentThread().isInterrupted()) {
            if (preAggregatedWindows.poll(pollTimeout) == 0) {
                // Timed out --> quit
                System.out.println("No more data to come. Ending root worker...");
                return;
            }
            if (preAggregatedWindows.pollin(0)) {
                String childId = this.windowPuller.recvStr(ZMQ.DONTWAIT);
                String rawAggregateWindowId = this.windowPuller.recvStr(ZMQ.DONTWAIT);
                String rawWindowTimestamps = this.windowPuller.recvStr(ZMQ.DONTWAIT);
                byte[] rawPreAggregatedResult = this.windowPuller.recv(ZMQ.DONTWAIT);

                // WindowId
                List<Long> windowIdSplit = stringToLongs(rawAggregateWindowId);
                WindowAggregateId windowId = new WindowAggregateId(windowIdSplit.get(0), windowIdSplit.get(1));

                // Window Timestamps
                List<Long> windowTimestampSplit = stringToLongs(rawWindowTimestamps);
                long windowStartTimestamp = windowTimestampSplit.get(0);
                long windowEndTimestamp = windowTimestampSplit.get(1);

                // Partial Aggregate
                Object partialAggregateObject = DistributedUtils.bytesToObject(rawPreAggregatedResult);
                Integer partialAggregate = (Integer) partialAggregateObject;

                System.out.println("[" + childId + "] " + partialAggregate + " <-- pre-aggregated window from " +
                        windowStartTimestamp + " to " + windowEndTimestamp + " with " + windowId);
            }
        }
    }

    private void waitForChildren(int numChildren) {
        ZMQ.Socket childReceiver = this.context.createSocket(SocketType.REP);
        childReceiver.bind(DistributedUtils.buildTcpUrl("0.0.0.0", this.controllerPort));

        ZMQ.Poller children = this.context.createPoller(1);
        children.register(childReceiver, Poller.POLLIN);

        String[] windowStrings = {"TUMBLING,1000,1", "SLIDING,10000,5000,3"};
        String completeWindowString = String.join("\n", windowStrings);

        int numChildrenRegistered = 0;
        while (numChildrenRegistered < numChildren) {
            children.poll();

            if (children.pollin(0)) {
                String message = childReceiver.recvStr();
                System.out.println("Received from child: " + message);
                childReceiver.send(completeWindowString);
                numChildrenRegistered++;
            }
        }
    }

    private List<Long> stringToLongs(String rawString) {
        String[] strings = rawString.split(",");
        List<Long> longs = new ArrayList<>(strings.length);
        for (int i = 0; i < strings.length; i++) {
            longs.add(Long.valueOf(strings[i]));
        }
        return longs;
    }
}
