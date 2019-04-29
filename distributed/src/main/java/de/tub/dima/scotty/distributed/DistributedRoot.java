package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
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

    private static final int NUM_EXPECTED_CHILDREN = 2;

    // Slicing related
    private final DistributedRootSlicer<Integer> slicer;

    public DistributedRoot(int controllerPort, int windowPort) {
        this.controllerPort = controllerPort;
        this.windowPort = windowPort;

        this.context = new ZContext();

        StateFactory stateFactory = new MemoryStateFactory();
        this.slicer = new DistributedRootSlicer<>(stateFactory);
    }

    public void run() {
        System.out.println("Starting root worker with controller port " + this.controllerPort +
                " and window port " + this.windowPort);

        // 1. Wait for all children to register (hard-coded for now)
        this.waitForChildren(NUM_EXPECTED_CHILDREN);

        // 2. wait for pre-aggregated windows
        this.waitForPreAggregatedWindows();
    }

    private void waitForPreAggregatedWindows() {
        if (this.windowPuller == null) {
            this.windowPuller = this.context.createSocket(SocketType.PULL);
            this.windowPuller.bind("tcp://0.0.0.0:" + this.windowPort);
        }

        while (true) {
            String preAggregatedWindow = this.windowPuller.recvStr();
            System.out.println("Received pre-aggregatedWindow: " + preAggregatedWindow);
        }
    }

    private void waitForChildren(final int numChildren) {
        ZMQ.Socket childReceiver = this.context.createSocket(SocketType.REP);
        childReceiver.bind("tcp://0.0.0.0:" + this.controllerPort);

        ZMQ.Poller children = this.context.createPoller(1);
        children.register(childReceiver, Poller.POLLIN);

        int numChildrenRegistered = 0;
        while (numChildrenRegistered < numChildren) {
            children.poll();

            if (children.pollin(0)) {
                String message = childReceiver.recvStr();
                System.out.println("Received from child: " + message);
                childReceiver.send("TUMBLING,1000,1");
                numChildrenRegistered++;
            }
        }

    }
}
