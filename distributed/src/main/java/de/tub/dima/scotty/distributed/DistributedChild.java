package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

public class DistributedChild implements Runnable {

    private final int childId;

    // Network related
    private final String rootIp;
    private final int rootControllerPort;
    private final int rootWindowPort;
    private final int streamInputPort;
    private final ZContext context;
    private ZMQ.Socket windowPusher;

    // Slicing related
    private final DistributedChildSlicer<Integer> slicer;
    private final Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;

    private static final long WATERMARK_DURATION_MS = 1000;
    private long watermarkMs = WATERMARK_DURATION_MS;


    public DistributedChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamInputPort, int childId) {
        this.rootIp = rootIp;
        this.rootControllerPort = rootControllerPort;
        this.rootWindowPort = rootWindowPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;

        StateFactory stateFactory = new MemoryStateFactory();
        // TODO: add root in correct way
        this.slicer = new DistributedChildSlicer<>(stateFactory);
        this.slicerPerStream = new HashMap<>();

        this.context = new ZContext();
    }

    @Override
    public void run() {
        System.out.println(this.childIdString("Starting child worker. Connecting to root at " + this.rootIp +
                " with controller port " + this.rootControllerPort + " and window port " + this.rootWindowPort));

        // 1. connect to root server
        // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
        List<Window> windows = this.getWindowsFromRoot();

        if (windows.isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any windows from root!"));
        }

        // TODO: also get this from root
        final ReduceAggregateFunction<Integer> SUM = Integer::sum;
        this.slicer.addWindowFunction(SUM);

        for (Window window : windows) {
            this.slicer.addWindowAssigner(window);
        }

        // 3. process streams
        // 4. send windows to root
        this.processStreams();

    }

    private void processStreams() {
        ZMQ.Socket streamInput = this.context.createSocket(SocketType.PULL);
        streamInput.bind(DistributedUtils.buildLocalTcpUrl(this.streamInputPort));

        ZMQ.Poller streamPoller = this.context.createPoller(1);
        streamPoller.register(streamInput, Poller.POLLIN);

        System.out.println(this.childIdString("Waiting for stream data."));

        long currentEventTime = 0;
        long lastWatermark = 0;
        final long pollTimeout = 3 * 1000;

        while (!Thread.currentThread().isInterrupted()) {
            if (streamPoller.poll(pollTimeout) == 0) {
                System.out.println(this.childIdString("Processed all events."));
                final long watermarkTimestamp = currentEventTime + WATERMARK_DURATION_MS;
                List<AggregateWindow> preAggregatedWindows = this.slicer.processWatermark(watermarkTimestamp);
                this.sendPreAggregatedWindowsToRoot(preAggregatedWindows);
                System.out.println(this.childIdString("No more data to come. Ending child worker..."));
                return;
            }

            if (streamPoller.pollin(0)) {
                int streamId = Integer.parseInt(streamInput.recvStr(ZMQ.DONTWAIT));
                long eventTimestamp = Long.valueOf(streamInput.recvStr(ZMQ.DONTWAIT));
                Object eventValue = DistributedUtils.bytesToObject(streamInput.recv(ZMQ.DONTWAIT));

//                slicerPerStream.putIfAbsent(streamId, new DistributedChildSlicer<>(new MemoryStateFactory()));
//                DistributedChildSlicer<Integer> slicer = slicerPerStream.get(streamId);
                this.slicer.processElement((this.slicer.castFromObject(eventValue)), eventTimestamp);
                currentEventTime = eventTimestamp;
            }


            // If we haven't processed a watermark in WATERMARK_DURATION_MS, process it.
            final long watermarkTimestamp = lastWatermark + this.watermarkMs;
            if (currentEventTime >= watermarkTimestamp) {
//                System.out.println(this.childIdString("Processing watermark " + watermarkTimestamp));
                List<AggregateWindow> preAggregatedWindows = this.slicer.processWatermark(watermarkTimestamp);
                this.sendPreAggregatedWindowsToRoot(preAggregatedWindows);
                lastWatermark = watermarkTimestamp;
            }
        }


    }

    private void sendPreAggregatedWindowsToRoot(List<AggregateWindow> preAggregatedWindows) {
        if (this.windowPusher == null) {
            this.windowPusher = this.context.createSocket(SocketType.PUSH);
            this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootWindowPort));
        }

        for (AggregateWindow preAggregatedWindow : preAggregatedWindows) {
            WindowAggregateId windowId = preAggregatedWindow.getWindowAggregateId();

            // Convert partial aggregation result to byte array
            List aggValues = preAggregatedWindow.getAggValues();
            Object partialAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            byte[] partialAggregateBytes = DistributedUtils.objectToBytes(partialAggregate);

            // Order:
            // Integer      childId
            // Integer,Long WindowAggregateId
            // Long,Long    window start, window end
            // Byte[]       raw bytes of partial aggregate
            this.windowPusher.sendMore(String.valueOf(this.childId));
            this.windowPusher.sendMore(windowId.getWindowId() + "," + windowId.getWindowStartTimestamp() + "," + windowId.getWindowEndTimestamp());
            this.windowPusher.send(partialAggregateBytes);
        }
    }

    private List<Window> getWindowsFromRoot() {
        ZMQ.Socket controlClient = this.context.createSocket(SocketType.REQ);
        controlClient.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootControllerPort));

        controlClient.send(this.childIdString("I am a new child."));

        this.watermarkMs = Long.valueOf(controlClient.recvStr());
        String windowString = controlClient.recvStr();
        System.out.println(this.childIdString("Received: " + this.watermarkMs +
                " [" + windowString.replace("\n", ";") + "]"));

        return this.createWindowsFromString(windowString);
    }

    private List<Window> createWindowsFromString(String windowString) {
        ArrayList<Window> windows = new ArrayList<>();

        String[] windowRows = windowString.split("\n");
        for (String windowRow : windowRows) {
            windows.add(DistributedUtils.buildWindowFromString(windowRow));
            System.out.println(this.childIdString("Adding window: " + windows.get(windows.size() - 1)));
        }

        return windows;
    }

    private String childIdString(String msg) {
        return "[CHILD-" + this.childId + "] " + msg;
    }
}
