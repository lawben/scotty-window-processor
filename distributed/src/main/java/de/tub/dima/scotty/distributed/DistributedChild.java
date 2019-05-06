package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    final static int STREAM_REGISTER_PORT_OFFSET = 100;
    final static long STREAM_REGISTER_TIMEOUT_MS = 1500;

    // Slicing related
//    private final DistributedChildSlicer<Integer> slicer;
    private final Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;
    private int numStreams;
    private DistributedWindowMerger<Integer> streamWindowMerger;

//    private static final long WATERMARK_DURATION_MS = 1000;
    private long watermarkMs;


    public DistributedChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamInputPort, int childId) {
        this.rootIp = rootIp;
        this.rootControllerPort = rootControllerPort;
        this.rootWindowPort = rootWindowPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;

        this.slicerPerStream = new HashMap<>();

        this.context = new ZContext();
    }

    @Override
    public void run() {
        System.out.println(this.childIdString("Starting child worker on port " + this.streamInputPort +
                ". Connecting to root at " + this.rootIp + " with controller port " + this.rootControllerPort +
                " and window port " + this.rootWindowPort));

        // 1. connect to root server
        // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
        List<Window> windows = this.getWindowsFromRoot();

        if (windows.isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any windows from root!"));
        }

        // 2. register streams
        this.registerStreams(STREAM_REGISTER_TIMEOUT_MS, windows);

        // 3. process streams
        // 4. send windows to root
        this.processStreams();

    }

    private void registerStreams(final long timeout, final List<Window> windows) {
        final ZMQ.Socket streamReceiver = this.context.createSocket(SocketType.REP);
        streamReceiver.bind(DistributedUtils.buildLocalTcpUrl(this.streamInputPort + STREAM_REGISTER_PORT_OFFSET));

        final ZMQ.Poller streams = this.context.createPoller(1);
        streams.register(streamReceiver, Poller.POLLIN);

        final MemoryStateFactory stateFactory = new MemoryStateFactory();

        // TODO: also get this from root
        final ReduceAggregateFunction<Integer> aggFn = DistributedUtils.aggregateFunction();
        byte[] ackResponse = new byte[] {'\0'};

        while (!Thread.currentThread().isInterrupted()) {
            if (streams.poll(timeout) == 0) {
                // Timed out --> all streams registered
                this.streamWindowMerger = new DistributedWindowMerger<>(stateFactory, this.numStreams, windows, aggFn);
                return;
            }

            final int streamId = Integer.parseInt(streamReceiver.recvStr(ZMQ.DONTWAIT));
            System.out.println(this.childIdString("Registering stream " + streamId));

            DistributedChildSlicer<Integer> childSlicer = new DistributedChildSlicer<>(stateFactory);
            childSlicer.addWindowFunction(aggFn);
            for (Window window : windows) {
                childSlicer.addWindowAssigner(window);
            }
            this.slicerPerStream.put(streamId, childSlicer);
            this.numStreams++;

            streamReceiver.send(ackResponse);
        }
    }

    private void processStreams() {
        ZMQ.Socket streamInput = this.context.createSocket(SocketType.PULL);
        streamInput.bind(DistributedUtils.buildLocalTcpUrl(this.streamInputPort));

        ZMQ.Poller streamPoller = this.context.createPoller(1);
        streamPoller.register(streamInput, Poller.POLLIN);

        System.out.println(this.childIdString("Waiting for stream data."));

        long currentEventTime = 0;
        long lastWatermark = 0;
        long numEvents = 0;
        final long pollTimeout = 3 * 1000;

        while (!Thread.currentThread().isInterrupted()) {
            if (streamPoller.poll(pollTimeout) == 0) {
                System.out.println(this.childIdString("Processed " + numEvents + " events in total."));
                final long watermarkTimestamp = currentEventTime + this.watermarkMs;
                this.processWatermarkedWindows(watermarkTimestamp);
                System.out.println(this.childIdString("No more data to come. Ending child worker..."));
                return;
            }

            int streamId = Integer.parseInt(streamInput.recvStr(ZMQ.DONTWAIT));
            long eventTimestamp = Long.valueOf(streamInput.recvStr(ZMQ.DONTWAIT));
            Object eventValue = DistributedUtils.bytesToObject(streamInput.recv(ZMQ.DONTWAIT));

            DistributedChildSlicer<Integer> perStreamSlicer = slicerPerStream.get(streamId);
            perStreamSlicer.processElement(perStreamSlicer.castFromObject(eventValue), eventTimestamp);
            currentEventTime = eventTimestamp;
            numEvents++;


            // If we haven't processed a watermark in watermarkMs milliseconds, process it.
            final long watermarkTimestamp = lastWatermark + this.watermarkMs;
            if (currentEventTime >= watermarkTimestamp) {
//                System.out.println(this.childIdString("Processing watermark " + watermarkTimestamp));
                this.processWatermarkedWindows(watermarkTimestamp);
                lastWatermark = watermarkTimestamp;
            }
        }
    }

    private void processWatermarkedWindows(long watermarkTimestamp) {
        this.slicerPerStream.forEach((streamId, slicer) -> {
            List<AggregateWindow> preAggregatedWindows = slicer.processWatermark(watermarkTimestamp);
            List<AggregateWindow> finalPreWAggregateWindows = this.mergeStreamWindows(preAggregatedWindows);
            this.sendPreAggregatedWindowsToRoot(finalPreWAggregateWindows);
        });
    }

    private List<AggregateWindow> mergeStreamWindows(List<AggregateWindow> preAggregatedWindows) {
        List<AggregateWindow> finalPreAggregateWindows = new ArrayList<>(preAggregatedWindows.size());

        for (AggregateWindow preAggWindow : preAggregatedWindows) {
            AggregateWindow<Integer> typedPreAggregateWindow = (AggregateWindow<Integer>) preAggWindow;
            List<Integer> aggValues = typedPreAggregateWindow.getAggValues();
            Integer partialAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            WindowAggregateId windowId = preAggWindow.getWindowAggregateId();
            boolean finalTrigger = this.streamWindowMerger.processPreAggregate(partialAggregate, windowId);

            if (finalTrigger) {
//                System.out.println(this.childIdString("Trigger in merge for " + windowId));
                AggregateWindow<Integer> finalPreAggregateWindow = this.streamWindowMerger.triggerFinalWindow(windowId);
                finalPreAggregateWindows.add(finalPreAggregateWindow);
            }
        }

        return finalPreAggregateWindows;
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
