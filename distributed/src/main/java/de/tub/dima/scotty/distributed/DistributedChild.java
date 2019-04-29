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
import java.util.List;
import java.util.Random;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class DistributedChild implements Runnable {

    private final int childId;

    // Network related
    private final String rootIp;
    private final int rootControllerPort;
    private final int rootWindowPort;
    private final ZContext context;
    private ZMQ.Socket windowPusher;

    // Slicing related
    private final DistributedChildSlicer<Integer> slicer;

    private static final long WATERMARK_DURATION_MS = 1000;
    private static final long NUM_RECORDS_TO_PROCESS = 300;

    public DistributedChild(String rootIp, int rootControllerPort, int rootWindowPort, int childId) {
        this.rootIp = rootIp;
        this.rootControllerPort = rootControllerPort;
        this.rootWindowPort = rootWindowPort;
        this.childId = childId;

        StateFactory stateFactory = new MemoryStateFactory();
        // TODO: add root in correct way
        this.slicer = new DistributedChildSlicer<>(stateFactory);

        this.context = new ZContext();
    }

    public void run() {
        System.out.println("Starting child worker [" + this.childId +
                "]. Connecting to root at " + this.rootIp + " with controller port "
                + this.rootControllerPort + " and window port " + this.rootWindowPort);

        // 1. connect to root server
        // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
        List<Window> windows = this.getWindowsFromRoot();

        if (windows.isEmpty()) {
            throw new RuntimeException("Did not receive any windows from root!");
        }

        // TODO: also get this from root
        final ReduceAggregateFunction<Integer> SUM = Integer::sum;
        this.slicer.addWindowFunction(SUM);

        for (Window window : windows) {
            this.slicer.addWindowAssigner(window);
        }

        // 2. let IoT nodes register themselves
        this.waitForInputStreams();

        // 3. process streams
        // 4. send windows to root
        this.processStreams();

    }

    private void processStreams() {
        // TODO: open connection here. Fake it for now.
        // Should be implemented with PUSH/PULL.

        final long startTime = System.currentTimeMillis();
        long lastWatermark = 0;
        Random rand = new Random();

        int numRecordsProcessed = 0;
        while (numRecordsProcessed < NUM_RECORDS_TO_PROCESS) {
            long eventTimestamp = System.currentTimeMillis() - startTime;
            this.slicer.processElement(rand.nextInt(100), eventTimestamp);
            numRecordsProcessed++;

            // TODO: obviously remove this in real code.
            // Wait for 0 to 100 milliseconds until next record.
            try {
                Thread.sleep(rand.nextInt(10) * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // If we haven't processed a watermark in 5 seconds, process it.
            final long watermarkTimestamp = System.currentTimeMillis() - startTime;
            if (watermarkTimestamp > lastWatermark + WATERMARK_DURATION_MS) {
                System.out.println("[" + this.childId + "] Processing watermark " + watermarkTimestamp);
                List<AggregateWindow> preAggregatedWindows = this.slicer.processWatermark(watermarkTimestamp);
                this.sendPreAggregatedWindowsToRoot(preAggregatedWindows);
                lastWatermark = watermarkTimestamp;
            }
        }

        System.out.println("[" + this.childId + "] Processed all events.");
        final long watermarkTimestamp = System.currentTimeMillis() - startTime + WATERMARK_DURATION_MS;
        List<AggregateWindow> preAggregatedWindows = this.slicer.processWatermark(watermarkTimestamp);
        this.sendPreAggregatedWindowsToRoot(preAggregatedWindows);
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
            this.windowPusher.sendMore(windowId.getWindowId() + "," + windowId.getWindowStartTimestamp());
            this.windowPusher.sendMore(preAggregatedWindow.getStart() + "," + preAggregatedWindow.getEnd());
            this.windowPusher.send(partialAggregateBytes);
        }
    }

    private void waitForInputStreams() {
        // TODO: open connection here. Fake it for now with only 1 stream.
        // Should be implemented with REQ/REP.
    }

    private List<Window> getWindowsFromRoot() {
        ZMQ.Socket controlClient = this.context.createSocket(SocketType.REQ);
        controlClient.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootControllerPort));

        controlClient.send("[" + this.childId + "] I am a new child.");

        byte[] response = controlClient.recv();
        String windowString = new String(response, ZMQ.CHARSET);
        System.out.println("[" + this.childId + "] Received: [" + windowString.replace("\n", ";") + "]");
        return this.createWindowsFromString(windowString);
    }

    private List<Window> createWindowsFromString(String windowString) {
        ArrayList<Window> windows = new ArrayList<>();

        String[] windowRows = windowString.split("\n");
        for (String windowRow : windowRows) {
            String[] windowDetails = windowRow.split(",");
            assert windowDetails.length > 0;
            switch (windowDetails[0]) {
                case "TUMBLING": {
                    assert windowDetails.length >= 2;
                    final int size = Integer.parseInt(windowDetails[1]);
                    final int windowId = windowDetails.length == 3 ? Integer.parseInt(windowDetails[2]) : -1;
                    windows.add(new TumblingWindow(WindowMeasure.Time, size, windowId));
                    break;
                }
                case "SLIDING": {
                    assert windowDetails.length >= 3;
                    final int size = Integer.parseInt(windowDetails[1]);
                    final int slide = Integer.parseInt(windowDetails[2]);
                    final int windowId = windowDetails.length == 4 ? Integer.parseInt(windowDetails[3]) : -1;
                    windows.add(new SlidingWindow(WindowMeasure.Time, size, slide, windowId));
                    break;
                }
                default: {
                    System.out.println("No window type known for: '" + windowDetails[0] + "'");
                }

            }
            System.out.println("Adding window: " + windows.get(windows.size() - 1));
        }

        return windows;
    }
}
