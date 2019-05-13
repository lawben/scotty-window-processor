package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

@State(Scope.Benchmark)
public class DistributedBenchmark {
    List<Window> windows;
    ReduceAggregateFunction<Integer> aggFn;

    private DistributedWindowMerger<Integer> windowMerger;
    private DistributedRoot root;
    private int numChildren = 100;
    long counter;
    private Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;
    private DistributedWindowMerger<Integer> streamWindowMerger;
    private int numStreams;
    private long lastWatermark;
    private long watermarkMs;

    ZMQ.Socket pusher;
    ZMQ.Socket pusher2;
    ZMQ.Socket puller;
    ZMQ.Socket puller2;
    ZContext zContext;
    byte[] bytes;

    @Setup(Level.Iteration)
    public void setupIteration()  {
        windows = Collections.singletonList(new TumblingWindow(WindowMeasure.Time, 100, 1));
        aggFn = DistributedUtils.aggregateFunctionSum();
        windowMerger = new DistributedWindowMerger<>(new MemoryStateFactory(), 1, windows, aggFn);

        root = new DistributedRoot(0, 0, "/tmp/bench-res", numChildren);
        root.setupWindowMerger(windows, aggFn);

        counter = 0;
        watermarkMs = 100;

        setupChildRun();
//        setupNetworkRun();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        Integer value = 10;
        bytes = DistributedUtils.objectToBytes(value);
        setupNetworkPushRun();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        pusher.close();
        puller.close();
        zContext.destroy();
    }

    private void setupNetworkPushRun() {
        zContext = new ZContext();
        pusher = zContext.createSocket(SocketType.PUSH);
        puller = zContext.createSocket(SocketType.PULL);
        puller.setReceiveTimeOut(10 * 1000);

        int port = new Random().nextInt(64000) + 1000;
        System.out.println("Using port " + port);
        puller.bind("tcp://0.0.0.0:" + port);
        pusher.connect("tcp://localhost:" + port);

        Thread thread = new Thread(() -> {
            while (true) {
                puller.recvStr();
                puller.recvStr(ZMQ.DONTWAIT);
                puller.recv();
//                Object value = DistributedUtils.bytesToObject(puller.recv(ZMQ.DONTWAIT));
            }
        });
        thread.start();
    }


    private void setupNetworkPullRun() {
        zContext = new ZContext();
        pusher = zContext.createSocket(SocketType.PUSH);
        puller = zContext.createSocket(SocketType.PULL);

        int port = new Random().nextInt(64000) + 1000;
        System.out.println("Using port " + port);
        puller.bind("tcp://0.0.0.0:" + port);
        pusher.connect("tcp://localhost:" + port);

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (true) {
                Integer value = 10;
                pusher.sendMore(String.valueOf(0));
                pusher.sendMore(String.valueOf(1000));
//        pusher.sendMore(bytes);
                pusher.send(DistributedUtils.objectToBytes(value));
            }
        });
        thread.start();
    }

    public void setupChildRun() {
        slicerPerStream = new HashMap<>();
        numStreams = 5;
        streamWindowMerger = new DistributedWindowMerger<>(new MemoryStateFactory(), numStreams, windows, aggFn);
        lastWatermark = 0;

        for (int streamId = 0; streamId < numStreams; streamId++) {
            DistributedChildSlicer<Integer> childSlicer = new DistributedChildSlicer<>(new MemoryStateFactory());
            childSlicer.addWindowFunction(aggFn);
            for (Window window : windows) {
                childSlicer.addWindowAssigner(window);
            }
            slicerPerStream.put(streamId, childSlicer);
        }
    }

    @Benchmark()
    public void benchmarkZMQPush() {
        Integer value = 10;
        pusher.sendMore(String.valueOf(0));
        pusher.sendMore(String.valueOf(1000));
//        pusher.sendMore(bytes);
        pusher.send(DistributedUtils.objectToBytes(value));
    }

//    @Benchmark()
    public void benchmarkZMQPull() {
        puller.recvStr(ZMQ.DONTWAIT);
        puller.recvStr(ZMQ.DONTWAIT);
//        puller.recv();
        Object value = DistributedUtils.bytesToObject(puller.recv(ZMQ.DONTWAIT));
    }



//    @Benchmark()
    public void benchmarkChildProcessing() {
        // Process event
        int streamId = (int) (counter % numStreams);
        long eventTimestamp = counter;

        DistributedChildSlicer<Integer> perStreamSlicer = slicerPerStream.get(streamId);
        perStreamSlicer.processElement(perStreamSlicer.castFromObject(10), eventTimestamp);

        // Process watermark
        final long watermarkTimestamp = lastWatermark + this.watermarkMs;
        if (eventTimestamp >= watermarkTimestamp) {
            List<AggregateWindow> preAggregatedWindows = perStreamSlicer.processWatermark(watermarkTimestamp);
            List<AggregateWindow> finalPreAggregateWindows = new ArrayList<>(preAggregatedWindows.size());

            for (AggregateWindow preAggWindow : preAggregatedWindows) {
                AggregateWindow<Integer> typedPreAggregateWindow = (AggregateWindow<Integer>) preAggWindow;
                List<Integer> aggValues = typedPreAggregateWindow.getAggValues();
                Integer partialAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
                WindowAggregateId windowId = preAggWindow.getWindowAggregateId();
                boolean finalTrigger = streamWindowMerger.processPreAggregate(partialAggregate, windowId);

                if (finalTrigger) {
//                System.out.println(this.forwardIdString("Trigger in merge for " + windowId));
                    AggregateWindow<Integer> finalPreAggregateWindow = streamWindowMerger.triggerFinalWindow(windowId);
                    finalPreAggregateWindows.add(finalPreAggregateWindow);
                }
            }
            lastWatermark = watermarkTimestamp;
        }

        counter++;
    }


    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(DistributedBenchmark.class.getName())
                .forks(1)
                .warmupIterations(10)
                .measurementIterations(10)
                .build();

        new Runner(opt).run();
    }


}
