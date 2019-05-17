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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class ChildNodeBenchmark {
    List<Window> windows;
    ReduceAggregateFunction<Integer> aggFn;

    long counter;
    private Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;
    private DistributedWindowMerger<Integer> streamWindowMerger;
    private int numStreams;
    private long lastWatermark;
    private long watermarkMs;

    @Setup(Level.Iteration)
    public void setupIteration() {
        windows = Collections.singletonList(new TumblingWindow(WindowMeasure.Time, 100, 1));
        aggFn = DistributedUtils.aggregateFunctionSum();
        counter = 0;
        watermarkMs = 100;

        setupChildRun();
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
    public void benchmarkChildProcessing(Blackhole bh) {
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
                    AggregateWindow<Integer> finalPreAggregateWindow = streamWindowMerger.triggerFinalWindow(windowId);
                    finalPreAggregateWindows.add(finalPreAggregateWindow);
                }
            }
            lastWatermark = watermarkTimestamp;
            bh.consume(finalPreAggregateWindows);
        }

        counter++;
        bh.consume(lastWatermark);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ChildNodeBenchmark.class.getName())
                .forks(1)
                .warmupIterations(10)
                .measurementIterations(10)
                .build();

        new Runner(opt).run();
    }


}
