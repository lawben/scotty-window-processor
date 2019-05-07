package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class DistributedBenchmark {
    private DistributedWindowMerger<Integer> windowMerger;
    private DistributedRoot root;
    private int numChildren = 100;

    long counter;
    BiFunction<WindowAggregateId, Integer, Optional<AggregateWindow>> x;

    @Setup(Level.Iteration)
    public void setupIteration()  {
        List<Window> windows = Collections.singletonList(new TumblingWindow(WindowMeasure.Time, 1000, 1));
        ReduceAggregateFunction<Integer> aggFn = DistributedUtils.aggregateFunctionSum();
        windowMerger = new DistributedWindowMerger<>(new MemoryStateFactory(), 1, windows, aggFn);
        root = new DistributedRoot(0, 0, "/tmp/bench-res", numChildren);
        root.setupWindowMerger(windows, aggFn);
        counter = 0;
    }

    @Benchmark()
    public void benchmarkTumblingWindowMerger() {
        windowMerger.processPreAggregate(10, new WindowAggregateId(1, 0, 1000));
    }

    @Benchmark()
    public void benchmarkTumblingWindowRoot() {
        long windowId = counter / numChildren;
        Optional<AggregateWindow> bla = root.processPreAggregateWindow(new WindowAggregateId(windowId, 0, 1000), 10);
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
