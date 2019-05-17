package de.tub.dima.scotty.distributed;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class GeneratorBenchmark {

    Random rand;
    Function<Random, Integer> generatorFunction;
    long numProcessed;

    AtomicLong counter;

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        rand = new Random();
        generatorFunction = (rand) -> rand.nextInt(100);
        numProcessed = 0;
        counter = new AtomicLong();
    }

//    @Threads(4)
//    @Benchmark()
    public void throughputGenerator(Blackhole bh) {
        doSleep(10, 10);
        Integer eventValue = generatorFunction.apply(rand);
        numProcessed++;
        bh.consume(eventValue);
    }


    @Threads(4)
    @Benchmark()
    public long incCounter() {
        return counter.getAndIncrement();
    }



    void doSleep(long x, long y) {}

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GeneratorBenchmark.class.getName())
                .forks(1)
                .warmupIterations(10)
                .measurementIterations(10)
                .syncIterations(true)
                .build();

        new Runner(opt).run();
    }


}
