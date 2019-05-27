package de.tub.dima.scotty.distributed.executables;

import de.tub.dima.scotty.distributed.DistributedChild;
import de.tub.dima.scotty.distributed.EventGenerator;
import de.tub.dima.scotty.distributed.FakeTimeEventGenerator;
import de.tub.dima.scotty.distributed.InputStream;
import de.tub.dima.scotty.distributed.InputStreamConfig;
import de.tub.dima.scotty.distributed.ThroughputEventGenerator;
import de.tub.dima.scotty.distributed.single.SingleInputStream;
import java.util.Random;
import java.util.function.Function;

public class InputStreamMain {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Not enough arguments!\nUsage: java ... nodeIp nodePort numEvents streamId [randomSeed]");
            System.exit(1);
        }

        final String nodeIp = args[0];
        final int nodePort = Integer.parseInt(args[1]);
        final int numEvents = Integer.parseInt(args[2]);
        final int streamId = Integer.parseInt(args[3]);
        final long randomSeed = args.length >= 5 ? Long.valueOf(args[4]) : new Random().nextLong();

        runInputStream(nodeIp, nodePort, numEvents, streamId, randomSeed);
    }

    public static Thread runInputStream(String nodeIp, int nodePort, int numEvents, int streamId, long randomSeed) {
        return runInputStream(nodeIp, nodePort, numEvents, streamId, randomSeed, /*isDistributed=*/true);
    }

    public static Thread runInputStream(String nodeIp, int nodePort, int numEvents, int streamId, long randomSeed, boolean isDistributed) {
        Function<Random, Integer> valueGenerator = (rand) -> 1; //rand.nextInt(100);

        long startTime = System.currentTimeMillis() + DistributedChild.STREAM_REGISTER_TIMEOUT_MS * 2;
        InputStreamConfig<Integer> config =
                new InputStreamConfig<>(numEvents, 1, 5, startTime, valueGenerator, randomSeed);

        EventGenerator<Integer> eventGenerator = new ThroughputEventGenerator<>(streamId, config);

        InputStream<Integer> stream = new InputStream<>(streamId, config, nodeIp, nodePort, eventGenerator);
        if (!isDistributed) {
            stream = new SingleInputStream<>(streamId, config, nodeIp, nodePort, eventGenerator);
        }

        Thread thread = new Thread(stream);
        thread.start();
        return thread;
    }
}
