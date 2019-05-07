package de.tub.dima.scotty.distributed;

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

    public static void runInputStream(String nodeIp, int nodePort, int numEvents, int streamId, long randomSeed) {
        Function<Random, Integer> eventGenerator = (rand) -> rand.nextInt(100);

        InputStreamConfig<Integer> config =
                new InputStreamConfig<>(numEvents, 10, 100, System.currentTimeMillis(), eventGenerator, randomSeed);

        InputStream<Integer> stream = new ThroughputInputStream<>(streamId, config, nodeIp, nodePort);
        Thread thread = new Thread(stream);
        thread.start();
    }
}
