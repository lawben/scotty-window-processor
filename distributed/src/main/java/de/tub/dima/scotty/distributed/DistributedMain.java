package de.tub.dima.scotty.distributed;

import static java.lang.String.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class DistributedMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Not enough arguments!\nUsage: java ... "
                    + "controllerPort "
                    + "windowPort "
                    + "streamPortStart "
                    + "numChildren "
                    + "numStreams "
                    + "numEvents "
                    + "[seedList]");
            System.exit(1);
        }

        final int rootControllerPort = Integer.parseInt(args[0]);
        final int rootWindowPort = Integer.parseInt(args[1]);
        final int streamPort = Integer.parseInt(args[2]);
        final int numChildren = Integer.parseInt(args[3]);
        final int numStreams = Integer.parseInt(args[4]);
        final int numEvents = Integer.parseInt(args[5]);

        System.out.println("Running with " + numChildren + " children, " + numStreams + " streams, and " +
                numEvents + " events per stream.");

        final List<Long> randomSeeds = getRandomSeeds(args, numStreams);
        List<String> seedStrings = randomSeeds.stream().map(String::valueOf).collect(Collectors.toList());
        System.out.println("Using seeds: " + String.join(",", seedStrings));

        if (numChildren > numStreams) {
            System.err.println("Need at least as many streams as children! "
                    + "Got " + numStreams + ", need at least " + numChildren);
            System.exit(1);
        }

        DistributedRootMain.runRoot(rootControllerPort, rootWindowPort, numChildren);

        for (int childId = 0; childId < numChildren; childId++) {
            DistributedChildMain.runChild("localhost", rootControllerPort, rootWindowPort, streamPort + childId, childId);
        }

        for (int streamId = 0; streamId < numStreams; streamId++) {
            int assignedChild = streamId % numChildren;
            InputStreamMain.runInputStream("localhost", streamPort + assignedChild, numEvents, streamId, randomSeeds.get(streamId));
        }
    }

    @NotNull
    private static List<Long> getRandomSeeds(String[] args, int numStreams) {
        final List<Long> randomSeeds = new ArrayList<>();
        if (args.length >= 7) {
            String seedString = args[6];
            String[] seedStringSplit = seedString.split(",");
            assert seedStringSplit.length == numStreams;

            for (String seed : seedStringSplit) {
                randomSeeds.add(Long.valueOf(seed));
            }
        } else {
            Random rand = new Random();
            for (int i = 0; i < numStreams; i++) {
                randomSeeds.add(rand.nextLong());
            }
        }
        return randomSeeds;
    }
}
