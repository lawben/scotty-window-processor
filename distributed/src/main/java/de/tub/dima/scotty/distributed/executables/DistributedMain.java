package de.tub.dima.scotty.distributed.executables;

import de.tub.dima.scotty.distributed.DistributedUtils;
import java.util.List;
import java.util.stream.Collectors;

public class DistributedMain {
    public static void main(String[] args) {
        if (args.length < 7) {
            System.err.println("Not enough arguments!\nUsage: java ... "
                    + "controllerPort "
                    + "windowPort "
                    + "resultPath "
                    + "streamPortStart "
                    + "numChildren "
                    + "numStreams "
                    + "numEvents "
                    + "[seedList]");
            System.exit(1);
        }

        final int rootControllerPort = Integer.parseInt(args[0]);
        final int rootWindowPort = Integer.parseInt(args[1]);
        final String resultPath = args[2];
        final int streamPort = Integer.parseInt(args[3]);
        final int numChildren = Integer.parseInt(args[4]);
        final int numStreams = Integer.parseInt(args[5]);
        final int numEvents = Integer.parseInt(args[6]);

        System.out.println("Running with " + numChildren + " children, " + numStreams + " streams, and " +
                numEvents + " events per stream.");

        final List<Long> randomSeeds = DistributedUtils.getRandomSeeds(args, numStreams, 7);
        List<String> seedStrings = randomSeeds.stream().map(String::valueOf).collect(Collectors.toList());
        System.out.println("Using seeds: " + String.join(",", seedStrings));

        if (numChildren > numStreams) {
            System.err.println("Need at least as many streams as children! "
                    + "Got " + numStreams + ", need at least " + numChildren);
            System.exit(1);
        }

        DistributedRootMain.runRoot(rootControllerPort, rootWindowPort, resultPath, numChildren);

        for (int childId = 0; childId < numChildren; childId++) {
            DistributedChildMain.runChild("localhost", rootControllerPort, rootWindowPort, streamPort + childId, childId);
        }

        for (int streamId = 0; streamId < numStreams; streamId++) {
            int assignedChild = streamId % numChildren;
            InputStreamMain.runInputStream("localhost", streamPort + assignedChild, numEvents, streamId, randomSeeds.get(streamId));
        }
    }

}
