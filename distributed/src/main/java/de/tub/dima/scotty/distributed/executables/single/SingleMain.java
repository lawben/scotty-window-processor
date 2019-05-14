package de.tub.dima.scotty.distributed.executables.single;

import de.tub.dima.scotty.distributed.DistributedChild;
import de.tub.dima.scotty.distributed.DistributedUtils;
import de.tub.dima.scotty.distributed.executables.InputStreamMain;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SingleMain {
    // Example invocation:
    // tshark -f \"tcp port 4055 || tcp port 4056 || tcp portrange "4060-4069 || tcp portrange 4160-4169\"
    // -i lo0 -w /tmp/single-run-4-20-1000000.pcap (-a duration:300)
    private static final String TSHARK_PORT_TEMPLATE = "tcp port %d || tcp port %d || tcp portrange %d-%d || tcp portrange %d-%d";

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("Not enough arguments!\nUsage: java ... "
                    + "rootPort "
                    + "resultPath "
                    + "streamPortStart "
                    + "numChildren "
                    + "numStreams "
                    + "numEvents "
                    + "[seedList]");
            System.exit(1);
        }

        final int rootPort = Integer.parseInt(args[0]);
        final String resultPath = args[1];
        final int streamPort = Integer.parseInt(args[2]);
        final int numChildren = Integer.parseInt(args[3]);
        final int numStreams = Integer.parseInt(args[4]);
        final int numEvents = Integer.parseInt(args[5]);


        final int streamRegisterPort = streamPort + DistributedChild.STREAM_REGISTER_PORT_OFFSET;
        final String networkInterface = "lo0";
        Instant runTimestamp = Instant.ofEpochMilli(System.currentTimeMillis());
        final String tsharkOutputFile = String.format("/Users/law/repos/ma/runs-scotty/dist-run-%d-%d-%d-%s.pcap", numChildren, numStreams, numEvents, runTimestamp);

        String tsharkPortString = String.format(TSHARK_PORT_TEMPLATE, rootPort, rootPort, streamPort,
                streamPort + 10, streamRegisterPort, streamRegisterPort + 10);
        String[] tsharkArgs = {"tshark", "-f", tsharkPortString, "-i", networkInterface, "-w", tsharkOutputFile};
        System.out.println("Starting tshark with command:\n\t" + Arrays.toString(tsharkArgs));
        Process tshark = Runtime.getRuntime().exec(tsharkArgs);


        System.out.println("Running with " + numChildren + " children, " + numStreams + " streams, and " +
                numEvents + " events per stream.");

        final List<Long> randomSeeds = DistributedUtils.getRandomSeeds(args, numStreams, 6);
        List<String> seedStrings = randomSeeds.stream().map(String::valueOf).collect(Collectors.toList());
        System.out.println("Using seeds: " + String.join(",", seedStrings));

        if (numChildren > numStreams) {
            System.err.println("Need at least as many streams as children! "
                    + "Got " + numStreams + ", need at least " + numChildren);
            System.exit(1);
        }

        Thread rootThread = SingleNodeMain.runSingleNode(rootPort, resultPath);

        for (int childId = 0; childId < numChildren; childId++) {
            EventForwarderMain.runForwarder("localhost", rootPort, streamPort + childId, childId);
        }

        for (int streamId = 0; streamId < numStreams; streamId++) {
            int assignedChild = streamId % numChildren;
            InputStreamMain.runInputStream("localhost", streamPort + assignedChild, numEvents, streamId,
                    randomSeeds.get(streamId), /*isDistributed=*/false);
        }

        rootThread.join();
        System.out.println("Finished streaming.");
        System.out.println("TSHARK OUT:\n-----------");
        printTsharkOut(tshark.getErrorStream());
        printTsharkOut(tshark.getInputStream());
        tshark.destroy();
    }

    private static void printTsharkOut(java.io.InputStream in) throws IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(in));
        while (input.ready()) {
            System.out.println(input.readLine());
        }
    }
}
