package de.tub.dima.scotty.distributed;

public class DistributedRootMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Not enough arguments!\nUsage: java ... controllerPort windowPort");
            System.exit(1);
        }

        final int rootControllerPort = Integer.parseInt(args[0]);
        final int rootWindowPort = Integer.parseInt(args[1]);
        DistributedRoot worker = new DistributedRoot(rootControllerPort, rootWindowPort);

        worker.run();
    }
}
