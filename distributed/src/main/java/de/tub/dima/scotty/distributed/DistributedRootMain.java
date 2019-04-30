package de.tub.dima.scotty.distributed;

public class DistributedRootMain {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Not enough arguments!\nUsage: java ... controllerPort windowPort numChildren");
            System.exit(1);
        }

        final int rootControllerPort = Integer.parseInt(args[0]);
        final int rootWindowPort = Integer.parseInt(args[1]);
        final int numChildren = Integer.parseInt(args[2]);
        runRoot(rootControllerPort, rootWindowPort, numChildren);
    }

    public static void runRoot(int controllerPort, int windowPort, int numChildren) {
        DistributedRoot worker = new DistributedRoot(controllerPort, windowPort, numChildren);
        Thread thread = new Thread(worker);
        thread.start();
    }
}
