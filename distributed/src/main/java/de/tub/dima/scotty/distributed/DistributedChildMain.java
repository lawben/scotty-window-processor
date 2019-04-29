package de.tub.dima.scotty.distributed;

public class DistributedChildMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Not enough arguments!\nUsage: java ... rootIp rootControllerPort rootWindowPort numChildren");
            System.exit(1);
        }

        final String rootIp = args[0];
        final int rootControllerPort = Integer.parseInt(args[1]);
        final int rootWindowPort = Integer.parseInt(args[2]);
        final int numChildren = Integer.parseInt(args[3]);

        for (int i = 0; i < numChildren; i++) {
            DistributedChild worker = new DistributedChild(rootIp, rootControllerPort, rootWindowPort, i);
            Thread thread = new Thread(worker);
            thread.start();
        }
    }
}
