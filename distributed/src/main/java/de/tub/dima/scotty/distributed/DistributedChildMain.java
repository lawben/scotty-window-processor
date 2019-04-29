package de.tub.dima.scotty.distributed;

public class DistributedChildMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Not enough arguments!\nUsage: java ... rootIp rootControllerPort rootWindowPort");
            System.exit(1);
        }

        final String rootIp = args[0];
        final int rootControllerPort = Integer.parseInt(args[1]);
        final int rootWindowPort = Integer.parseInt(args[2]);
        DistributedChild worker1 = new DistributedChild(rootIp, rootControllerPort, rootWindowPort);
        DistributedChild worker2 = new DistributedChild(rootIp, rootControllerPort, rootWindowPort);

        Thread thread1 = new Thread(worker1);
        Thread thread2 = new Thread(worker2);
        thread1.start();
        thread2.start();
    }
}
