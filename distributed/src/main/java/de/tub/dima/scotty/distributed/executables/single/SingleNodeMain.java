package de.tub.dima.scotty.distributed.executables.single;

import de.tub.dima.scotty.distributed.ResultListener;
import de.tub.dima.scotty.distributed.single.SingleNode;

public class SingleNodeMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Not enough arguments!\nUsage: java ... rootPort resultPath");
            System.exit(1);
        }

        final int rootPort = Integer.parseInt(args[0]);
        final String resultPath = args[1];

        runSingleNode(rootPort, resultPath);
    }

    public static void runSingleNode(int rootPort, String resultPath) {
        ResultListener resultListener = new ResultListener(resultPath);
        Thread resultThread = new Thread(resultListener);
        resultThread.start();

        SingleNode worker = new SingleNode(rootPort, resultPath);
        Thread thread = new Thread(worker);
        thread.start();
    }
}
