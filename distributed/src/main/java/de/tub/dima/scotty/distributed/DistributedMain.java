package de.tub.dima.scotty.distributed;

import java.lang.reflect.Array;

public class DistributedMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Not enough arguments!\nUsage: java ... controllerPort windowPort");
            System.exit(1);
        }

        String[] childArgs = {"localhost", args[0], args[1]};
        DistributedChildMain.main(childArgs);

        // Need to start after children because this never finishes.
        DistributedRootMain.main(args);
    }
}
