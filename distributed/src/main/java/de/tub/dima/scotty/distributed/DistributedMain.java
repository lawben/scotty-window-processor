package de.tub.dima.scotty.distributed;

import java.lang.reflect.Array;

public class DistributedMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Not enough arguments!\nUsage: java ... controllerPort windowPort numChildren");
            System.exit(1);
        }

        String[] childArgs = {"localhost", args[0], args[1], args[2]};
        DistributedChildMain.main(childArgs);

        // Need to start after children because this only finishes after a long timeout.
        DistributedRootMain.main(args);
    }
}
