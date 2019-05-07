package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DistributedUtils {

    public static byte[] objectToBytes(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[]{0};
    }

    public static Object bytesToObject(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            ObjectInputStream in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (Exception e) {
            // The object is null, cannot convert it.
            e.printStackTrace();
            return null;
        }
    }

    public static String buildTcpUrl(String ip, int port) {
        return "tcp://" + ip + ":" + port;
    }

    public static String buildLocalTcpUrl(int port) {
        return buildTcpUrl("0.0.0.0", port);
    }

    public static String buildIpcUrl(String path) {
        return "ipc://" + path;
    }

    public static Window buildWindowFromString(String windowString) {
        String[] windowDetails = windowString.split(",");
        assert windowDetails.length > 0;
        switch (windowDetails[0]) {
            case "TUMBLING": {
                assert windowDetails.length >= 2;
                final long size = Integer.parseInt(windowDetails[1]);
                final long windowId = windowDetails.length == 3 ? Integer.parseInt(windowDetails[2]) : -1;
                return new TumblingWindow(WindowMeasure.Time, size, windowId);
            }
            case "SLIDING": {
                assert windowDetails.length >= 3;
                final long size = Integer.parseInt(windowDetails[1]);
                final long slide = Integer.parseInt(windowDetails[2]);
                final long windowId = windowDetails.length == 4 ? Integer.parseInt(windowDetails[3]) : -1;
                return new SlidingWindow(WindowMeasure.Time, size, slide, windowId);
            }
            default: {
                System.err.println("No window type known for: '" + windowDetails[0] + "'");
                return null;
            }
        }
    }

    public static String windowIdToString(WindowAggregateId windowId) {
        return windowId.getWindowId() + "," +
               windowId.getWindowStartTimestamp() + "," +
               windowId.getWindowEndTimestamp();
    }

    public static WindowAggregateId stringToWindowId(String rawString) {
        List<Long> windowIdSplit = stringToLongs(rawString);
        assert windowIdSplit.size() == 3;
        return new WindowAggregateId(windowIdSplit.get(0), windowIdSplit.get(1), windowIdSplit.get(2));
    }

    public static List<Long> stringToLongs(String rawString) {
        String[] strings = rawString.split(",");
        List<Long> longs = new ArrayList<>(strings.length);
        for (String string : strings) {
            longs.add(Long.valueOf(string));
        }
        return longs;
    }

    public static List<Long> getRandomSeeds(String[] args, int numStreams, int position) {
        final List<Long> randomSeeds = new ArrayList<>();
        if (args.length >= position + 1) {
            String seedString = args[position];
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

    public static ReduceAggregateFunction<Integer> aggregateFunctionSum() {
        return (a, b) -> b == null ? a : a + b;
    }
}
