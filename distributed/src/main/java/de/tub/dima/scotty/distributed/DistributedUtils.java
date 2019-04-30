package de.tub.dima.scotty.distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class DistributedUtils {

    static byte[] objectToBytes(Object object) {
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

    static Object bytesToObject(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            ObjectInputStream in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (Exception e) {
            // The object is null, cannot convert it.
            e.printStackTrace();
            return null;
        }
    }

    static String buildTcpUrl(String ip, int port) {
        return "tcp://" + ip + ":" + port;
    }

    static String buildLocalTcpUrl(int port) {
        return buildTcpUrl("0.0.0.0", port);
    }
}
