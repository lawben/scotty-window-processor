package de.tub.dima.scotty.distributed;

import java.util.Random;
import org.zeromq.ZMQ;

public interface EventGenerator<T> {
    long generateAndSendEvents(Random rand, ZMQ.Socket eventSender) throws Exception;
}
