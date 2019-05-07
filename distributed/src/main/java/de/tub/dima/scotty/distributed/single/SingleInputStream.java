package de.tub.dima.scotty.distributed.single;

import de.tub.dima.scotty.distributed.EventGenerator;
import de.tub.dima.scotty.distributed.InputStream;
import de.tub.dima.scotty.distributed.InputStreamConfig;
import org.zeromq.ZContext;

public class SingleInputStream<T> extends InputStream<T> {
    public SingleInputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort, EventGenerator<T> eventGenerator) {
        super(streamId, config, nodeIp, nodePort, eventGenerator);
    }

    @Override
    protected void registerAtNode(ZContext context) {
        // Do nothing
    }
}
