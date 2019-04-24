package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.StateFactory;
import java.util.List;

public class DistributedChild<InputType> extends SlicingWindowOperator<InputType> {

    protected final DistributedRoot<InputType> root;

    public DistributedChild(StateFactory stateFactory, DistributedRoot<InputType> rootNode) {
        super(stateFactory);
        this.root = rootNode;
        this.root.addChildNode(this);
    }

    @Override
    public List<AggregateWindow> processWatermark(long watermarkTs) {
        List<AggregateWindow> resultWindows = windowManager.processWatermark(watermarkTs);
        for (AggregateWindow aggregateWindow : resultWindows) {
            AggregateWindow<InputType> typedWindow = (AggregateWindow<InputType>) aggregateWindow;
            root.processPreAggregate(typedWindow.getAggValues().get(0), typedWindow.getWindowAggregateId());
        }

        return resultWindows;
    }
}
