package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DistributedAggregateWindowState<AggregateType> implements AggregateWindow<AggregateType> {
    private final long startTimestamp;
    private final AggregateState<AggregateType> windowState;

    public DistributedAggregateWindowState(long startTimestamp, AggregateState<AggregateType> windowState) {
        this.startTimestamp = startTimestamp;
        this.windowState = windowState;
    }

    @Override
    public WindowMeasure getMeasure() {
        return null;
    }

    @Override
    public long getStart() {
        return startTimestamp;
    }

    @Override
    public long getEnd() {
        return -1;
    }

    @Override
    public List<AggregateType> getAggValues() {
        return windowState.getValues().stream()
                .map((Object value) -> (AggregateType) value)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public boolean hasValue() {
        return windowState.hasValues();
    }

    @Override
    public WindowAggregateId getWindowAggregateId() {
        return null;
    }
}
