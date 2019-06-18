package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DistributedAggregateWindowState<AggregateType> implements AggregateWindow<AggregateType> {
    private final WindowAggregateId windowId;
    private final AggregateState<AggregateType> windowState;

    public DistributedAggregateWindowState(WindowAggregateId windowId, AggregateState<AggregateType> windowState) {
        this.windowId = windowId;
        this.windowState = windowState;
    }

    @Override
    public WindowMeasure getMeasure() {
        return WindowMeasure.Time;
    }

    @Override
    public long getStart() {
        return this.windowId.getWindowStartTimestamp();
    }

    @Override
    public long getEnd() {
        return this.windowId.getWindowEndTimestamp();
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
        return this.windowId;
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return this.windowState.getAggregateFunctions();
    }
}
