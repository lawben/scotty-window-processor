package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.slice.*;
import de.tub.dima.scotty.state.*;

import java.util.*;

public class AggregateWindowState implements AggregateWindow {

    private final long start;
    private final long endTs;
    private final WindowMeasure measure;
    private final AggregateState windowState;
    private final WindowAggregateId windowAggregateId;
    private final AggregateFunction aggregateFunction;

    public AggregateWindowState(WindowMeasure measure, StateFactory stateFactory, AggregateFunction aggregateFunction, WindowAggregateId windowAggregateId) {
        this.windowState = new AggregateState(stateFactory, aggregateFunction);
        this.measure = measure;
        this.aggregateFunction = aggregateFunction;
        this.windowAggregateId = windowAggregateId;
        this.start = windowAggregateId.getWindowStartTimestamp();
        this.endTs = windowAggregateId.getWindowEndTimestamp();
    }

    public boolean containsSlice(Slice currentSlice) {
        if (measure == WindowMeasure.Time) {
            return this.getStart() <= currentSlice.getTStart() && (this.getEnd() > currentSlice.getTLast());
        } else {
            return this.getStart() <= currentSlice.getCStart() && (this.getEnd() >= currentSlice.getCLast());
        }
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return endTs;
    }

    @Override
    public List getAggValues() {
        return windowState.getValues();
    }

    @Override
    public boolean hasValue() {
        return windowState.hasValues();
    }

    @Override
    public WindowAggregateId getWindowAggregateId() {
        return this.windowAggregateId;
    }

    public void addState(AggregateState aggregationState) {
        this.windowState.merge(aggregationState);
    }

    @Override
    public WindowMeasure getMeasure() {
        return measure;
    }

    @Override
    public AggregateFunction getAggregateFunction() {
        return this.aggregateFunction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateWindowState that = (AggregateWindowState) o;
        return start == that.start &&
                endTs == that.endTs &&
                Objects.equals(windowState, that.windowState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, endTs, windowState);
    }

    @Override
    public String toString() {
        return "WindowResult(" +
                measure.toString() + ","+
                start + "-" + endTs +
                "," + windowState +
                ')';
    }
}
