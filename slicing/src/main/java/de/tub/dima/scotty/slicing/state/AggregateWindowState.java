package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.slice.*;
import de.tub.dima.scotty.state.*;

import java.util.*;

public class AggregateWindowState implements AggregateWindow {

    private final long startTs;
    private final long endTs;
    private final WindowMeasure measure;

    private AggregateState windowState;

    public AggregateWindowState(long startTs, long endTs, WindowMeasure measure, StateFactory stateFactory, List<AggregateFunction> windowFunctionList) {
        this.startTs = startTs;
        this.endTs = endTs;
        this.windowState = new AggregateState(stateFactory, windowFunctionList);
        this.measure = measure;
    }

    public long getStart() {
        return startTs;
    }

    public long getEnd() {
        return endTs;
    }

    @Override
    public List getAggValue() {
        return windowState.getValues();
    }

    public void addState(AggregateState aggregationState) {
        this.windowState.merge(aggregationState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateWindowState that = (AggregateWindowState) o;
        return startTs == that.startTs &&
                endTs == that.endTs &&
                Objects.equals(windowState, that.windowState);
    }

    @Override
    public int hashCode() {

        return Objects.hash(startTs, endTs, windowState);
    }

    @Override
    public String toString() {
        return "AggregateWindowState{" +
                "startTs=" + startTs +
                ", endTs=" + endTs +
                ", windowState=" + windowState +
                '}';
    }

    public WindowMeasure getMeasure() {
        return measure;
    }

    public boolean containsSlice(Slice currentSlice) {
        if (measure == WindowMeasure.Time) {
            return this.getStart() <= currentSlice.getTStart() && (this.getEnd() > currentSlice.getTLast());
        }else{
            return this.getStart() <= currentSlice.getCStart() && (this.getEnd() >= currentSlice.getCLast());
        }
    }
}
