package de.tub.dima.scotty.slicing.slice;


import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.slicing.state.*;
import de.tub.dima.scotty.state.*;

import java.util.*;

public class LazySlice<InputType, ValueType> extends AbstractSlice<InputType, ValueType> {

    private final AggregateState<InputType> state;

    private final ArrayList<InputType> tuples;

    public LazySlice(StateFactory stateFactory, WindowManager windowManager, long startTs, long endTs, long startC, long endC, Type type) {
        super(startTs, endTs,startC, endC, type);
        this.state = new AggregateState<InputType>(stateFactory, windowManager.getAggregations());
        this.tuples = new ArrayList<>();
    }

    @Override
    public AggregateState getAggState() {
        return state;
    }

    @Override
    public void addElement(InputType element, long ts) {
        super.addElement(element, ts);
        state.addElement(element);
        tuples.add(element);
    }

    @Override
    public void removeElement(InputType element) {
        // remove and recompute state;
        this.tuples.remove(element);
        recompute();
    }

    private void recompute(){
        state.clear();
        for(InputType tuple: tuples){
            state.addElement(tuple);
        }
    }
}
