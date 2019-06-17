package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.slicing.slice.*;
import de.tub.dima.scotty.state.*;

import java.io.*;
import java.util.*;

public class AggregateState<InputType> implements Serializable {

    private final List<AggregateValueState<InputType,Object,Object>> aggregateValueStates;
    private final AggregateFunction aggregateFunction;

    public AggregateState(StateFactory stateFactory, AggregateFunction aggregateFunction) {
        this(stateFactory, aggregateFunction, null);
    }

    public AggregateState(StateFactory stateFactory, AggregateFunction aggregateFunction, SetState<StreamRecord<InputType>> records) {
        this.aggregateFunction = aggregateFunction;
        this.aggregateValueStates = new ArrayList<>();
        this.aggregateValueStates.add(new AggregateValueState<>(stateFactory.createValueState(), aggregateFunction, records));
    }

    public void addElement(InputType state) {
        for(AggregateValueState<InputType,Object,Object> valueState: aggregateValueStates){
            valueState.addElement(state);
        }
    }

    public void removeElement(StreamRecord<InputType> toRemove){
        for(AggregateValueState<InputType,Object,Object> valueState: aggregateValueStates){
            valueState.removeElement(toRemove);
        }
    }

    public void clear() {
       for(AggregateValueState valueState: aggregateValueStates){
           valueState.clear();
       }
    }


    public void merge(AggregateState<InputType> otherAggState) {
        if (this.isMergeable(otherAggState)) {
            for (int i = 0; i < otherAggState.aggregateValueStates.size(); i++) {
                this.aggregateValueStates.get(i).merge(otherAggState.aggregateValueStates.get(i));
            }
        }
    }

    private boolean isMergeable(AggregateState otherAggState) {
        return otherAggState.aggregateValueStates.size() <= this.aggregateValueStates.size();
    }

    public boolean hasValues(){
        for(AggregateValueState<InputType,Object,Object> valueState: aggregateValueStates){
          if(valueState.hasValue()){
              return true;
          }
        }
        return false;
    }

    public List<Object> getValues() {
        List<Object> objectList = new ArrayList<>(aggregateValueStates.size());
        for(AggregateValueState<InputType,Object,Object> valueState: aggregateValueStates){
            if(valueState.hasValue())
                objectList.add(valueState.getValue());
        }
        return objectList;
    }

    public AggregateFunction getAggregateFunction() {
        return this.aggregateFunction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateState<?> that = (AggregateState<?>) o;
        return aggregateValueStates.equals(((AggregateState<?>) o).aggregateValueStates);
    }

    @Override
    public int hashCode() {

        return Objects.hash(aggregateValueStates);
    }

    @Override
    public String toString() {
        return aggregateValueStates.toString();
    }

}
