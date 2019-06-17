package de.tub.dima.scotty.slicing.aggregationstore.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.slicing.aggregationstore.AggregationStore;
import de.tub.dima.scotty.slicing.aggregationstore.LazyAggregateStore;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.slice.SliceFactory;
import de.tub.dima.scotty.slicing.state.AggregateWindowState;
import de.tub.dima.scotty.state.StateFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class LazyAggregateStoreTest {

    AggregationStore<Integer> aggregationStore;
    StateFactory stateFactory;
    WindowManager windowManager;
    SliceFactory<Integer, Integer> sliceFactory;

    @Before
    public void setup() {
        aggregationStore = new LazyAggregateStore<>();
        stateFactory = new StateFactoryMock();
        windowManager = new WindowManager(stateFactory, aggregationStore);
        sliceFactory = new SliceFactory<>(windowManager, stateFactory);
        windowManager.addAggregation(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }
        });
    }

    @Test
    public void getSliceByIndex() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(40, 50, new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);


        // tests
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.get(i), aggregationStore.getSlice(i));
        }

        assertEquals(list.get(list.size() - 1), aggregationStore.getCurrentSlice());

    }

    @Test
    public void findSliceByTs() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(40, 50, new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);


        // tests
        for (int i = 0; i < list.size(); i++) {
            Slice<Integer, Integer> expectedSlice = list.get(i);
            assertEquals(i, aggregationStore.findSliceIndexByTimestamp(expectedSlice.getTStart()));
            assertEquals(i, aggregationStore.findSliceIndexByTimestamp(expectedSlice.getTEnd() - 1));
            assertEquals(i, aggregationStore.findSliceIndexByTimestamp(expectedSlice.getTStart() + 5));
        }
    }


    @Test
    public void insertValue() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(40, 50,  new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);

        aggregationStore.insertValueToSlice(1, 1, 14);
        aggregationStore.insertValueToSlice(2, 2, 22);
        aggregationStore.insertValueToCurrentSlice(3, 22);

        assertNull(aggregationStore.getSlice(0).getAggState().getValues().get(0));
        assertEquals(aggregationStore.getSlice(1).getAggState().getValues().get(0), 1);

    }

    @Test
    public void aggregateWindow() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10,   new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(30, 40,  new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);

        aggregationStore.insertValueToSlice(1, 1, 14);
        aggregationStore.insertValueToSlice(2, 2, 22);
        aggregationStore.insertValueToCurrentSlice(3, 33);


        List<AggregateWindowState> window = new ArrayList<>();
        window.add(new AggregateWindowState(WindowMeasure.Time, stateFactory, windowManager.getAggregation(), new WindowAggregateId(0, 10, 40)));
        window.add(new AggregateWindowState(WindowMeasure.Time, stateFactory, windowManager.getAggregation(), new WindowAggregateId(0, 10, 20)));


    }

}
