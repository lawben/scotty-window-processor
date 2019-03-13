package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.state.memory.*;
import org.junit.*;

import java.util.*;

public class SlidingWindowOperatorTest {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
    }

    @Test
    public void inOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        // 1,10 ; 5-15; 20-30---
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertNull(resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(2).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(4).getAggValues().get(0));
    }

    @Test
    public void inOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        slicingWindowOperator.processElement(1, 0);
        slicingWindowOperator.processElement(2, 0);
        slicingWindowOperator.processElement(3, 20);
        slicingWindowOperator.processElement(4, 30);
        slicingWindowOperator.processElement(5, 40);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertNull(resultWindows.get(1).getAggValues().get(0));
        Assert.assertNull(resultWindows.get(2).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(4).getAggValues().get(0));
    }


    @Test
    public void inOrderTwoWindowsTest() {
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(null, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(3).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(4).getAggValues().get(0));
        Assert.assertEquals(7, resultWindows.get(5).getAggValues().get(0));
    }

    @Test
    public void inOrderTwoWindowsDynamicTest() {

        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(null, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(3).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(4).getAggValues().get(0));
        Assert.assertEquals(7, resultWindows.get(5).getAggValues().get(0));
    }

    @Test
    public void inOrderTwoWindowsDynamicTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);


        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));


        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);


        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(7, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(4).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(5).getAggValues().get(0));
    }


    @Test
    public void outOfOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 20);
        slicingWindowOperator.processElement(1, 23);
        slicingWindowOperator.processElement(1, 25);

        slicingWindowOperator.processElement(1, 45);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertNull(resultWindows.get(1).getAggValues().get(0));
        Assert.assertNull(resultWindows.get(2).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(1, resultWindows.get(2).getAggValues().get(0));
        Assert.assertNull( resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(1, resultWindows.get(4).getAggValues().get(0));
    }


}
