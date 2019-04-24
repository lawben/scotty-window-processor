package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DistributedWindowTest {
    private final ReduceAggregateFunction<Integer> SUM = Integer::sum;

    private SlicingWindowOperator<Integer> slicingWindowOperator1;
    private MemoryStateFactory stateFactory1;

    private SlicingWindowOperator<Integer> slicingWindowOperator2;
    private MemoryStateFactory stateFactory2;

    private SlicingWindowOperator<Integer> slicingWindowOperatorRoot;
    private MemoryStateFactory stateFactoryRoot;

    @Before
    public void setup() {
        this.stateFactory1 = new MemoryStateFactory();
        this.slicingWindowOperator1 = new SlicingWindowOperator<>(stateFactory1);

        this.stateFactory2 = new MemoryStateFactory();
        this.slicingWindowOperator2 = new SlicingWindowOperator<>(stateFactory2);

        this.stateFactoryRoot = new MemoryStateFactory();
        this.slicingWindowOperatorRoot = new SlicingWindowOperator<>(stateFactoryRoot);
    }

    @Test
    public void distributedTest() {
        slicingWindowOperator1.addWindowFunction(SUM);
        slicingWindowOperator2.addWindowFunction(SUM);
        slicingWindowOperatorRoot.addWindowFunction(SUM);

        slicingWindowOperator1.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator2.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperatorRoot.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        slicingWindowOperator1.processElement(1, 1);
        slicingWindowOperator1.processElement(3, 3);
        slicingWindowOperator1.processElement(5, 5);
        slicingWindowOperator1.processElement(11, 11);
        slicingWindowOperator1.processElement(13, 13);
        slicingWindowOperator1.processElement(15, 15);

        slicingWindowOperator2.processElement(2, 2);
        slicingWindowOperator2.processElement(4, 4);
        slicingWindowOperator2.processElement(6, 6);
        slicingWindowOperator2.processElement(12, 12);
        slicingWindowOperator2.processElement(14, 14);
        slicingWindowOperator2.processElement(16, 16);

        // First Window
        List<AggregateWindow> resultWindows1 = slicingWindowOperator1.processWatermark(10);
        List<AggregateWindow> resultWindows2 = slicingWindowOperator2.processWatermark(10);

        slicingWindowOperatorRoot.processElement((int) resultWindows1.get(0).getAggValues().get(0), 9);
        slicingWindowOperatorRoot.processElement((int) resultWindows2.get(0).getAggValues().get(0), 9);
        List<AggregateWindow> resultWindowsRoot = slicingWindowOperatorRoot.processWatermark(10);

        Assert.assertEquals(resultWindowsRoot.get(0).getAggValues().get(0), 21);

        // Second Window
        resultWindows1 = slicingWindowOperator1.processWatermark(20);
        resultWindows2 = slicingWindowOperator2.processWatermark(20);

        slicingWindowOperatorRoot.processElement((int) resultWindows1.get(0).getAggValues().get(0), 19);
        slicingWindowOperatorRoot.processElement((int) resultWindows2.get(0).getAggValues().get(0), 19);
        resultWindowsRoot = slicingWindowOperatorRoot.processWatermark(20);

        Assert.assertEquals(resultWindowsRoot.get(0).getAggValues().get(0), 81);
    }

    @Test
    public void distributedRootTest() {
        MemoryStateFactory stateFactory = new MemoryStateFactory();

        DistributedRoot<Integer> root = new DistributedRoot<>(stateFactory);

        DistributedChild<Integer> leftChild = new DistributedChild<>(stateFactory, root);
        DistributedChild<Integer> rightChild = new DistributedChild<>(stateFactory, root);

        final ReduceAggregateFunction<Integer> SUM = Integer::sum;

        root.addWindowFunction(SUM);
        leftChild.addWindowFunction(SUM);
        rightChild.addWindowFunction(SUM);

        root.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        leftChild.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        rightChild.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        leftChild.processElement(1, 1);
        leftChild.processElement(3, 3);
        leftChild.processElement(5, 5);
        leftChild.processElement(11, 11);
        leftChild.processElement(13, 13);
        leftChild.processElement(15, 15);

        rightChild.processElement(2, 2);
        rightChild.processElement(4, 4);
        rightChild.processElement(6, 6);
        rightChild.processElement(12, 12);
        rightChild.processElement(14, 14);
        rightChild.processElement(16, 16);

        // First Window
        List<AggregateWindow> leftWindows = leftChild.processWatermark(10);
        List<AggregateWindow> rightWindows = rightChild.processWatermark(10);
        List<AggregateWindow> rootWindows = root.processWatermark(10);
        Assert.assertEquals(rootWindows.get(0).getAggValues().get(0), 21);

        // Second Window
        List<AggregateWindow> leftWindows2 = leftChild.processWatermark(20);
        List<AggregateWindow> rightWindows2 = rightChild.processWatermark(20);
        List<AggregateWindow> rootWindows2 = root.processWatermark(20);
        Assert.assertEquals(rootWindows2.get(0).getAggValues().get(0), 81);
    }

    @Test
    public void distributedParallelRootTest() {
        MemoryStateFactory stateFactory = new MemoryStateFactory();

        DistributedRoot<Integer> root = new DistributedRoot<>(stateFactory);

        DistributedChild<Integer> leftChild = new DistributedChild<>(stateFactory, root);
        DistributedChild<Integer> rightChild = new DistributedChild<>(stateFactory, root);

        final ReduceAggregateFunction<Integer> SUM = Integer::sum;

        root.addWindowFunction(SUM);
        leftChild.addWindowFunction(SUM);
        rightChild.addWindowFunction(SUM);

        root.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10, 1));

        class LeftRunner implements Runnable {
            public void run() {
                leftChild.processElement(1, 1);
                leftChild.processElement(3, 3);
                leftChild.processElement(5, 5);
                leftChild.processElement(11, 11);
                leftChild.processElement(13, 13);
                leftChild.processElement(15, 15);

                leftChild.processWatermark(10);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                leftChild.processWatermark(20);
            }
        }

        class RightRunner implements Runnable {
            public void run() {
                rightChild.processElement(2, 2);
                rightChild.processElement(4, 4);
                rightChild.processElement(6, 6);
                rightChild.processElement(12, 12);
                rightChild.processElement(14, 14);
                rightChild.processElement(16, 16);

                rightChild.processWatermark(10);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                rightChild.processWatermark(20);
            }
        }

        LeftRunner leftRunner = new LeftRunner();
        RightRunner rightRunner = new RightRunner();
        new Thread(leftRunner).start();
        new Thread(rightRunner).start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // First Window
        List<AggregateWindow> rootWindows = root.processWatermark(10);
        Assert.assertEquals(rootWindows.get(0).getAggValues().get(0), 21);

        // Second Window
        List<AggregateWindow> rootWindows2 = root.processWatermark(20);
        Assert.assertEquals(rootWindows2.get(0).getAggValues().get(0), 21);
        Assert.assertEquals(rootWindows2.get(1).getAggValues().get(0), 81);
    }
}
