package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.state.DistributedAggregateWindowState;
import de.tub.dima.scotty.state.StateFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class DistributedRoot<InputType> extends SlicingWindowOperator<InputType> {

    protected final List<SlicingWindowOperator<InputType>> childNodes;
    protected final Map<WindowAggregateId, LongAdder> receivedWindowPreAggregates = new HashMap<>();
    protected final Map<WindowAggregateId, AggregateState<InputType>> windowAggregates = new HashMap<>();



    public DistributedRoot(StateFactory stateFactory) {
        super(stateFactory);
        this.childNodes = new ArrayList<>();
    }

    public void addChildNode(SlicingWindowOperator<InputType> childNode) {
        this.childNodes.add(childNode);
    }

    public void processPreAggregate(InputType preAggregate, WindowAggregateId windowAggregateId) {
        synchronized (this) {
            receivedWindowPreAggregates.computeIfAbsent(windowAggregateId, k -> new LongAdder()).increment();
            Optional<AggregateState<InputType>> presentAggWindow =
                    Optional.ofNullable(windowAggregates.putIfAbsent(windowAggregateId,
                            new AggregateState<>(this.stateFactory, this.windowManager.getAggregations())));

            AggregateState<InputType> aggWindow = presentAggWindow.orElseGet(() -> windowAggregates.get(windowAggregateId));
            aggWindow.addElement(preAggregate);
        }

    }

    @Override
    public List<AggregateWindow> processWatermark(long watermarkTs) {
        List<AggregateWindow> aggregatedWindows = new ArrayList<>();
        List<WindowAggregateId> toRemove = new ArrayList<>();

        receivedWindowPreAggregates.forEach((windowId, counter) -> {
            if (windowId.getWindowStartTimestamp() < watermarkTs) {
                assert counter.longValue() == childNodes.size();
                aggregatedWindows.add(new DistributedAggregateWindowState<>(
                        windowId.getWindowStartTimestamp(), windowAggregates.get(windowId)));
                toRemove.add(windowId);
            }
        });

        toRemove.forEach((id) -> {
            receivedWindowPreAggregates.remove(id);
            windowAggregates.remove(id);
        });

        return aggregatedWindows;
    }

    @Override
    public <Agg, OutputType> void addWindowFunction(AggregateFunction<InputType, Agg, OutputType> windowFunction) {
        super.addWindowFunction(windowFunction);

        for (SlicingWindowOperator<InputType> child : childNodes) {
            child.addWindowFunction(windowFunction);
        }
    }

    @Override
    public void addWindowAssigner(Window window) {
        super.addWindowAssigner(window);

        for (SlicingWindowOperator<InputType> child : childNodes) {
            child.addWindowAssigner(window);
        }
    }
}
