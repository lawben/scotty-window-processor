package de.tub.dima.scotty.distributed;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.state.DistributedAggregateWindowState;
import de.tub.dima.scotty.state.StateFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class DistributedWindowMerger<InputType> extends SlicingWindowOperator<InputType> {

    protected final int numChildren;
    protected final Map<WindowAggregateId, LongAdder> receivedWindowPreAggregates = new HashMap<>();
    protected final Map<WindowAggregateId, AggregateState<InputType>> windowAggregates = new HashMap<>();

    public DistributedWindowMerger(StateFactory stateFactory, int numChildren, List<Window> windows, AggregateFunction aggFn) {
        super(stateFactory);
        this.numChildren = numChildren;

        this.addWindowFunction(aggFn);
        for (Window window : windows) {
            this.addWindowAssigner(window);
        }
    }

    public boolean processPreAggregate(InputType preAggregate, WindowAggregateId windowAggregateId) {
        Optional<AggregateState<InputType>> presentAggWindow =
                Optional.ofNullable(windowAggregates.putIfAbsent(windowAggregateId,
                        new AggregateState<>(this.stateFactory, this.windowManager.getAggregations())));

        AggregateState<InputType> aggWindow = presentAggWindow.orElseGet(() -> windowAggregates.get(windowAggregateId));
        aggWindow.addElement(preAggregate);

        LongAdder receivedCounter = receivedWindowPreAggregates.computeIfAbsent(windowAggregateId, k -> new LongAdder());
        receivedCounter.increment();
        return receivedCounter.longValue() == numChildren;
    }

    public AggregateWindow<InputType> triggerFinalWindow(WindowAggregateId windowId) {
        AggregateWindow finalWindow = new DistributedAggregateWindowState<>(windowId, windowAggregates.get(windowId));

        receivedWindowPreAggregates.remove(windowId);
        windowAggregates.remove(windowId);

        return finalWindow;
    }
}
