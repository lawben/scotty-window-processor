package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

public class TumblingWindow implements ContextFreeWindow {

    private final WindowMeasure measure;
    /**
     * Size of the tumbling window
     */
    private final long size;
    private final long windowId;

    public TumblingWindow(WindowMeasure measure, long size) {
        this(measure, size, -1);
    }

    public TumblingWindow(WindowMeasure measure, long size, long windowId) {
        this.measure = measure;
        this.size = size;
        this.windowId = windowId;
    }

    @Override
    public long getWindowId() {
        return this.windowId;
    }


    public long getSize() {
        return size;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    @Override
    public long assignNextWindowStart(long recordStamp) {
        return recordStamp + getSize() - (recordStamp) % getSize();
    }

    @Override
    public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
        long lastStart = lastWatermark - ((lastWatermark + size) % size);
        for (long windowStart = lastStart; windowStart + size <= currentWatermark; windowStart += size) {
            WindowAggregateId windowAggregateId = new WindowAggregateId(this.getWindowId(), windowStart);
            aggregateWindows.setWindowAggregateId(windowAggregateId);
            aggregateWindows.trigger(windowStart, windowStart + size, measure);
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "TumblingWindow{" +
                "measure=" + measure +
                ", size=" + size +
                '}';
    }
}
