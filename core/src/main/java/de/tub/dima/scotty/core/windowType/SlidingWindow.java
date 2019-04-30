package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

public class SlidingWindow implements ContextFreeWindow {

    private final WindowMeasure measure;
    private final long windowId;

    /**
     * Size of the sliding window
     */
    private final long size;

    /**
     * The window slide step
     */
    private final long slide;

    public SlidingWindow(WindowMeasure measure, long size, long slide) {
        this(measure, size, slide, -1);
    }

    public SlidingWindow(WindowMeasure measure, long size, long slide, long windowId) {
        this.measure = measure;
        this.size = size;
        this.slide = slide;
        this.windowId = windowId;
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    @Override
    public long getWindowId() {
        return this.windowId;
    }

    @Override
    public long assignNextWindowStart(long recordStamp) {
        return recordStamp + getSlide() - (recordStamp) % getSlide();
    }

    public static long getWindowStartWithOffset(long timestamp, long windowSize) {
        return timestamp - (timestamp  + windowSize) % windowSize;
    }

    @Override
    public void triggerWindows(WindowCollector collector, long lastWatermark, long currentWatermark) {
        long lastStart  = getWindowStartWithOffset(currentWatermark, slide);

        for (long windowStart = lastStart; windowStart + size > lastWatermark; windowStart -= slide) {
            if (windowStart >= 0 && windowStart + size <= currentWatermark + 1) {
                WindowAggregateId windowAggregateId = new WindowAggregateId(this.getWindowId(), windowStart);
                collector.setWindowAggregateId(windowAggregateId);
                collector.trigger(windowStart, windowStart + size, measure);
            }
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "SlidingWindow{" +
                "measure=" + measure +
                ", size=" + size +
                ", slide=" + slide +
                '}';
    }
}
