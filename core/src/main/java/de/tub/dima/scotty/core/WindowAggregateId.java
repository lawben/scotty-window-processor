package de.tub.dima.scotty.core;

import java.util.Objects;

public class WindowAggregateId {
    private final long windowId;
    private final long windowStartTimestamp;
    private final long windowEndTimestamp;

    public WindowAggregateId(long windowId, long windowStartTimestamp, long windowEndTimestamp) {
        this.windowId = windowId;
        this.windowStartTimestamp = windowStartTimestamp;
        this.windowEndTimestamp = windowEndTimestamp;
    }

    public long getWindowId() {
        return windowId;
    }

    public long getWindowStartTimestamp() {
        return windowStartTimestamp;
    }

    public long getWindowEndTimestamp() {
        return windowEndTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowAggregateId that = (WindowAggregateId) o;
        return windowId == that.windowId &&
                windowStartTimestamp == that.windowStartTimestamp &&
                windowEndTimestamp == that.windowEndTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, windowStartTimestamp, windowEndTimestamp);
    }

    @Override
    public String toString() {
        return "WindowAggregateId{" +
                "windowId=" + windowId +
                ", windowStartTimestamp=" + windowStartTimestamp +
                ", windowEndTimestamp=" + windowEndTimestamp +
                '}';
    }
}
