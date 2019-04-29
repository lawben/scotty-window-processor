package de.tub.dima.scotty.core;

import java.util.Objects;

public class WindowAggregateId {
    private final long windowId;
    private final long windowStartTimestamp;

    public WindowAggregateId(long windowId, long windowStartTimestamp) {
        this.windowId = windowId;
        this.windowStartTimestamp = windowStartTimestamp;
    }

    public long getWindowId() {
        return windowId;
    }

    public long getWindowStartTimestamp() {
        return windowStartTimestamp;
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
                windowStartTimestamp == that.windowStartTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, windowStartTimestamp);
    }

    @Override
    public String toString() {
        return "WindowAggregateId{" +
                "windowId=" + windowId +
                ", windowStartTimestamp=" + windowStartTimestamp +
                '}';
    }
}
