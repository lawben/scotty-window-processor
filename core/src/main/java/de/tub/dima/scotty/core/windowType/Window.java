package de.tub.dima.scotty.core.windowType;

import java.io.*;

public interface Window extends Serializable {
    int NO_ID = -1;

    WindowMeasure getWindowMeasure();

    default long getWindowId() {
        return NO_ID;
    }
}
