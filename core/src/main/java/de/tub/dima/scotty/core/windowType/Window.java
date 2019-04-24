package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

import java.io.*;

public interface Window extends Serializable {
    int NO_ID = -1;

    WindowMeasure getWindowMeasure();

    default int getWindowId() {
        return NO_ID;
    }
}
