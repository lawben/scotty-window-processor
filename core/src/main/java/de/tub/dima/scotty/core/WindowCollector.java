package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowType.WindowMeasure;

public interface WindowCollector {

    void trigger(WindowAggregateId windowAggregateId, WindowMeasure measure);
}
