// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility;

import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.DefaultingDictionary;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An implementation of VolatileIntervalsService.
 */
public class DefaultingVolatileIntervalsService implements VolatileIntervalsService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultingVolatileIntervalsService.class);

    /**
     * The default volatile intervals supply.
     */
    private final VolatileIntervalsFunction defaultIntervals;

    /**
     * The map of specific functions for physical tables.
     */
    private final Map<String, VolatileIntervalsFunction> intervalsFunctions;

    /**
     * Use the no op interval function with no customized functions.
     */
    public DefaultingVolatileIntervalsService() {
        this(NoVolatileIntervalsFunction.INSTANCE, Collections.emptyMap());
    }

    /**
     * Use a single default function for all physical tables.
     *
     * @param defaultIntervalsFunction  The volatile intervals function to apply to all physical tables.
     */
    public DefaultingVolatileIntervalsService(VolatileIntervalsFunction defaultIntervalsFunction) {
        this(defaultIntervalsFunction, Collections.emptyMap());
    }

    /**
     * Use the map of specific functions for physical tables.
     *
     * @param intervalsFunctions  the map of specific functions for physical tables
     *
     * @deprecated Simply use maps and default values
     */
    @Deprecated
    public DefaultingVolatileIntervalsService(
                DefaultingDictionary<PhysicalTable, VolatileIntervalsFunction> intervalsFunctions
    ) {
        this(intervalsFunctions.getDefaultValue(), intervalsFunctions);
    }

    /**
     * Use a default function with overrides for particular tables.
     *
     * @param defaultIntervalsFunction  The volatile intervals function to apply to physical tables
     * @param intervalsFunctions  A map of functions to provide volatile intervals for physical tables
     */
    public DefaultingVolatileIntervalsService(
            VolatileIntervalsFunction defaultIntervalsFunction,
            Map<PhysicalTable, VolatileIntervalsFunction> intervalsFunctions
    ) {
        this.defaultIntervals = defaultIntervalsFunction;
        this.intervalsFunctions = Collections.unmodifiableMap(
                intervalsFunctions.entrySet().stream()
                        .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue))
        );
    }

    @Override
    public SimplifiedIntervalList getVolatileIntervals(
            Granularity granularity,
            List<Interval> intervals,
            PhysicalTable factSource
    ) {
        SimplifiedIntervalList simplifiedIntervals = new SimplifiedIntervalList(intervals);
        SimplifiedIntervalList volatileIntervals = IntervalUtils.collectBucketedIntervalsIntersectingIntervalList(
                intervalsFunctions.getOrDefault(factSource.getName(), defaultIntervals).getVolatileIntervals(),
                simplifiedIntervals,
                granularity
        );
        if (granularity instanceof AllGranularity && !volatileIntervals.isEmpty()) {
            volatileIntervals = simplifiedIntervals;
        }
        LOG.trace("Volatile intervals: {} for grain {}", volatileIntervals, granularity);
        return volatileIntervals;
    }
}
