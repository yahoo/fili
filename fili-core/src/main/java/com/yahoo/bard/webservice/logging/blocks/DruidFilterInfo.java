// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.druid.model.filter.ComplexFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Logs some structural data about the filter sent to Druid, without actually logging the entire (potentially massive)
 * filter.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DruidFilterInfo implements LogInfo {

    protected final Map<String, Long> numEachFilterType;

    /**
     * Constructor.
     *
     * @param filter  The filter that needs to be analyzed
     */
    public DruidFilterInfo(Filter filter) {
        numEachFilterType = buildFilterCount(filter);
    }

    /**
     * Performs a DFS search of the filter tree, populating the specified map with the number of instances of each
     * filter type appearing in the filter.
     *
     * @param filter  The filter that needs to be traversed
     *
     * @return A map containing a count of each type of filter
     */
    private Map<String, Long> buildFilterCount(Filter filter) {
        if (filter == null) {
            return Collections.emptyMap();
        }
        Map<String, Long> filterTypeCounter = new LinkedHashMap<>();
        Deque<Filter> filterStack = new ArrayDeque<>();
        filterStack.add(filter);
        while (!filterStack.isEmpty()) {
            Filter currentFilter = filterStack.pop();
            filterTypeCounter.merge(currentFilter.getClass().getSimpleName(), 1L, (old, increment) -> old + increment);
            if (currentFilter instanceof ComplexFilter) {
                filterStack.addAll(((ComplexFilter) currentFilter).getFields());
            }
        }
        return filterTypeCounter;
    }
}
