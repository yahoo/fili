// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.util.ImmutableWrapperMap;
import com.yahoo.bard.webservice.table.Column;

import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * An availability which guarantees immutability on its contents.
 */
public class ImmutableAvailability extends ImmutableWrapperMap<Column, List<Interval>> implements Availability {

    /**
     * Constructor.
     *
     * @param map A map of columns to lists of available intervals
     */
    public ImmutableAvailability(Map<Column, List<Interval>> map) {
        super(map);
    }
}
