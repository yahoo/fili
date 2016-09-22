// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.util.GranularityParseException;

import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * StandardGranularityParser implements a time grain dictionary, as well as factory methods to dynamically build zoned
 * time grains.
 */
public class StandardGranularityParser implements GranularityParser {

    private final Map<String, Granularity> namedGrains;

    /**
     * Constructor.
     */
    public StandardGranularityParser() {
        namedGrains = Collections.unmodifiableMap(getGrainMap());
    }

    /**
     * This method loads default grains and can be extended to add customer grain extensions.
     *
     * @return A map of time grain api name to time grain instances.
     */
    protected Map<String, Granularity> getGrainMap() {
        Map<String, Granularity> result = Arrays.stream(DefaultTimeGrain.values())
                .collect(Collectors.toMap(DefaultTimeGrain::name, Function.identity()));
        result.put(AllGranularity.ALL_NAME.toUpperCase(Locale.ENGLISH), AllGranularity.INSTANCE);
        return result;
    }

    @Override
    public Granularity parseGranularity(String granularityName, DateTimeZone dateTimeZone)
            throws GranularityParseException {
        Granularity result = parseGranularity(granularityName);
        return (result instanceof ZonelessTimeGrain) ?
                ((ZonelessTimeGrain) result).buildZonedTimeGrain(dateTimeZone) :
                result;
    }

    @Override
    public Granularity parseGranularity(String granularityName) throws GranularityParseException {
        String key = granularityName.toUpperCase(Locale.ENGLISH);
        if (namedGrains.containsKey(key)) {
            return namedGrains.get(key);
        }
        throw new GranularityParseException(granularityName);
    }
}
