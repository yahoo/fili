// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.util.StreamUtils;

import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

/**
 * StandardGranularityParser implements a time grain dictionary, as well as factory methods to dynamically build zoned
 * time grains.
 */
public class StandardGranularityParser implements GranularityParser {

    private final GranularityDictionary namedGranularities;

    /**
     * Constructor.
     * <p>
     * @param dictionary  a dictionary containing names mapped to granularities
     */
    @Inject
    public StandardGranularityParser(@NotNull GranularityDictionary dictionary) {
        assert dictionary != null;
        namedGranularities = dictionary;
    }

    /**
     * Constructor.
     * <p>
     * Use a default grain map with the enum name()s and the all granularity.
     */
    public StandardGranularityParser() {
        this(getDefaultGrainMap());
    }

    /**
     * This method loads default grains and can be extended to add customer grain extensions.
     * <p>
     * @return A map of time grain api name to time grain instances.
     */
    public static GranularityDictionary getDefaultGrainMap() {
        return Stream.concat(
               Stream.of(AllGranularity.INSTANCE),
               Arrays.stream(DefaultTimeGrain.values())
        ).collect(
               StreamUtils.toDictionary(
                       Granularity::getName,
                       GranularityDictionary::new
               )
        );
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
        String key = granularityName.toLowerCase(Locale.ENGLISH);
        if (namedGranularities.containsKey(key)) {
            return namedGranularities.get(key);
        }
        throw new GranularityParseException(granularityName);
    }
}
