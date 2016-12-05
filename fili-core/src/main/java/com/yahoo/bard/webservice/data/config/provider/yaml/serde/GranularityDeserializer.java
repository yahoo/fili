// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml.serde;

import com.yahoo.bard.webservice.data.time.GranularityDictionary;
import com.yahoo.bard.webservice.data.time.StandardGranularityParser;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Locale;

/**
 * Deserialize a granularity string into an object, e.g. 'minute'
 *
 * FIXME: Currently only parses built-in granularities; probably should be extensible
 *
 * @see com.yahoo.bard.webservice.data.time.StandardGranularityParser
 */
public class GranularityDeserializer extends JsonDeserializer<Granularity> {

    protected static final GranularityDictionary DICTIONARY = StandardGranularityParser.getDefaultGrainMap();

    @Override
    public Granularity deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {
        JsonLocation currentLocation = jsonParser.getCurrentLocation();

        String granularityString = jsonParser.getValueAsString().toLowerCase(Locale.ENGLISH);
        Granularity granularity = DICTIONARY.get(granularityString);

        if (granularity == null) {
            throw new JsonParseException("Unable to parse granularity. Found: [" + granularityString + "]; available:" +
                    " " + DICTIONARY
                    .toString(), currentLocation);
        }

        return granularity;
    }
}
