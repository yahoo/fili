// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.druid.model.metadata.ShardSpecMixIn;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import org.joda.time.Interval;

import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;

/**
 * Gathers all the object mappers that are used for object serialization/deserialization.
 */
public class ObjectMappersSuite {

    private final ObjectMapper jsonMapper;
    private final CsvMapper csvMapper;
    private final ObjectMapper metadataMapper;

    /**
     * No-arg constructor.
     */
    public ObjectMappersSuite() {
        jsonMapper = new ObjectMapper();
        csvMapper = new CsvMapper();


        JodaModule jodaModule = new JodaModule();
        jodaModule.addSerializer(Interval.class, new ToStringSerializer());
        jodaModule.setMixInAnnotation(ShardSpec.class, ShardSpecMixIn.class);
        jsonMapper.registerModule(jodaModule);
        jsonMapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(false));
        jsonMapper.registerModule(new AfterburnerModule());

        InjectableValues.Std injectableValues = new InjectableValues.Std();
        injectableValues.addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);
        jsonMapper.setInjectableValues(injectableValues);

        metadataMapper = jsonMapper.copy();
        metadataMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    /**
     * Get the default mapper.
     * This is the JSON object mapper.
     *
     * @return the instance of ObjectMapper
     */
    public ObjectMapper getMapper() {
        return jsonMapper;
    }

    /**
     * Get the mapper for reading druid metadata.
     * This is the JSON object mapper.
     *
     * @return the instance of ObjectMapper
     */
    public ObjectMapper getMetadataMapper() {
        return metadataMapper;
    }

    /**
     * Get the CSV mapper.
     *
     * @return the instance of ObjectMapper
     */
    public CsvMapper getCsvMapper() {
        return csvMapper;
    }
}
