// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.application.Loader;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.table.ConcretePhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Singleton;

/**
 * Segment Metadata Loader sends requests to the Druid segment metadata endpoint ('datasources') and returns interval
 * maps to column availability. It then builds Segment Metadata objects which pivot this data into columns of intervals
 * and then updates the Physical Table instances in the physical table dictionary.
 * <p>
 * Note that this uses the old dataSources endpoint that queries the Broker.
 *
 * @deprecated The http endpoints in Druid that this loader relies on have been deprecated
 */
@Singleton
@Deprecated
public class SegmentMetadataLoader extends Loader<Boolean> {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String SEGMENT_METADATA_QUERY_FORMAT = "/datasources/%s?full&interval=1970-01-01/3000-01-01";
    public static final String DRUID_SEG_LOADER_TIMER_DURATION_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_seg_loader_timer_duration");
    public static final String DRUID_SEG_LOADER_TIMER_DELAY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_seg_loader_timer_delay");

    private static final Logger LOG = LoggerFactory.getLogger(SegmentMetadataLoader.class);

    private final DruidWebService druidWebService;
    private final PhysicalTableDictionary physicalTableDictionary;
    private final DimensionDictionary dimensionDictionary;
    private final AtomicReference<DateTime> lastRunTimestamp;
    private final ObjectMapper mapper;

    protected final HttpErrorCallback errorCallback;
    protected final FailureCallback failureCallback;

    /**
     * Segment Metadata Loader fetches data from a webservice endpoint and updates the dimensions on that table.
     *
     * @param physicalTableDictionary  The physical tables to update
     * @param dimensionDictionary  The dimensions to update
     * @param druidWebService  The druid webservice to query
     * @param mapper  Object mapper to parse druid metadata
     */
    public SegmentMetadataLoader(
        PhysicalTableDictionary physicalTableDictionary,
        DimensionDictionary dimensionDictionary,
        DruidWebService druidWebService,
        ObjectMapper mapper
    ) {
        super(
                SegmentMetadataLoader.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(DRUID_SEG_LOADER_TIMER_DELAY_KEY, 0),
                SYSTEM_CONFIG.getLongProperty(
                        DRUID_SEG_LOADER_TIMER_DURATION_KEY,
                        TimeUnit.MINUTES.toMillis(1)
                )
        );

        this.physicalTableDictionary = physicalTableDictionary;
        this.dimensionDictionary = dimensionDictionary;
        this.druidWebService = druidWebService;
        this.mapper = mapper;
        this.errorCallback = getErrorCallback();
        this.failureCallback = getFailureCallback();

        lastRunTimestamp = new AtomicReference<>();
    }

    @Override
    public void run() {
        physicalTableDictionary.values().stream()
                .peek(table -> LOG.trace("Querying segment metadata for table: {}", table))
                .filter(table -> table instanceof ConcretePhysicalTable)
                .map(table -> (ConcretePhysicalTable) table)
                .forEach(this::querySegmentMetadata);
        lastRunTimestamp.set(DateTime.now());
    }

    /**
     * Queries the data mart for updated Segment Metadata and ten updates the physical table.
     *
     * @param table  The physical table to be updated.
     */
    protected void querySegmentMetadata(ConcretePhysicalTable table) {
        String resourcePath = String.format(SEGMENT_METADATA_QUERY_FORMAT, table.getFactTableName());

        // Success callback will update segment metadata on success
        SuccessCallback success = buildSegmentMetadataSuccessCallback(table);
        druidWebService.getJsonObject(success, errorCallback, failureCallback, resourcePath);
    }

    /**
     * Callback to parse druid segment metadata response.
     * <p>
     * Typical druid segment metadata response:
     * <pre>
     *  """{
     *       "2014-09-04T00:00:00.000Z/2014-09-05T00:00:00.000Z":
     *       {
     *          "dimensions":[ "color", "shape" ],
     *          "metrics": [ "height","width" ]
     *       },
     *       "2014-09-05T00:00:00.000Z/2014-09-11T00:00:00.000Z":
     *       {
     *          "dimensions":[ "color", "shape" ],
     *          "metrics": [ "height" ]
     *       },
     *       "2014-09-11T00:00:00.000Z/2014-10-01T00:00:00.000Z":
     *       {
     *          "dimensions":[ "color", "shape" ],
     *          "metrics": [ "height","width" ]
     *       }
     *   }"""
     * </pre>
     *
     * @param table  The table to inject into this callback.
     *
     * @return The callback itself.
     */
    protected final SuccessCallback buildSegmentMetadataSuccessCallback(ConcretePhysicalTable table) {
        return new SuccessCallback() {
            @Override
            public void invoke(JsonNode rootNode) {
                RawSegmentMetadataTypeReference typeReference = new RawSegmentMetadataTypeReference();
                SegmentMetadata segmentMetadata = new SegmentMetadata(
                        mapper.<RawSegmentMetadataConcrete>convertValue(rootNode, typeReference)
                );
                if (segmentMetadata.isEmpty()) {
                    LOG.warn("Empty segment metadata detected when loading table '{}'", table.getFactTableName());
                }
            }
        };
    }

    /**
     * Return when this loader ran most recently.
     *
     * @return The date and time of the most recent execution of this loader.
     */
    public DateTime getLastRunTimestamp() {
        return lastRunTimestamp.get();
    }

    /**
     * Type-def to give a more useful name to a Map of String to Map of String to List of String.
     */
    @SuppressWarnings("serial")
    protected class RawSegmentMetadataConcrete extends HashMap<String, Map<String, List<String>>> { }

    /**
     * Type Reference to aid in Jackson processing.
     */
    protected class RawSegmentMetadataTypeReference extends TypeReference<Map<String, Map<String, List<String>>>> { }
}
