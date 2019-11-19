// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.CacheFeatureFlag;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.web.handlers.AsyncWebServiceRequestHandler;
import com.yahoo.bard.webservice.web.handlers.CacheRequestHandler;
import com.yahoo.bard.webservice.web.handlers.CacheV2RequestHandler;
import com.yahoo.bard.webservice.web.handlers.DataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.DateTimeSortRequestHandler;
import com.yahoo.bard.webservice.web.handlers.DebugRequestHandler;
import com.yahoo.bard.webservice.web.handlers.DruidPartialDataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.EtagCacheRequestHandler;
import com.yahoo.bard.webservice.web.handlers.PaginationRequestHandler;
import com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.SplitQueryRequestHandler;
import com.yahoo.bard.webservice.web.handlers.SqlRequestHandler;
import com.yahoo.bard.webservice.web.handlers.TopNMapperRequestHandler;
import com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.WebServiceSelectorRequestHandler;
import com.yahoo.bard.webservice.web.handlers.WeightCheckRequestHandler;
import com.yahoo.bard.webservice.web.util.QueryWeightUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;

/**
 * The Druid Workflow proceeds until a druid data request is returned or an error response is written.
 * <ul>
 *     <li>Partial data filtering is attached to the response. (Feature flagged)
 *     <li>Requests are routed by selecting a druid web service.
 *     <li>The cache is checked for responses matching the query. (Feature flagged)
 *     <li>Non UI requests may pass through an asynchronous druid query to test the aggregation cost.
 *     <li>Requests are sent asynchronously to the druid web service
 * </ul>
 */
@Singleton
public class SqlWorkflow implements RequestWorkflowProvider {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String RESPONSE_WORKFLOW_TIMER = "ResponseWorkflow";
    public static final String REQUEST_WORKFLOW_TIMER = "RequestWorkflow";
    private final int druidUncoveredIntervalLimit = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit"),
            0
    );

    protected final @NotNull DataCache<?> dataCache;
    protected final @NotNull DruidWebService webService;
    protected final @NotNull QueryWeightUtil weightUtil;
    protected final @NotNull PhysicalTableDictionary physicalTableDictionary;
    protected final @NotNull PartialDataHandler partialDataHandler;
    protected final @NotNull QuerySigningService<?> querySigningService;
    protected final @NotNull ObjectMapper mapper;
    protected final @NotNull VolatileIntervalsService volatileIntervalsService;

    /**
     * Constructor.
     *
     * @param dataCache  Response cache to use for caching Druid responses
     * @param webService  Web Service
     * @param weightUtil  Utility for dealing with the weight check step
     * @param physicalTableDictionary  Collection of all physical tables
     * @param partialDataHandler  Handler for dealing with the partial data step
     * @param querySigningService  Service to sign a query based on it's segment metadata
     * @param volatileIntervalsService  Service to get volatile intervals from
     * @param mapper  JSON mapper
     */
    @Inject
    public SqlWorkflow(
            @NotNull DataCache<?> dataCache,
            DruidWebService webService,
            QueryWeightUtil weightUtil,
            PhysicalTableDictionary physicalTableDictionary,
            PartialDataHandler partialDataHandler,
            QuerySigningService<?> querySigningService,
            VolatileIntervalsService volatileIntervalsService,
            ObjectMapper mapper
    ) {
        this.dataCache = dataCache;
        this.webService = webService;
        this.weightUtil = weightUtil;
        this.physicalTableDictionary = physicalTableDictionary;
        this.partialDataHandler = partialDataHandler;
        this.querySigningService = querySigningService;
        this.volatileIntervalsService = volatileIntervalsService;
        this.mapper = mapper;
    }

    @Override
    public DataRequestHandler buildWorkflow() {
        // The final stage of the workflow is to send a request to a druid web service
        DataRequestHandler handler = new AsyncWebServiceRequestHandler(webService, mapper);

        // If Druid sends uncoveredIntervals, missing intervals are checked before sending the request
        if (druidUncoveredIntervalLimit > 0) {
            handler = new DruidPartialDataRequestHandler(handler);
        }

        // If query caching is enabled, the cache is checked before sending the request
        if (CacheFeatureFlag.TTL.isOn()) {
            handler = new CacheRequestHandler(handler, dataCache, mapper);
        } else if (CacheFeatureFlag.LOCAL_SIGNATURE.isOn()) {
            handler = new CacheV2RequestHandler(handler, dataCache, querySigningService, mapper);
        } else if (CacheFeatureFlag.ETAG.isOn()) {
            handler = new EtagCacheRequestHandler(
                    handler,
                    (TupleDataCache<String, String, String>) dataCache,
                    mapper
            );
        }

        if (BardFeatureFlag.QUERY_SPLIT.isOn()) {
            handler = new SplitQueryRequestHandler(handler);
        }

        // Requests sent to the NonUI we service are checked to see if they are too heavy to process
        handler = new WeightCheckRequestHandler(handler, webService, weightUtil, mapper);

        handler = new DebugRequestHandler(handler, mapper);

        // Requests should be processed by UI or NonUI web services, select one
        handler = new WebServiceSelectorRequestHandler(
                webService,
                handler,
                mapper
        );

        handler = new SqlRequestHandler(handler, mapper);

        //The PaginationRequestHandler adds a mapper to the mapper chain that strips the result set down to just the
        //page desired. That mapper should be one of the last mappers to execute, so the handler that adds the mapper
        //to the chain needs to be one of the first handlers to execute.
        handler = new PaginationRequestHandler(handler);

        handler = new DateTimeSortRequestHandler(handler);

        handler = new TopNMapperRequestHandler(handler);

        handler = new PartialDataRequestHandler(handler, partialDataHandler);

        handler = new VolatileDataRequestHandler(
                handler,
                physicalTableDictionary,
                volatileIntervalsService
        );

        return handler;
    }
}
