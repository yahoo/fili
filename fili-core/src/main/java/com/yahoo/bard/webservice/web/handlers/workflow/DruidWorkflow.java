// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.web.handlers.AsyncWebServiceRequestHandler;
import com.yahoo.bard.webservice.web.handlers.CacheRequestHandler;
import com.yahoo.bard.webservice.web.handlers.CacheV2RequestHandler;
import com.yahoo.bard.webservice.web.handlers.DataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.DebugRequestHandler;
import com.yahoo.bard.webservice.web.handlers.DruidPartialDataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.PaginationRequestHandler;
import com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.SplitQueryRequestHandler;
import com.yahoo.bard.webservice.web.handlers.DateTimeSortRequestHandler;
import com.yahoo.bard.webservice.web.handlers.TopNMapperRequestHandler;
import com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.WebServiceSelectorRequestHandler;
import com.yahoo.bard.webservice.web.handlers.WeightCheckRequestHandler;
import com.yahoo.bard.webservice.web.util.QueryWeightUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import javax.inject.Named;
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
public class DruidWorkflow implements RequestWorkflowProvider {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final int DRUID_UNCOVERED_INTERVAL_LIMIT = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit"),
            0
    );

    public static final String RESPONSE_WORKFLOW_TIMER = "ResponseWorkflow";
    public static final String REQUEST_WORKFLOW_TIMER = "RequestWorkflow";

    protected final @NotNull DataCache<?> dataCache;
    protected final @NotNull DruidWebService uiWebService;
    protected final @NotNull DruidWebService nonUiWebService;
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
     * @param uiWebService  Web Service to use for UI-path queries
     * @param nonUiWebService  WebService to use for Non-UI-Path queries
     * @param weightUtil  Utility for dealing with the weight check step
     * @param physicalTableDictionary  Collection of all physical tables
     * @param partialDataHandler  Handler for dealing with the partial data step
     * @param querySigningService  Service to sign a query based on it's segment metadata
     * @param volatileIntervalsService  Service to get volatile intervals from
     * @param mapper  JSON mapper
     */
    @Inject
    public DruidWorkflow(
            @NotNull DataCache<?> dataCache,
            @Named("uiDruidWebService") DruidWebService uiWebService,
            @Named("nonUiDruidWebService") DruidWebService nonUiWebService,
            QueryWeightUtil weightUtil,
            PhysicalTableDictionary physicalTableDictionary,
            PartialDataHandler partialDataHandler,
            QuerySigningService<?> querySigningService,
            VolatileIntervalsService volatileIntervalsService,
            ObjectMapper mapper
    ) {
        this.dataCache = dataCache;
        this.uiWebService = uiWebService;
        this.nonUiWebService = nonUiWebService;
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
        DataRequestHandler uiHandler = new AsyncWebServiceRequestHandler(uiWebService, mapper);
        DataRequestHandler nonUiHandler = new AsyncWebServiceRequestHandler(nonUiWebService, mapper);

        if (DRUID_UNCOVERED_INTERVAL_LIMIT > 0) {
            uiHandler = new DruidPartialDataRequestHandler(uiHandler);
            nonUiHandler = new DruidPartialDataRequestHandler(nonUiHandler);
        }

        // If query caching is enabled, the cache is checked before sending the request
        if (BardFeatureFlag.DRUID_CACHE.isOn()) {
            if (BardFeatureFlag.DRUID_CACHE_V2.isOn()) {
                uiHandler = new CacheV2RequestHandler(uiHandler, dataCache, querySigningService, mapper);
                nonUiHandler = new CacheV2RequestHandler(nonUiHandler, dataCache, querySigningService, mapper);
            } else {
                uiHandler = new CacheRequestHandler(uiHandler, dataCache, mapper);
                nonUiHandler = new CacheRequestHandler(nonUiHandler, dataCache, mapper);
            }
        }

        if (BardFeatureFlag.QUERY_SPLIT.isOn()) {
            uiHandler = new SplitQueryRequestHandler(uiHandler);
            nonUiHandler = new SplitQueryRequestHandler(nonUiHandler);
        }

        // Requests sent to the NonUI we service are checked to see if they are too heavy to process
        nonUiHandler = new WeightCheckRequestHandler(nonUiHandler, nonUiWebService, weightUtil, mapper);

        uiHandler = new DebugRequestHandler(uiHandler, mapper);
        nonUiHandler = new DebugRequestHandler(nonUiHandler, mapper);

        // Requests should be processed by UI or NonUI web services, select one
        DataRequestHandler handler = new WebServiceSelectorRequestHandler(
                uiWebService,
                nonUiWebService,
                uiHandler,
                nonUiHandler,
                mapper
         );

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
