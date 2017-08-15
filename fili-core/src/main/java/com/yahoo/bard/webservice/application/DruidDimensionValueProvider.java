// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidSearchQuery;
import com.yahoo.bard.webservice.druid.model.query.RegexSearchQuerySpec;
import com.yahoo.bard.webservice.druid.model.query.SearchQuerySpec;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The DruidDimensionRowProvider queries druid to update values for dimensions.
 */
public class DruidDimensionValueProvider extends AbstractDimensionValueProvider {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String DRUID_DIM_LOADER_DIMENSIONS =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_dimensions");
    public static final String DRUID_DIM_LOADER_ROW_LIMIT =
            SYSTEM_CONFIG.getPackageVariableName("druid_dim_loader_row_limit");
    private static final Integer ROW_LIMIT = SYSTEM_CONFIG.getIntProperty(DRUID_DIM_LOADER_ROW_LIMIT, 1000);

    private static final long TEN_YEARS_MILLIS = 10 * TimeUnit.DAYS.toMillis(365);
    private static final Duration DURATION = new Duration(TEN_YEARS_MILLIS);
    private static final Interval INTERVAL = new Interval(DURATION, DateTime.now());
    private static final String ANY_MATCH_PATTERN = ".*";
    private static final SearchQuerySpec SEARCH_QUERY_SPEC = new RegexSearchQuerySpec(ANY_MATCH_PATTERN);
    private final DruidWebService druidWebService;

    /**
     * DruidDimensionRowProvider fetches data from Druid and adds it to the dimension cache.
     * The dimensions loaded are taken from the system config.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimensions to update
     * @param druidWebService  The druid webservice to query
     */
    public DruidDimensionValueProvider(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            DruidWebService druidWebService
    ) {
        this(
                physicalTableDictionary,
                dimensionDictionary,
                //Our configuration framework automatically converts a comma-separated config value into a list.
                SYSTEM_CONFIG.getListProperty(DRUID_DIM_LOADER_DIMENSIONS),
                druidWebService
        );
    }

    /**
     * DruidDimensionRowProvider fetches data from Druid and adds it to the dimension cache.
     * The dimensions to be loaded can be passed in as a parameter.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimension dictionary to load dimensions from.
     * @param dimensionsToLoad  The dimensions to use.
     * @param druidWebService  The druid webservice to query.
     */
    public DruidDimensionValueProvider(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            List<String> dimensionsToLoad,
            DruidWebService druidWebService
    ) {
        super(physicalTableDictionary, dimensionDictionary, dimensionsToLoad);
        this.druidWebService = druidWebService;
    }

    @Override
    public void query(Dimension dimension, DataSource dataSource) {

        // Success callback will update the dimension cache
        SuccessCallback success = buildDruidDimensionsSuccessCallback(dimension);

        DruidSearchQuery druidSearchQuery = new DruidSearchQuery(
                dataSource,
                AllGranularity.INSTANCE,
                null,
                Collections.singletonList(INTERVAL),
                Collections.singletonList(dimension),
                SEARCH_QUERY_SPEC,
                null,
                ROW_LIMIT
        );

        RequestContext requestContext = new RequestContext(null, false);
        druidWebService.postDruidQuery(
                requestContext,
                success,
                getErrorCallback(),
                getFailureCallback(),
                druidSearchQuery
        );
    }

    /**
     * Build the callback to handle the successful druid query response.
     *
     * @param dimension  Dimension for which we are getting values
     *
     * @return the callback
     */
    private SuccessCallback buildDruidDimensionsSuccessCallback(Dimension dimension) {
        return rootNode -> {
            rootNode.forEach(intervalNode -> {
                intervalNode.get("result").forEach(dim -> {
                    String value = dim.get("value").asText();
                    if (dimension.findDimensionRowByKeyValue(value) == null) {
                        DimensionRow dimRow = dimension.createEmptyDimensionRow(value);
                        provideDimensionRow(dimension, dimRow);
                    }
                });
            });

            provideLoadedDimension(dimension);
        };
    }
}
