// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery;

import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;

/**
 * Query Weight Util provides features used for configuring and adjusting weight query thresholds.
 */
@Singleton
public class QueryWeightUtil {
    private static final Logger LOG = LoggerFactory.getLogger(QueryWeightUtil.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final @NotNull String QUERY_WEIGHT_LIMIT_KEY = SYSTEM_CONFIG
        .getPackageVariableName("query_weight_limit");

    private static final @NotNull String DAILY_SEGMENTATION_FACTOR_KEY = SYSTEM_CONFIG
        .getPackageVariableName("weight_segmentation_factor_daily");

    private static final @NotNull String WEEKLY_SEGMENTATION_FACTOR_KEY = SYSTEM_CONFIG
        .getPackageVariableName("weight_segmentation_factor_weekly");

    private static final @NotNull String MONTHLY_SEGMENTATION_FACTOR_KEY = SYSTEM_CONFIG
        .getPackageVariableName("weight_segmentation_factor_monthly");

    private static final @NotNull String ALL_SEGMENTATION_FACTOR_KEY = SYSTEM_CONFIG
            .getPackageVariableName("weight_segmentation_factor_all");

    private static final @NotNull String WEIGHT_CHECK_BYPASS_FACTOR_KEY = SYSTEM_CONFIG
        .getPackageVariableName("weight_check_bypass_factor");

    private static final @NotNull String DEFAULT_SEGMENTATION_FACTOR_KEY = SYSTEM_CONFIG
            .getPackageVariableName("weight_segmentation_factor_default");

    // The default weight limit for queries
    private static final long QUERY_WEIGHT_LIMIT_DEFAULT = 10000;

    // The default factor to divide the weight limit by for any unspecified grain
    private static final float SEGMENTATION_FACTOR_DEFAULT = 1;

    // The default factor to divide the weight limit by for daily grain
    private static final float DAILY_SEGMENTATION_FACTOR_DEFAULT = 1;

    // The default factor to divide the weight limit by for weekly grain
    private static final float WEEKLY_SEGMENTATION_FACTOR_DEFAULT = 3;

    // The default factor to divide the weight limit by for monthly grain
    private static final float MONTHLY_SEGMENTATION_FACTOR_DEFAULT = 5;

    // The default factor to divide the weight limit by for the 'all' grain
    private static final float ALL_SEGMENTATION_FACTOR_DEFAULT = 15;

    // The default factor to divide the weight threshold by to not require a weight check
    private static final float WEIGHT_CHECK_BYPASS_FACTOR_DEFAULT = 4;

    // The factor to divide the weight threshold by to not require a weight check
    private final float weightCheckBypassFactor = SYSTEM_CONFIG.getFloatProperty(
        WEIGHT_CHECK_BYPASS_FACTOR_KEY,
        WEIGHT_CHECK_BYPASS_FACTOR_DEFAULT);

    private final Map<ReadablePeriod, Long> weightLimitTimeMap;

    private final Long defaultRowLimit;
    private final Long allTimeRowLimit;

    /**
     * Constructor.
     */
    @Inject
    public QueryWeightUtil() {
        weightLimitTimeMap = new HashMap<>();

        // The factor to divide the weight limit by for daily grain
        float dailySegmentationFactor = SYSTEM_CONFIG.getFloatProperty(
                DAILY_SEGMENTATION_FACTOR_KEY,
                DAILY_SEGMENTATION_FACTOR_DEFAULT);

        // The factor to divide the weight limit by for weekly grain
        float weeklySegmentationFactor = SYSTEM_CONFIG.getFloatProperty(
                WEEKLY_SEGMENTATION_FACTOR_KEY,
                WEEKLY_SEGMENTATION_FACTOR_DEFAULT);

        // The factor to divide the weight limit by for monthly grain
        float monthlySegmentationFactor = SYSTEM_CONFIG.getFloatProperty(
                MONTHLY_SEGMENTATION_FACTOR_KEY,
                MONTHLY_SEGMENTATION_FACTOR_DEFAULT);

        // The factor to divide the weight limit by for the 'all' grain
        float allFactor = SYSTEM_CONFIG.getFloatProperty(ALL_SEGMENTATION_FACTOR_KEY, ALL_SEGMENTATION_FACTOR_DEFAULT);

        // The number of rows that the broker can safely be expected to handle, based on day grain testing
        long queryWeightLimit = SYSTEM_CONFIG.getLongProperty(
                QUERY_WEIGHT_LIMIT_KEY,
                QUERY_WEIGHT_LIMIT_DEFAULT
        );

        float defaultFactor = SYSTEM_CONFIG.getFloatProperty(
                DEFAULT_SEGMENTATION_FACTOR_KEY,
                SEGMENTATION_FACTOR_DEFAULT
        );

        defaultRowLimit = (long) (queryWeightLimit / defaultFactor);
        allTimeRowLimit = (long) (queryWeightLimit / allFactor);

        weightLimitTimeMap.put(HOUR.getPeriod(), queryWeightLimit);
        weightLimitTimeMap.put(DAY.getPeriod(), (long) (queryWeightLimit / dailySegmentationFactor));
        weightLimitTimeMap.put(WEEK.getPeriod(), (long) (queryWeightLimit / weeklySegmentationFactor));
        weightLimitTimeMap.put(MONTH.getPeriod(), (long) (queryWeightLimit / monthlySegmentationFactor));
    }

    /**
     * Get the weight threshold for the granularity.
     *
     * @param granularity  Granularity to get the threshold for.
     *
     * @return the threshold
     */
    public long getQueryWeightThreshold(Granularity granularity) {
        if (granularity instanceof AllGranularity) {
            return allTimeRowLimit;
        }
        ReadablePeriod period = ((TimeGrain) granularity).getPeriod();
        return weightLimitTimeMap.getOrDefault(period, defaultRowLimit);
    }

    /**
     * Indicate if the weight check query can be skipped based on heuristics.
     *
     * @param query  Query to test
     *
     * @return true if the weight check query does not need to be run
     */
    public boolean skipWeightCheckQuery(DruidAggregationQuery<?> query) {
        try {
            long worstCaseRows = WeightEvaluationQuery.getWorstCaseWeightEstimate(query);
            double skipThreshold = getQueryWeightThreshold(query.getGranularity()) / weightCheckBypassFactor;
            return worstCaseRows <= skipThreshold;
        } catch (ArithmeticException ignored) {
            // We got a really big estimate, so don't skip the check
            LOG.debug("worst case weight larger than {}", Long.MAX_VALUE);
            return false;
        }
    }

    /**
     * Get the weight check query for the given query.
     *
     * @param druidQuery  Druid query to convert to a weight check query
     *
     * @return the converted query
     */
    public WeightEvaluationQuery makeWeightEvaluationQuery(DruidAggregationQuery<?> druidQuery) {
        return WeightEvaluationQuery.makeWeightEvaluationQuery(druidQuery);
    }
}
