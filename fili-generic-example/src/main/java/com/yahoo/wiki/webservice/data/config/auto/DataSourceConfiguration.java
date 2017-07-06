// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.util.Utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the minimum necessary configuration necessary to set up fili to
 * make requests to druid. This defines all metrics, dimensions, and *one*
 * valid time grain of a datasource.
 * Note the restriction to one time grain, a druid datasource could have
 * more than one, but it *should* have just a single grain.
 */
public interface DataSourceConfiguration {
    /**
     * Gets the name of a datasource as would be stored in Druid.
     *
     * @return the name of the datasource.
     */
    String getPhysicalTableName();

    String getApiTableName();

    /**
     * Gets the name of the datasource to be used as a {@link TableName} in fili.
     *
     * @return the {@link TableName} for this datasource.
     */
    default TableName getTableName() {
        return this::getApiTableName;
    }

    /**
     * Gets the names of all the metrics for the current datasource.
     *
     * @return a list of names of metrics for the current datasource.
     */
    List<String> getMetrics();

    default Set<MetricConfig> getMetricConfigs() {
        TimeGrain timeGrain = getZonedTimeGrain().getBaseTimeGrain();
        return getMetrics().stream()
                .map(metric -> new MetricConfig(
                        metric,
                        metric,
                        Collections.singletonList(timeGrain),
                        MetricConfig.MetricMakerType.DOUBLE_SUM
                ))
                .collect(Collectors.toSet());
    }

    /**
     * Gets the names of all the dimensions for the current datasource.
     *
     * @return a list of names of dimensions for the current datasource.
     */
    List<String> getDimensions();

    default Set<DimensionConfig> getDimensionConfigs() {
        return getDimensions().stream()
                .map(dimensionName -> new DefaultKeyValueStoreDimensionConfig(
                                () -> dimensionName,
                                dimensionName,
                                "",
                                dimensionName,
                                "General",
                                Utils.asLinkedHashSet(DefaultDimensionField.ID),
                                MapStoreManager.getInstance(dimensionName),
                                ScanSearchProviderManager.getInstance(dimensionName)
                        )
                )
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                Collections::unmodifiableSet
                        )
                );
    }

    ZonedTimeGrain getZonedTimeGrain();

    /**
     * Gets the {@link TimeGrain} which is valid for use in queries.
     * Note: In theory, a Datasource in Druid could have more than 1 grain at
     * different times, but for now we only want to support one.
     *
     * @return a {@link TimeGrain} for the current table.
     */
    List<TimeGrain> getValidTimeGrains();

    /**
     * Creates a set of valid granularities from valid timegrains.
     *
     * @return set of valid granularities.
     */
    default Set<Granularity> getGranularities() {
        Set<Granularity> granularities = new HashSet<>(getValidTimeGrains());
        granularities.add(AllGranularity.INSTANCE);
        return granularities;
    }
}
