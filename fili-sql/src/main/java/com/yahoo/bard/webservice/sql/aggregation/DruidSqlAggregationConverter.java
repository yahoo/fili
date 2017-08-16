// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * The default implementation of  mapping from Druid's {@link Aggregation} to a {@link SqlAggregation}.
 */
public class DruidSqlAggregationConverter
        implements BiFunction<Aggregation, ApiToFieldMapper, Optional<SqlAggregation>> {
    private Map<String, SqlAggregationType> druidToSqlAggregation;

    /**
     * Constructors a map from druid to sql aggregations using {@link #getDefaultDruidToSqlAggregation()}.
     */
    public DruidSqlAggregationConverter() {
        this(getDefaultDruidToSqlAggregation());
    }

    /**
     * Use the given map for converting from druid to sql aggregations.
     *
     * @param druidToSqlAggregation  The mapping from Druid aggregation name to Sql aggregation type.
     */
    public DruidSqlAggregationConverter(Map<String, SqlAggregationType> druidToSqlAggregation) {
        this.druidToSqlAggregation = druidToSqlAggregation;
    }

    /**
     * The default mapping from druid to sql aggregations defined by {@link DefaultSqlAggregationType}.
     *
     * @return the mapping from druid aggregation names to sql aggregation types.
     */
    public static Map<String, SqlAggregationType> getDefaultDruidToSqlAggregation() {
        Map<String, SqlAggregationType> druidToSqlAggregation = new HashMap<>();
        Arrays.stream(DefaultSqlAggregationType.values())
                .forEach(defaultSqlAggregationType -> {
                    defaultSqlAggregationType.getSupportedDruidAggregations()
                            .forEach(druidAggregation -> {
                                druidToSqlAggregation.put(druidAggregation, defaultSqlAggregationType);
                            });
                });
        return druidToSqlAggregation;
    }

    /**
     * Finds the corresponding {@link SqlAggregation} from a druid aggregation.
     *
     * @param aggregation  The druid aggregation, i.e.
     * {@link com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation}.
     * @param apiToFieldMapper  the mapping from api name to field name of the aggregation.
     *
     *  @return the supported sql aggregation type.
     */
    @Override
    public Optional<SqlAggregation> apply(Aggregation aggregation, ApiToFieldMapper apiToFieldMapper) {
        String aggregationType = aggregation.getType();
        return Optional.ofNullable(druidToSqlAggregation.get(aggregationType))
                .map(sqlAggregationType -> sqlAggregationType.getSqlAggregation(aggregation, apiToFieldMapper));
    }
}
