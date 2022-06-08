// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Optional;

/**
 * A mapper that removes results which overlap a missing interval set.
 */
public class TimeDimensionResultSetMapper extends ResultSetMapper {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String DATE_TIME_STRING =
            SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName("time_dimension"));

    public final DateTimeZone dateTimeZone;

    /**
     * Build a mapper to use time dimension as the timestamp in the result set.
     *
     * @param dateTimeZone  The timezone to use to build the timestamp
     */
    public TimeDimensionResultSetMapper(DateTimeZone dateTimeZone) {
        this.dateTimeZone = dateTimeZone;
    }

    /**
     * Turn a time dimension into a timestamp for the record.
     * This can turn the timestamp null.
     *
     * @param result   The result row being transformed
     * @param schema   The schema for that result
     * @return Null if the bucket this result falls in is missing but not volatile
     */
    @Override
    public Result map(Result result, ResultSetSchema schema) {
        Optional<DimensionColumn> timeColumn = schema.getColumn(DATE_TIME_STRING, DimensionColumn.class);

        DateTime dateTime = result.getTimeStamp();

        if (timeColumn.isPresent()) {
            DimensionRow dimensionRow = result.getDimensionRow(timeColumn.get());
            dateTime = (dimensionRow == null ||
                        dimensionRow.getKeyValue() == null ||
                        dimensionRow.getKeyValue().isEmpty()) ?
                    null :
                    new DateTime(Long.parseLong(dimensionRow.getKeyValue()), dateTimeZone);
        }
        return new Result(result.getDimensionRows(), result.getMetricValues(), dateTime);
    }

    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        return schema;
    }
}
