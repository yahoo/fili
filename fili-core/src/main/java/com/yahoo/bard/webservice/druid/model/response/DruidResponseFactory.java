// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.QueryType;

import org.joda.time.DateTime;

/**
 * Allows for the creation of {@link DruidResponse} and {@link DruidResultRow}
 * using {@link DefaultQueryType}.
 */
public class DruidResponseFactory {
    /**
     * Private constructor - all methods static.
     */
    private DruidResponseFactory() {

    }

    /**
     * Builds a DruidResponse given the type of query.
     *
     * @param queryType  The type of query to be sent to Druid.
     *
     * @return the appropriate DruidResponse.
     */
    public static DruidResponse getResponse(QueryType queryType) {
        DefaultQueryType defaultQueryType = (DefaultQueryType) queryType;
        switch (defaultQueryType) {
            case GROUP_BY:
            case TIMESERIES:
                return new DruidResponse();
            case TOP_N:
                return new TopNDruidResponse();
            case TIME_BOUNDARY:
            case SEGMENT_METADATA:
            case SEARCH:
            case LOOKBACK:
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }

    /**
     * Builds a DruidResultRow given the type of query and the timestamp to create it at.
     *
     * @param queryType  The type of query to be sent to Druid.
     * @param timeStamp  The timestamp to create the result for.
     *
     * @return the appropriate DruidResultRow.
     */
    public static DruidResultRow getResultRow(QueryType queryType, DateTime timeStamp) {
        DefaultQueryType defaultQueryType = (DefaultQueryType) queryType;
        switch (defaultQueryType) {
            case GROUP_BY:
                return new GroupByResultRow(timeStamp, GroupByResultRow.Version.V1);
            case TOP_N:
                return new TopNResultRow(timeStamp);
            case TIMESERIES:
                return new TimeseriesResultRow(timeStamp);
            case TIME_BOUNDARY:
            case SEGMENT_METADATA:
            case SEARCH:
            case LOOKBACK:
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }
}
