// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TopNQuery
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.TableTestUtils

import org.joda.time.DateTimeZone

class RequestUtils {

    static GroupByQuery buildGroupByQuery(
            String dataSourceName = "dataSource",
            TimeGrain granularity = DAY,
            List aggregations = [],
            List postAggregations = []
    ) {
        DataSource dataSource = new TableDataSource(
                TableTestUtils.buildTable(
                        dataSourceName,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        [] as Set,
                        [:],
                        new TestDataSourceMetadataService()
                )
        )
        List dimensions = []
        List intervals = []
        new GroupByQuery(
                dataSource,
                granularity,
                dimensions,
                (Filter) null,
                (Having) null,
                aggregations,
                postAggregations,
                intervals,
                (LimitSpec) null,
        )
    }

    static TopNQuery buildTopNQuery(
            String dataSourceName = "dataSource",
            TimeGrain granularity = DAY,
            long threshold = 5,
            List aggregations = [],
            List postAggregations = []
    ) {
        DataSource dataSource = new TableDataSource(
                TableTestUtils.buildTable(
                        dataSourceName,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        [] as Set,
                        [:],
                        new TestDataSourceMetadataService()
                )
        )
        List intervals = []
        new TopNQuery(
                dataSource,
                granularity,
                (Dimension) null,
                (Filter) null,
                aggregations,
                postAggregations,
                intervals,
                threshold,
                (TopNMetric) null
        )
    }

    static TimeSeriesQuery buildTimeSeriesQuery(
            String dataSourceName = "dataSource",
            TimeGrain granularity = DAY,
            List aggregations = [],
            List postAggregations = []
    ) {
        DataSource dataSource = new TableDataSource(
                TableTestUtils.buildTable(
                        dataSourceName,
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        [] as Set,
                        [:],
                        new TestDataSourceMetadataService()
                )
        )
        List intervals = []
        new TimeSeriesQuery(
                dataSource,
                granularity,
                (Filter) null,
                aggregations,
                postAggregations,
                intervals
        )
    }

    static String defaultQueryJson(String dataSourceName = "dataSource", TimeGrain timeGrain = DAY) {
        """{
            "queryType": "groupBy",
            "context": {},
            "dataSource": {
              "name": "$dataSourceName",
              "type": "table"
            },
            "dimensions": [],
            "aggregations": [],
            "postAggregations": [],
            "intervals": [],
            "granularity": {
              "type": "period",
              "period": "$timeGrain.periodString"
            }
        }"""
    }
}
