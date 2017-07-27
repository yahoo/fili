// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification

class TimeSeriesQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared
    DateTimeZone currentTZ

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")))
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    TimeSeriesQuery defaultQuery(Map vars) {
        vars.dataSource = vars.dataSource ?: new TableDataSource(TableTestUtils.buildTable(
                "table_name",
                DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                [] as Set,
                [:],
                Mock(DataSourceMetadataService)
        ))
        vars.granularity = vars.granularity ?: DAY
        vars.filter = vars.filter ?: null
        vars.having = vars.having ?: null
        vars.aggregations = vars.aggregations ?: new ArrayList<Aggregation>()
        vars.postAggregations = vars.postAggregations ?: new ArrayList<PostAggregation>()
        vars.intervals = vars.intervals ?: new ArrayList<Interval>()
        QueryContext initial = new QueryContext(Collections.<QueryContext.Param, Object> emptyMap(), null)
                .withValue(QueryContext.Param.QUERY_ID, "dummy100")
        QueryContext context = vars.context != null ?
                new QueryContext(initial, vars.context as Map).withValue(QueryContext.Param.QUERY_ID, "dummy100") :
                initial

        new TimeSeriesQuery(
                vars.dataSource,
                vars.granularity,
                vars.filter,
                vars.aggregations,
                vars.postAggregations,
                vars.intervals,
                context,
                false
        )
    }

    def stringQuery(Map vars) {
        vars.queryType = vars.queryType ?: "timeseries"
        vars.dataSource = vars.dataSource ?: '{"type":"table","name":"table_name"}'
        vars.granularity = vars.granularity ?: '{"type":"period","period":"P1D"}'
        vars.filter = vars.filter ? /"filter": $vars.filter,/ : ""
        vars.context = vars.context ?
                /{"queryId":"dummy100",$vars.context}/ :
                /{"queryId": "dummy100"}/
        vars.aggregations = vars.aggregations ?: "[]"
        vars.postAggregations = vars.postAggregations ?: "[]"
        vars.intervals = vars.intervals ?: "[]"


        """
        {
            "queryType":"$vars.queryType",
            "dataSource":$vars.dataSource,
            "granularity": $vars.granularity,
            $vars.filter
            "context":$vars.context,
            "aggregations":$vars.aggregations,
            "postAggregations":$vars.postAggregations,
            "intervals":$vars.intervals
        }
        """
    }

    def "check query serialization"() {
        TimeSeriesQuery dq1 = defaultQuery([:])
        String druidQuery1 = MAPPER.writeValueAsString(dq1)

        String queryString1 = stringQuery([:])

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
    }
}
