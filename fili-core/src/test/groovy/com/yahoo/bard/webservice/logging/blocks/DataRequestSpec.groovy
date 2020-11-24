// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.web.ApiFilter

import org.joda.time.Interval

import spock.lang.Specification

class DataRequestSpec extends Specification{
    DataRequest dataRequest
    DruidAggregationQuery query
    Dimension dim1
    Dimension dim2
    LogicalMetric metric1
    LogicalTable table1
    List<Interval> intervals
    Set set1
    Set set2
    Collection<Set<ApiFilter>> filterSuperSet
    Set<LogicalMetric> metricsSet
    Set<Dimension> groupByDimensionsSet
    Set<Dimension> filteredDimensionsSet
    Set<String> dataSourceNames
    String format

    def setup() {
        dim1 = Mock(Dimension)
        dim2 = Mock(Dimension)
        dim1.getApiName() >> "dim1"
        dim2.getApiName() >> "dim2"
        metric1 = Mock(LogicalMetric)
        table1 = new LogicalTable(
                "table",
                DefaultTimeGrain.DAY,
                new TableGroup([] as LinkedHashSet, [] as Set, [] as Set), Stub(MetricDictionary)
        )
        intervals = []
        set1 = [] as Set
        set2 = [] as Set
        filterSuperSet = [set1, set2] as Set
        metricsSet = [metric1] as Set
        groupByDimensionsSet = [dim2] as Set
        filteredDimensionsSet = [dim1] as Set
        dataSourceNames = ['val1', 'val2'] as Set
        format = "json"
    }

    def "DataRequest does logs filtered dimensions"() {
        when:
        dataRequest = new DataRequest(table1, intervals, filterSuperSet, metricsSet, groupByDimensionsSet, filteredDimensionsSet, dataSourceNames,false, format)

        then:
        dataRequest.combinedDimensions.contains("dim1")
        dataRequest.combinedDimensions.contains("dim2")
    }
}
