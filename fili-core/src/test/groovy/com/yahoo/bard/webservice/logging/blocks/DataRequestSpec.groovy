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
    Dimension dim1 = Mock(Dimension)
    Dimension dim2 = Mock(Dimension)
    LogicalMetric metric1 = Mock(LogicalMetric)
    LogicalTable table1 = new LogicalTable(
            "table",
            DefaultTimeGrain.DAY,
            new TableGroup([] as LinkedHashSet, [] as Set, [] as Set), Stub(MetricDictionary)
    )
    List<Interval> intervals = []
    Set set1 = [] as Set
    Set set2 = [] as Set
    Collection<Set<ApiFilter>> filterSuperSet = [set1, set2] as Set
    Set<LogicalMetric> metricsSet = [metric1] as Set
    Set<Dimension> groupByDimensionsSet
    Set<String> dataSourceNames = ['val1', 'val2'] as Set
    String format = "json"


    def "DataRequest does not log filtered dimensions"() {
        setup:
        dim1.getApiName() >> "dim1"
        dim2.getApiName() >> "dim2"
        groupByDimensionsSet = [dim2] as Set
        query.getDimensions() >> groupByDimensionsSet
        query.getMetricDimensions() >> ([dim1] as Set)
        query.getDependentFieldNames() >> ([] as Set)

        when:
        dataRequest = new DataRequest(table1, intervals, filterSuperSet, metricsSet, groupByDimensionsSet, dataSourceNames,false, format)

        then:
        !dataRequest.combinedDimensions.contains("dim1")
        dataRequest.combinedDimensions.contains("dim2")
    }
}
