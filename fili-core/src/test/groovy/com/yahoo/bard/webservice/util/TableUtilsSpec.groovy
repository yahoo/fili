// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery
import com.yahoo.bard.webservice.web.DataApiRequest

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Table Utils methods encapsulate business logic around how columns are pulled from queries and requests
 */
class TableUtilsSpec extends  Specification {

    @Shared Dimension d1, d2, d3
    @Shared String d1Name, d2Name, d3Name
    @Shared Set<Dimension> ds1, ds12, ds13, ds123, dsNone
    @Shared String metric1, metric2, metric3
    AbstractDruidAggregationQuery<?> query = Mock(AbstractDruidAggregationQuery)
    DataApiRequest request = Mock(DataApiRequest)

    def setupSpec() {
        d1 = Mock(Dimension)
        d2 = Mock(Dimension)
        d3 = Mock(Dimension)

        d1Name = "d1"
        d2Name = "d2"
        d3Name = "d3"

        d1.getApiName() >> d1Name
        d2.getApiName() >> d2Name
        d3.getApiName() >> d3Name

        ds1 = [d1]
        ds12 = [d1, d2]
        ds13 = [d1, d3]
        ds123 = [d1, d2, d3]
        dsNone = []

        metric1 = "m1"
        metric2 = "m2"
        metric3 = "m3"
    }

    @Unroll
    def "With #requestDimensions, #filterDimensions, #metricFilterDimensions dimension names are: #expected  "() {
        setup:
        request.dimensions >> requestDimensions
        request.filterDimensions >> filterDimensions
        query.metricDimensions >> metricFilterDimensions
        query.getDependentFieldNames() >> new HashSet<String>()
        expected = expected as Set

        expect:
        TableUtils.getColumnNames(request, query) == expected

        where:
        requestDimensions | filterDimensions | metricFilterDimensions | expected
        dsNone            | dsNone           | dsNone                 | []
        ds1               | dsNone           | dsNone                 | [d1Name]
        dsNone            | ds1              | dsNone                 | [d1Name]
        dsNone            | dsNone           | ds1                    | [d1Name]
        ds1               | ds12             | dsNone                 | [d1Name, d2Name]
        ds1               | ds13             | dsNone                 | [d1Name, d3Name]
        ds1               | dsNone           | ds1                    | [d1Name]
        ds1               | ds12             | ds13                   | [d1Name, d2Name, d3Name]
    }

    def "Metric columns are returned "() {
        setup:
        request.dimensions >> ds1
        request.filterDimensions >> ds1
        query.metricDimensions >> ds1
        query.dependentFieldNames >> ([metric1, metric2, metric3] as Set)

        expect:
        TableUtils.getColumnNames(request, query) == [d1Name, metric1, metric2, metric3] as Set
    }

    def "metric name correctly is correctly represented as logicalName" () {
        setup:
        request.dimensions >> []
        request.filterDimensions >> []
        query.metricDimensions >> []
        query.dependentFieldNames >> ([metric1] as Set)

        expect:
        TableUtils.getColumnNames(request, query) == [metric1] as Set
    }
}
