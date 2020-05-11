// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy

import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.DIMENSION
import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.METRIC
import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.TIME
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.ASC
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.DESC
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_IN_QUERY_FORMAT
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_SORTABLE_FORMAT

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder
import com.yahoo.bard.webservice.web.apirequest.RequestParameters
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Specification
import spock.lang.Unroll

class AntlrOrderByGeneratorSpec extends Specification {

    TemplateDruidQuery templateDruidQuery = Mock(TemplateDruidQuery)
    Aggregation xyzAggregation = new LongSumAggregation("xyz", "foo")
    LogicalMetric logicalMetric = new LogicalMetricImpl(templateDruidQuery, null, "xyz")
    LogicalMetric noTDQLogicalMetric = new LogicalMetricImpl(null, null, "noTDQ")

    Dimension sampleDimension = Mock(Dimension)
    LinkedHashSet<Dimension> selectedDimensions = [sampleDimension] as LinkedHashSet

    def setup() {
        templateDruidQuery = new TemplateDruidQuery([xyzAggregation], [])
        // Create the test web container to test the resources
        DimensionField dimensionField = Mock(DimensionField)
        sampleDimension.getKey() >> dimensionField
        dimensionField.getName() >> "fieldName"
        sampleDimension.getApiName() >> "dim1"
    }

    def cleanup() {
    }

    AntlrOrderByGenerator generator = new AntlrOrderByGenerator()

    @Unroll
    def "Bind parses the metric string #sortString correctly"() {
        setup:
        List<String> metricFields = ["xyz", "abc", "dateTimexyz", "dinga",
                                     "xyz(param1=foo,param2=bar)", "xyz(param1=foo2,param2=bar2)"]
        Set<LogicalMetric> logicalMetrics = metricFields.collect() {
            Aggregation a = new LongSumAggregation(it, it + "_field")
            TemplateDruidQuery tdq = new TemplateDruidQuery([a], [])
            new LogicalMetricImpl(tdq, null, (String) it)
        } as Set<LogicalMetric>


        DataApiRequestBuilder dataApiRequestBuilder = Mock(DataApiRequestBuilder)
        RequestParameters requestParameters = Mock(RequestParameters)
        BardConfigResources configResources = Mock(BardConfigResources)
        requestParameters.getSorts() >> Optional.of(sortString)

        dataApiRequestBuilder.getLogicalMetricsIfInitialized() >> logicalMetrics
        dataApiRequestBuilder.getDimensionsIfInitialized() >> selectedDimensions

        List<OrderByColumn> orderByColumnList = generator.bind(dataApiRequestBuilder, requestParameters, configResources)

        expect:
        expected.eachWithIndex{ def entry, int i ->
            OrderByColumn column = orderByColumnList.get(i)
            assert entry.getAt(0) == column.getDimension()
            assert entry.getAt(1) == column.getDirection()
            assert entry.getAt(2) == column.getType()
        } || ( (List) expected).isEmpty()

        where:
        sortString                       | expected
        // empty set
        ""                               | []
        //date time every direction
        "dateTime"                       | [["dateTime", DESC, TIME]]
        "dateTime|ASC"                   | [["dateTime", ASC, TIME]]
        "dateTime|DESC"                  | [["dateTime", DESC, TIME]]

        // simple metric
        "xyz"                            | [["xyz", DESC, METRIC]]
        "xyz|ascending"                  | [["xyz", ASC, METRIC]]
        "xyz|descen"                     | [["xyz", DESC, METRIC]]
        // simple dimension maps to keyfield in binding phase
        "dim1"                           | [["fieldName", DESC, DIMENSION]]

        // dateTime not being used as a string match
        "dateTimexyz|DESC"               | [["dateTimexyz", DESC, METRIC]]

        // Three metrics, internal date time not resulting in an error here
        "xyz,dateTime,abc"               | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", DESC, METRIC]]
        // interior directions parse
        "xyz,dateTime|DESC,abc"          | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", DESC, METRIC]]
        "xyz|desc,dateTime|DESC,abc|ASC" | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", ASC, METRIC]]
        "xyz|DESC,dateTime,abc|DESC"     | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", DESC, METRIC]]
        "xyz|DESC,dateTime"              | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME]]

        // protocol metric
        "xyz(param1=foo,param2=bar)"            | [["xyz(param1=foo,param2=bar)", DESC, METRIC]]
        "xyz(param1=foo,param2=bar)|descending" | [["xyz(param1=foo,param2=bar)", DESC, METRIC]]
        "xyz(param1=foo,param2=bar)|ASC,xyz(param1=foo2,param2=bar2)|DESC"        | [["xyz(param1=foo,param2=bar)", ASC, METRIC],
                                                                                    ["xyz(param1=foo2,param2=bar2)", DESC, METRIC]]
        "xyz(param1=foo,param2=bar)|ASCENDING"  | [["xyz(param1=foo,param2=bar)", ASC, METRIC]]
    }

    @Unroll
    def "Validation fails for #rule"() {
        LinkedHashSet metrics = [logicalMetric, noTDQLogicalMetric]

        List<OrderByColumn> orderByColumnList = generator.generateBoundOrderByColumns(sortString, metrics, selectedDimensions)

        DataApiRequestBuilder dataApiRequestBuilder = Mock(DataApiRequestBuilder)

        RequestParameters requestParameters = Mock(RequestParameters)
        BardConfigResources configResources = Mock(BardConfigResources)
        requestParameters.getSorts() >> Optional.of(sortString)
        dataApiRequestBuilder.getLogicalMetricsIfInitialized() >> metrics

        when:
        generator.validate(orderByColumnList, dataApiRequestBuilder, requestParameters, configResources)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == errorMessage

        where:
        sortString               | errorMessage                                                        | rule
        "xyz|desc,dateTime|desc" | DATE_TIME_SORT_VALUE_INVALID.format()                               | "Date time follows other fields"
        "xyz|desc,unknown"       | SORT_METRICS_NOT_IN_QUERY_FORMAT.format(["unknown"], "[xyz,noTDQ]") | "No selected metric"
        "xyz|desc,noTDQ"         | SORT_METRICS_NOT_SORTABLE_FORMAT.format(["noTDQ"])                  | "No fact column to sort"
    }

    @Unroll
    def "Parsing sort direction parses #columns into #direction"() {
        expect:
        generator.parseSortDirection(columns) == direction

        where:
        columns               | direction
        ["foo"]               | DESC
        ["foo", "DESC"]       | DESC
        ["foo", "ASC"]        | ASC
        ["foo", "DESCENDING"] | DESC
        ["foo", "ASCENDING"]  | ASC
        ["foo", "desc"]       | DESC
        ["foo", "asc"]        | ASC
        ["foo", "descending"] | DESC
        ["foo", "ascending"]  | ASC
    }

    @Unroll
    def "Parsing #sortString results in the expected unbound columns"() {
        setup:

        expect:
        List<OrderByColumn> orderByColumnList = generator.parseOrderByColumns(sortString)
        orderByColumnList.collectEntries() { [(it.dimension) :it.direction] } == expected

        where:
        sortString                       | expected
        "dateTime"                       | ["dateTime": DESC] as Map
        "dateTime|ASC"                   | ["dateTime": ASC] as Map
        "dateTimexyz|DESC"               | ["dateTimexyz": DESC] as Map
        "xyz,dateTime,abc"               | ["xyz": DESC, "dateTime": DESC, "abc": DESC] as Map
        "xyz,dateTime|DESC,abc"          | ["xyz": DESC, "dateTime": DESC, "abc": DESC] as Map
        "xyz|DESC,dateTime|DESC,abc|ASC" | ["xyz": DESC, "dateTime": DESC, "abc": ASC] as Map
        "xyz|DESC,dateTime,abc|DESC"     | ["xyz": DESC, "dateTime": DESC, "abc": DESC] as Map
        "xyz|DESC,dateTime"              | ["xyz": DESC, "dateTime": DESC] as Map
        ""                               | [:]
        "unconfigured"                   | ["unconfigured": DESC] as Map
        "dim"                            | ["dim": DESC] as Map
    }

    @Unroll
    def "Generate and bind #sortString results in correct logical column, direction and type from "() {
        setup:
        List<String> metricFields = ["xyz", "abc", "dateTimexyz", "dinga"]
        LinkedHashSet<LogicalMetric> logicalMetrics = metricFields.collect() {
            Aggregation a = new LongSumAggregation(it, it + "_field")
            TemplateDruidQuery tdq = new TemplateDruidQuery([a], [])
            new LogicalMetricImpl(tdq, null, (String) it)
        } as Set<LogicalMetric>
        List<OrderByColumn> orderByColumnList = generator.generateBoundOrderByColumns(sortString, logicalMetrics, selectedDimensions)

        expect:
        expected.eachWithIndex{ def entry, int i ->
            OrderByColumn column = orderByColumnList.get(i)
            assert entry.getAt(0) == column.getDimension()
            assert entry.getAt(1) == column.getDirection()
            assert entry.getAt(2) == column.getType()
        } || ( (List) expected).isEmpty()

        where:
        sortString                       | expected
        // empty set
        ""                               | []
        //date time every direction
        "dateTime"                       | [["dateTime", DESC, TIME]]
        "dateTime|ASC"                   | [["dateTime", ASC, TIME]]
        "dateTime|DESC"                  | [["dateTime", DESC, TIME]]

        // simple metric
        "xyz"                            | [["xyz", DESC, METRIC]]
        "xyz|ascending"                  | [["xyz", ASC, METRIC]]
        "xyz|descen"                     | [["xyz", DESC, METRIC]]
        // simple dimension maps to keyfield in binding phase
        "dim1"                           | [["fieldName", DESC, DIMENSION]]

        // dateTime not being used as a string match
        "dateTimexyz|DESC"               | [["dateTimexyz", DESC, METRIC]]

        // Three metrics, internal date time not resulting in an error here
        "xyz,dateTime,abc"               | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", DESC, METRIC]]
        // interior directions parse
        "xyz,dateTime|DESC,abc"          | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", DESC, METRIC]]
        "xyz|desc,dateTime|DESC,abc|ASC" | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", ASC, METRIC]]
        "xyz|DESC,dateTime,abc|DESC"     | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME], ["abc", DESC, METRIC]]
        "xyz|DESC,dateTime"              | [["xyz", DESC, METRIC], ["dateTime", DESC, TIME]]
    }
}
