// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.table.TableTestUtils.buildTable

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.AndFilter
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
import com.yahoo.bard.webservice.druid.model.filter.OrFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification

import java.util.stream.Collectors

class GroupByQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared
    DateTimeZone currentTZ
    ZonedTimeGrain day = DAY.buildZonedTimeGrain(DateTimeZone.UTC)

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")))
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    Dimension dimension1
    Dimension dimension2
    Dimension dimension3

    Aggregation aggregation1 = new LongSumAggregation("pageViewsSum", "pageViews")
    Aggregation aggregation2 = new LongSumAggregation("timeSpentSum", "timeSpent")
    PostAggregation postAggregation1 = new FieldAccessorPostAggregation(aggregation1)
    PostAggregation postAggregation2 = new FieldAccessorPostAggregation(aggregation2)
    PostAggregation postAggregation3 = new ArithmeticPostAggregation(
            "postAggDiv",
            ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE,
            [postAggregation1, postAggregation2]
    )

    def setup() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        dimension1 = new KeyValueStoreDimension(
                "apiLocale",
                "localeDescription",
                dimensionFields,
                MapStoreManager.getInstance("apiLocale"),
                ScanSearchProviderManager.getInstance("apiLocale")
        )
        dimension2 = new KeyValueStoreDimension(
                "apiPlatform",
                "platformDescription",
                dimensionFields,
                MapStoreManager.getInstance("apiPlatform"),
                ScanSearchProviderManager.getInstance("apiPlatform")
        )
        dimension3 = new KeyValueStoreDimension(
                "apiProduct",
                "productDescription",
                dimensionFields,
                MapStoreManager.getInstance("apiProduct"),
                ScanSearchProviderManager.getInstance("apiProduct")
        )
    }

    GroupByQuery defaultQuery(Map vars) {
        ConstrainedTable constrainedTable = buildTable(
                "table_name",
                day,
                [] as Set,
                ["apiLocale": "locale", "apiPlatform": "platform", "apiProduct": "product"],
                Mock(DataSourceMetadataService)
        )

        vars.dataSource = vars.dataSource ?: new TableDataSource(constrainedTable)
        vars.granularity = vars.granularity ?: DAY
        vars.dimensions = vars.dimensions ?: new ArrayList<Dimension>()
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

        new GroupByQuery(
                vars.dataSource,
                vars.granularity,
                vars.dimensions,
                vars.filter,
                vars.having,
                vars.aggregations,
                vars.postAggregations,
                vars.intervals,
                vars.orderBy,
                context,
                false
        )
    }

    def stringQuery(Map vars) {
        vars.queryType = vars.queryType ?: "groupBy"
        vars.dataSource = vars.dataSource ?: '{"type":"table","name":"table_name"}'
        vars.granularity = vars.granularity ?: '{"type":"period","period":"P1D"}'
        vars.dimensions = vars.dimensions ?: "[]"
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
            "granularity":$vars.granularity,
            "dimensions":$vars.dimensions,
            $vars.filter
            "aggregations":$vars.aggregations,
            "postAggregations":$vars.postAggregations,
            "intervals":$vars.intervals,
            "context":$vars.context
        }
        """
    }

    def "check dimensions serialization"() {
        //no dimensions
        List<Dimension> dimensions1 = []
        //single dimension
        List<Dimension> dimensions2 = [dimension1]
        //multiple dimension
        List<Dimension> dimensions3 = [dimension1, dimension2, dimension3]

        GroupByQuery dq1 = defaultQuery(dimensions: dimensions1)
        GroupByQuery dq2 = defaultQuery(dimensions: dimensions2)
        GroupByQuery dq3 = defaultQuery(dimensions: dimensions3)
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)
        String druidQuery3 = MAPPER.writeValueAsString(dq3)

        String queryString1 = stringQuery(default: true)
        String queryString2 = stringQuery(dimensions: """[{"dimension":"locale","outputName":"apiLocale","type":"default"}]""")
        String queryString3 = stringQuery(dimensions: """[{"dimension":"locale","outputName":"apiLocale","type":"default"},{"dimension":"platform","outputName":"apiPlatform","type":"default"},{"dimension":"product","outputName":"apiProduct","type":"default"}]""")

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
        GroovyTestUtils.compareJson(druidQuery3, queryString3)
    }

    def "check dataSource serialization"() {
        //non nested query
        DataSource ds1 = new TableDataSource(buildTable("table_name", day, [] as Set, [:], Mock(DataSourceMetadataService)))
        GroupByQuery dq1 = defaultQuery(dataSource: ds1)

        //nested query
        DataSource ds2 = new QueryDataSource(dq1)
        GroupByQuery dq2 = defaultQuery(dataSource: ds2)
        //druid queries
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)

        //expected result
        String queryString1 = stringQuery(default: true)

        String dataSrc = """
            {
                "type":"query",
                "query":$queryString1
            }
        """

        String queryString2 = stringQuery(dataSource: dataSrc)

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
    }

    def "check filter serialization"() {
        Dimension locale = Mock(Dimension)
        locale.getApiName() >> "apiLocale"

        Filter filter1 = null
        Filter filter2 = new SelectorFilter(locale, "US")
        Filter filter3 = new NotFilter(filter2)
        Filter filter4 = new OrFilter([filter2, filter3])
        Filter filter5 = new AndFilter([filter4, filter2])

        GroupByQuery dq1 = defaultQuery(filter: filter1)
        GroupByQuery dq2 = defaultQuery(filter: filter2)
        GroupByQuery dq3 = defaultQuery(filter: filter5)
        //druid queries
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)
        String druidQuery3 = MAPPER.writeValueAsString(dq3)

        String filtr1 = """
                {
                    "type":"selector",
                    "dimension":"locale",
                    "value":"US"
                }
        """

        String filtr2 = """
                {
                    "type":"and",
                    "fields":
                        [
                            {
                                "type":"or",
                                "fields":
                                    [
                                        {
                                            "type":"selector",
                                            "dimension":"locale",
                                            "value":"US"
                                        },
                                        {
                                            "type":"not",
                                            "field":
                                                {
                                                    "type":"selector",
                                                    "dimension":"locale",
                                                    "value":"US"
                                                }
                                        }
                                    ]
                            },
                            {
                                "type":"selector",
                                "dimension":"locale",
                                "value":"US"
                            }
                        ]
                }
        """

        //expected result
        String queryString1 = stringQuery(default: true)
        String queryString2 = stringQuery(filter: filtr1)
        String queryString3 = stringQuery(filter: filtr2)

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
        GroovyTestUtils.compareJson(druidQuery3, queryString3)
    }

    def "check aggregation serialization"() {
        List<Aggregation> aggregations1 = []
        List<Aggregation> aggregations2 = [aggregation1]
        List<Aggregation> aggregations3 = [aggregation1, aggregation2]

        GroupByQuery dq1 = defaultQuery(aggregations: aggregations1)
        GroupByQuery dq2 = defaultQuery(aggregations: aggregations2)
        GroupByQuery dq3 = defaultQuery(aggregations: aggregations3)
        //druid queries
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)
        String druidQuery3 = MAPPER.writeValueAsString(dq3)

        String agg1 = """
            [
                {
                    "type":"longSum",
                    "name":"pageViewsSum",
                    "fieldName":"pageViews"
                }
            ]
        """

        String agg2 = """
            [
                {
                    "type":"longSum",
                    "name":"pageViewsSum",
                    "fieldName":"pageViews"
                },
                {
                    "type":"longSum",
                    "name":"timeSpentSum",
                    "fieldName":"timeSpent"
                }
            ]
        """

        //expected result
        String queryString1 = stringQuery(default: true)
        String queryString2 = stringQuery(aggregations: agg1)
        String queryString3 = stringQuery(aggregations: agg2)

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
        GroovyTestUtils.compareJson(druidQuery3, queryString3)
    }

    def "check post aggregation serialization"() {
        List<PostAggregation> postAggregations1 = []
        List<PostAggregation> postAggregations2 = [postAggregation1]
        List<PostAggregation> postAggregations3 = [postAggregation1, postAggregation3]
        GroupByQuery dq1 = defaultQuery(postAggregations: postAggregations1)
        GroupByQuery dq2 = defaultQuery(postAggregations: postAggregations2)
        GroupByQuery dq3 = defaultQuery(postAggregations: postAggregations3)

        //druid queries
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)
        String druidQuery3 = MAPPER.writeValueAsString(dq3)

        String postAgg1 = """
                [
                    {
                        "fieldName":"pageViewsSum",
                        "type":"fieldAccess"
                    }
                ]
        """

        String postAgg2 = """
                [
                    {
                        "fieldName":"pageViewsSum",
                        "type":"fieldAccess"
                    },
                    {
                        "name":"postAggDiv",
                        "fields":
                            [
                                {
                                    "fieldName":"pageViewsSum",
                                    "type":"fieldAccess"
                                },
                                {
                                    "fieldName":"timeSpentSum",
                                    "type":"fieldAccess"
                                }
                            ],
                        "type":"arithmetic",
                        "fn":"/"
                    }
                ]
        """

        //expected result
        String queryString1 = stringQuery(default: true)
        String queryString2 = stringQuery(postAggregations: postAgg1)
        String queryString3 = stringQuery(postAggregations: postAgg2)

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
        GroovyTestUtils.compareJson(druidQuery3, queryString3)
    }

    def "check interval serialization"() {
        List<Interval> intervals1 = []
        List<Interval> intervals2 = [new Interval("2011-07-04T00:00:00.000Z/2011-07-06T00:00:00.000Z")]
        List<Interval> intervals3 = [
                new Interval("2011-07-04T00:00:00.000Z/2011-07-06T00:00:00.000Z"),
                new Interval("2011-07-08T00:00:00.000Z/2011-07-10T00:00:00.000Z")
        ]

        GroupByQuery dq1 = defaultQuery(intervals: intervals1)
        GroupByQuery dq2 = defaultQuery(intervals: intervals2)
        GroupByQuery dq3 = defaultQuery(intervals: intervals3)
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)
        String druidQuery3 = MAPPER.writeValueAsString(dq3)

        String queryString1 = stringQuery(default: true)
        String queryString2 = stringQuery(
                intervals: """["2011-07-03T19:00:00.000-05:00/2011-07-05T19:00:00.000-05:00"]"""
        )
        String queryString3 = stringQuery(
                intervals: """[
                                    "2011-07-03T19:00:00.000-05:00/2011-07-05T19:00:00.000-05:00",
                                    "2011-07-07T19:00:00.000-05:00/2011-07-09T19:00:00.000-05:00"
                              ]"""
        )

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
        GroovyTestUtils.compareJson(druidQuery3, queryString3)
    }


    def "check context serialization"() {
        def context1 = [(QueryContext.Param.TIMEOUT): 5]
        def context2 = [(QueryContext.Param.POPULATE_CACHE): true, (QueryContext.Param.BY_SEGMENT): false]
        def context3 = [(QueryContext.Param.TIMEOUT): 5, (QueryContext.Param.POPULATE_CACHE): true]

        GroupByQuery dq0 = defaultQuery(dimensions: [])
        GroupByQuery dq1 = defaultQuery(context: context1)
        GroupByQuery dq2 = defaultQuery(context: context2)
        GroupByQuery dq3 = defaultQuery(context: context3)
        QueryContext context4 = dq2.getContext().withTimeout(5)
        GroupByQuery dq4 = dq1.withContext(context4)
        GroupByQuery dq5 = dq3.withContext(dq3.getContext().withPopulateCache(null))


        String druidQuery0 = MAPPER.writeValueAsString(dq0)
        String druidQuery1 = MAPPER.writeValueAsString(dq1)
        String druidQuery2 = MAPPER.writeValueAsString(dq2)
        String druidQuery3 = MAPPER.writeValueAsString(dq3)
        String druidQuery4 = MAPPER.writeValueAsString(dq4)
        String druidQuery5 = MAPPER.writeValueAsString(dq5)

        def contextString1 = '"timeout": 5'


        def contextString2 = '"populateCache": true, "bySegment": false'

        def contextString3 = '"timeout": 5, "populateCache": true'

        def contextString4 = '"timeout": 5, "populateCache": true, "bySegment": false'


        String queryString0 = stringQuery(default: true)
        String queryString1 = stringQuery(context: contextString1)
        String queryString2 = stringQuery(context: contextString2)
        String queryString3 = stringQuery(context: contextString3)
        String queryString4 = stringQuery(context: contextString4)

        expect:
        GroovyTestUtils.compareJson(druidQuery0, queryString0)
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
        GroovyTestUtils.compareJson(druidQuery2, queryString2)
        GroovyTestUtils.compareJson(druidQuery3, queryString3)
        GroovyTestUtils.compareJson(druidQuery4, queryString4)
        GroovyTestUtils.compareJson(druidQuery5, queryString1)
    }

    def "Check innermost query injection"() {
        setup:
        TableDataSource inner1 = new TableDataSource(
                buildTable("inner1", day, [] as Set, [:], Mock(DataSourceMetadataService))
        )
        TableDataSource inner2 = new TableDataSource(
                buildTable("inner2", day, [] as Set, [:], Mock(DataSourceMetadataService))
        )
        GroupByQuery dq1 = defaultQuery(dataSource: inner1)
        DataSource outer1 = new QueryDataSource(dq1)
        GroupByQuery dq2 = defaultQuery(dataSource: outer1)

        when:
        GroupByQuery dq3 = dq2.withInnermostDataSource(inner2)

        then:
        dq3.getDataSource().getNames() as List == ["inner2"]
        dq2.getDataSource().getNames() as List == ["inner1"]
        dq1.getDataSource().getNames() as List == ["inner1"]
        dq3.getDataSource().getQuery().getDataSource() == inner2
    }

    def "Check all intervals injection"() {
        given: "Starting and ending intervals"
        List<Interval> startingIntervals = [Interval.parse("2014/2015")]
        List<Interval> endingIntervals = [Interval.parse("2016/2017")]

        and: "A nested query"
        TableDataSource table = new TableDataSource(buildTable("inner1", day, [] as Set, [:], Mock(DataSourceMetadataService)))
        GroupByQuery inner = defaultQuery(dataSource: table, intervals: startingIntervals)
        GroupByQuery middle = defaultQuery(dataSource: new QueryDataSource<>(inner), intervals: startingIntervals)
        GroupByQuery outer = defaultQuery(dataSource: new QueryDataSource<>(middle), intervals: startingIntervals)

        when: "We set all intervals"
        GroupByQuery converted = outer.withAllIntervals(endingIntervals)

        then: "The intervals at each level are the new intervals"
        converted.intervals == endingIntervals
        converted.innerQuery.intervals == endingIntervals
        converted.innerQuery.innerQuery.intervals == endingIntervals
    }

    def "Check column stream is generated for query"() {
        setup:
        List<Dimension> dimensions = [dimension1, dimension2]
        List<Aggregation> aggregations = [aggregation1, aggregation2]
        List<PostAggregation> postAggregations = [postAggregation3]

        GroupByQuery query = defaultQuery(dimensions: dimensions, aggregations: aggregations, postAggregations: postAggregations )
        List<Column> columns = [dimension1, dimension2].collect {new DimensionColumn(it)}
        columns.addAll([aggregation1, aggregation2, postAggregation3].collect {new MetricColumn(it.name)})

        expect:
        query.buildSchemaColumns().collect(Collectors.toList()) == columns
    }
}
