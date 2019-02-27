// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.config.BardFeatureFlag.TOP_N
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder
import com.yahoo.bard.webservice.druid.model.datasource.DefaultDataSourceType
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TopNQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.ApiHaving
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders
import com.yahoo.bard.webservice.web.filters.ApiFilters

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

public class DruidQueryBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources
    @Shared DefaultPhysicalTableResolver resolver
    @Shared DruidQueryBuilder builder

    @Shared Map filterSpecs
    @Shared Map<String, ApiFilter> apiFiltersByName

    @Shared boolean topNStatus
    @Shared ApiHaving having

    LimitSpec limitSpec
    TopNMetric topNMetric
    DataApiRequest apiRequest

    TemplateDruidQuery tdq = Mock(TemplateDruidQuery)
    LogicalMetric lm1
    static LogicalMetricInfo lmi1 = new LogicalMetricInfo("m1")
    static LogicalMetricInfo lmi2 = new LogicalMetricInfo("m2")

    LogicalMetricInfo m1LogicalMetric = new LogicalMetricInfo("lm1")

    static final DruidFilterBuilder DRUID_FILTER_BUILDER = new DruidOrFilterBuilder()

    List<Interval> intervals
    static FilterBinders filterBinders = FilterBinders.INSTANCE


    def staticInitialize() {
        resources = new QueryBuildingTestingResources()
        resolver = new DefaultPhysicalTableResolver(new PartialDataHandler(), new DefaultingVolatileIntervalsService())

        builder = new DruidQueryBuilder(
                resources.logicalDictionary,
                resolver,
                resources.druidFilterBuilder,
                resources.druidHavingBuilder
        )

        filterSpecs = [
                abie1234 : "ageBracket|id-eq[1,2,3,4]",
                abde1129 : "ageBracket|desc-eq[11-14,14-29]",
                abne56   : "ageBracket|id-notin[5,6]",
                abdne1429: "ageBracket|desc-notin[14-29]"
        ]

        apiFiltersByName = (filterSpecs.collectEntries {
            [(it.key): filterBinders.generateApiFilter(it.value as String, resources.dimensionDictionary)]
        } ) as Map<String, ApiFilter>

        LogicalMetric metric = new LogicalMetric(tdq, null, (LogicalMetricInfo) lmi1)
        LinkedHashSet<OrderByColumn> orderByColumns = [new OrderByColumn(metric, SortDirection.DESC)]
        limitSpec = new LimitSpec(orderByColumns)
        topNMetric = new TopNMetric("m1", SortDirection.DESC)
    }

    def setupSpec() {
        staticInitialize()
        topNStatus = TOP_N.isOn();
        TOP_N.setOn(true)
        having = new ApiHaving("$resources.m1.name-eq[1,2,3]" as String, resources.metricDictionary)
    }

    def cleanupSpec() {
        TOP_N.setOn(topNStatus)
    }

    def setup() {
        intervals = [new Interval(new DateTime("2015"), Hours.ONE)]
    }

    def initDefault(DataApiRequest apiRequest) {
        lm1 = new LogicalMetric(resources.simpleTemplateQuery, new NoOpResultSetMapper(), m1LogicalMetric)

        apiRequest.getTable() >> resources.lt12
        apiRequest.getGranularity() >> HOUR.buildZonedTimeGrain(UTC)
        apiRequest.getTimeZone() >> UTC
        apiRequest.getDimensions() >> ([resources.d1] as Set)
        ApiFilters apiFilters = new ApiFilters(
                apiFiltersByName.collectEntries {[(resources.d3): [it.value] as Set]} as Map<Dimension, Set<ApiFilter>>
        )
        apiRequest.getApiFilters() >> apiFilters
        apiRequest.getLogicalMetrics() >> ([lm1] as Set)
        apiRequest.getIntervals() >> intervals
        apiRequest.getTopN() >> OptionalInt.empty()
        apiRequest.getSorts() >> ([] as Set)
        apiRequest.getCount() >> OptionalInt.empty()
    }

    def "Test recursive buildQueryMethods"() {
        setup:
        Set<ApiFilters> apiSet = (["abie1234", "abde1129"].collect() { apiFiltersByName.get(it) }) as Set

        ConstrainedTable table = TableTestUtils.buildTable(
                "tab1",
                DAY.buildZonedTimeGrain(UTC),
                [] as Set,
                [:],
                Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
        )
        Filter druidFilter = DRUID_FILTER_BUILDER.buildFilters([(resources.d3): apiSet])
        ZonedTimeGrain granularity = WEEK.buildZonedTimeGrain(UTC)
        Set dimension = [resources.d1] as Set
        topNMetric = new TopNMetric("m1", SortDirection.DESC)
        int topN = 5

        when:
        GroupByQuery dq = builder.buildGroupByQuery(
                resources.simpleTemplateQuery,
                table,
                granularity,
                UTC,
                [] as Set,
                druidFilter,
                (Having) null,
                [],
                limitSpec
        )

        then:
        dq?.filter == druidFilter
        dq.dataSource.type == DefaultDataSourceType.TABLE
        dq.dataSource.name == table.name
        granularity.withZone(UTC)

        when:
        GroupByQuery dq1 = builder.buildGroupByQuery(
                resources.complexTemplateQuery,
                table,
                granularity,
                UTC,
                [] as Set,
                druidFilter,
                (Having) null,
                [] as List,
                limitSpec
        )

        then:
        dq1?.filter == null
        dq1.dataSource.type == DefaultDataSourceType.QUERY
        dq1.granularity == granularity.withZone(UTC)

        GroupByQuery dq2 = dq1.dataSource.getQuery().get()
        dq2.filter == druidFilter
        dq2.dataSource.type == DefaultDataSourceType.TABLE
        dq2.dataSource.name == table.name
        dq2.granularity == granularity.withZone(UTC)

        when:
        TopNQuery topNQuery = builder.buildTopNQuery(
                resources.simpleTemplateQuery,
                table,
                granularity,
                UTC,
                dimension,
                druidFilter,
                [],
                topNMetric,
                topN
        )

        then:
        topNQuery != null
        topNQuery.filter == druidFilter
        topNQuery.dataSource.type == DefaultDataSourceType.TABLE
        topNQuery.dataSource.name == table.name
        topNQuery.granularity == granularity.withZone(UTC)

        when:
        TimeSeriesQuery timeseriesQuery = builder.buildTimeSeriesQuery(
                resources.simpleTemplateQuery,
                table,
                granularity,
                UTC,
                druidFilter,
                []
        )

        then:
        timeseriesQuery != null
        timeseriesQuery.filter == druidFilter
        timeseriesQuery.dataSource.type == DefaultDataSourceType.TABLE
        timeseriesQuery.dataSource.name == table.name
        timeseriesQuery.granularity == granularity.withZone(UTC)
    }

    def "Test recursive buildQueryMethods with Grain"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        initDefault(apiRequest)

        when:
        Set apiSet = (["abie1234", "abde1129"].collect() { apiFiltersByName.get(it) }) as Set
        ConstrainedTable table = TableTestUtils.buildTable(
                "tab1",
                DAY.buildZonedTimeGrain(UTC),
                [] as Set,
                [:],
                Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
        )
        Filter filter = DRUID_FILTER_BUILDER.buildFilters([(resources.d3): apiSet])
        ZonedTimeGrain granularity = YEAR.buildZonedTimeGrain(UTC)
        TemplateDruidQuery simpleQuery = resources.simpleTemplateWithGrainQuery
        GroupByQuery dq = builder.buildGroupByQuery(
                simpleQuery,
                table,
                granularity,
                UTC,
                [] as Set,
                filter,
                (Having) null,
                [],
                limitSpec
        )

        then:
        dq?.filter == filter
        dq.dataSource.type == DefaultDataSourceType.TABLE
        dq.dataSource.name == table.name
        dq.granularity == simpleQuery.getTimeGrain().buildZonedTimeGrain(UTC)

        when:
        TemplateDruidQuery outerQuery = resources.complexTemplateWithInnerGrainQuery
        GroupByQuery dq1 = builder.buildGroupByQuery(
                outerQuery,
                table,
                granularity,
                UTC,
                [] as Set,
                filter, (Having) null,
                [],
                limitSpec
        )
        GroupByQuery dq2 = dq1.dataSource.getQuery().get()

        then:
        dq1?.filter == null
        dq1.granularity == granularity.withZone(UTC)
        dq1.dataSource.type == DefaultDataSourceType.QUERY
        dq2.filter == filter
        dq2.dataSource.type == DefaultDataSourceType.TABLE
        dq2.dataSource.name == table.name
        dq2.granularity == simpleQuery.timeGrain.buildZonedTimeGrain(UTC)

        when:
        outerQuery = resources.complexTemplateWithDoubleGrainQuery
        dq1 = builder.buildGroupByQuery(
                resources.complexTemplateWithDoubleGrainQuery,
                table,
                granularity,
                UTC,
                [] as Set,
                filter,
                (Having) null,
                [],
                limitSpec
        )
        dq2 = dq1.dataSource.getQuery().get()


        then:
        dq1?.filter == null
        dq1.granularity == outerQuery.timeGrain.buildZonedTimeGrain(UTC)
        dq1.dataSource.type == DefaultDataSourceType.QUERY
        dq2.filter == filter
        dq2.dataSource.type == DefaultDataSourceType.TABLE
        dq2.dataSource.name == table.name
        dq2.granularity == simpleQuery.timeGrain.buildZonedTimeGrain(UTC)
    }

    def "Test top level buildQuery with group by druid query"() {
        setup:
        apiRequest = Mock(DataApiRequest)

        initDefault(apiRequest)

        when:
        def DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.getQueryType() == DefaultQueryType.GROUP_BY
    }

    @Unroll
    def "A #topNDruid query is built when there #isIsNot a having clause"() {
        setup:
        apiRequest = Mock(DataApiRequest)

        apiRequest.getTopN() >> OptionalInt.of(5)
        apiRequest.getSorts() >> ([new OrderByColumn(
                new LogicalMetric(null, null, lmi1),
                SortDirection.DESC
        )] as Set)
        apiRequest.havings >> havingMap

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.getQueryType() == queryType

        where:
        queryType                 | havingMap                         | topNDruid | isIsNot
        DefaultQueryType.TOP_N    | [:]                               | "topN"    | "is not"
        DefaultQueryType.GROUP_BY | [(resources.m1): [having] as Set] | "groupBy" | "is"

    }

    def "Test top level buildQuery with multiple dimensions/single sort top N query"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.dimensions >> ([resources.d1, resources.d2] as Set)
        apiRequest.topN >> OptionalInt.of(5)
        apiRequest.sorts >> ([new OrderByColumn(
                new LogicalMetric(null, null, (LogicalMetricInfo) lmi1),
                SortDirection.DESC
        )] as Set)

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.getQueryType() == DefaultQueryType.GROUP_BY
    }

    def "Test top level buildQuery with single dimension/multiple sorts top N query"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.topN >> OptionalInt.of(5)
        apiRequest.sorts >> ([
                new OrderByColumn(
                        new LogicalMetric(
                                (TemplateDruidQuery) null,
                                (ResultSetMapper) null,
                                (LogicalMetricInfo) lmi1
                        ),
                        SortDirection.DESC
                ),
                new OrderByColumn(
                        new LogicalMetric(
                                (TemplateDruidQuery) null,
                                (ResultSetMapper) null,
                                (LogicalMetricInfo) lmi2
                        ),
                        SortDirection.ASC
                )
        ] as Set)

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.queryType == DefaultQueryType.GROUP_BY
    }

    def "Test top level buildQuery with multiple dimension/multiple sorts top N query"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.dimensions >> ([resources.d1, resources.d2] as Set)
        apiRequest.topN >> OptionalInt.of(5)
        apiRequest.sorts >> ([
                new OrderByColumn(new LogicalMetric(tdq, null, lmi1), SortDirection.ASC),
                new OrderByColumn(new LogicalMetric(tdq, null, lmi2), SortDirection.DESC)
        ] as Set)

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.queryType == DefaultQueryType.GROUP_BY
    }

    @Unroll
    def "A #tsDruid query is built when there #isIsNot a having clause"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.dimensions >> ([] as Set)
        apiRequest.logicalMetrics >> ([resources.m1] as Set)
        apiRequest.havings >> havingMap
        apiRequest.queryHaving >> { DefaultDruidHavingBuilder.INSTANCE.buildHavings(havingMap) }

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.queryType == queryType

        where:
        queryType                   | havingMap                         | tsDruid      | isIsNot
        DefaultQueryType.TIMESERIES | [:]                               | "timeSeries" | "is not"
        DefaultQueryType.GROUP_BY   | [(resources.m1): [having] as Set] | "groupBy"    | "is"
    }

    @Unroll
    def "TopN maps to druid #query when nDim:#nDims, nesting:#nested, nSorts:#nSorts, topN flag:#flag, havingMap:#havingMap"() {
        setup:
        apiRequest = Mock(DataApiRequest)

        apiRequest.dimensions >> { nDims > 1 ? ([resources.d1, resources.d2] as Set) : [resources.d1] as Set }
        apiRequest.topN >> OptionalInt.of(5)
        apiRequest.sorts >> {
            nSorts > 1 ?
                    [
                            new OrderByColumn(new LogicalMetric(tdq, null, lmi1), SortDirection.DESC),
                            new OrderByColumn(new LogicalMetric(tdq, null, lmi2), SortDirection.ASC)
                    ] as Set :
                    [new OrderByColumn(new LogicalMetric(null, null, "m1"), SortDirection.DESC)] as Set
        }
        apiRequest.havings >> havingMap
        apiRequest.queryHaving >> { DefaultDruidHavingBuilder.INSTANCE.buildHavings(havingMap) }

        initDefault(apiRequest)

        TOP_N.setOn(flag)

        def getTDQ = { nested -> nested ? resources.simpleNestedTemplateQuery : resources.simpleTemplateQuery }

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, getTDQ(nested))

        then:
        dq?.queryType == queryType

        where:
        queryType                 | havingMap                         | nDims | nested | nSorts | flag  | query
        DefaultQueryType.TOP_N    | [:]                               | 1     | false  | 1      | true  | "topN"
        DefaultQueryType.GROUP_BY | [(resources.m1): [having] as Set] | 1     | false  | 1      | true  | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 2     | false  | 1      | true  | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | true   | 1      | true  | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 2      | true  | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 1      | false | "groupBy"
    }

    @Unroll
    def "TimeSeries maps to druid #query when nDim:#nDims, nesting:#nested, nSorts:#nSorts, havingMap:#havingMap"() {
        setup:
        apiRequest = Mock(DataApiRequest)

        apiRequest.dimensions >> { nDims > 0 ? [resources.d1] as Set : [] as Set }

        apiRequest.sorts >> {
            nSorts > 0 ?
                    [new OrderByColumn(new LogicalMetric(null, null, "m1"), SortDirection.DESC)] as Set :
                    [] as Set
        }
        apiRequest.havings >> havingMap
        apiRequest.queryHaving >> { DefaultDruidHavingBuilder.INSTANCE.buildHavings(havingMap) }

        initDefault(apiRequest)

        def getTDQ = { nested -> nested ? resources.simpleNestedTemplateQuery : resources.simpleTemplateQuery }

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, getTDQ(nested))

        then:
        dq?.queryType == queryType

        where:
        queryType                   | havingMap                         | nDims | nested | nSorts | query
        DefaultQueryType.TIMESERIES | [:]                               | 0     | false  | 0      | "timeSeries"
        DefaultQueryType.GROUP_BY   | [(resources.m1): [having] as Set] | 0     | false  | 0      | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 1     | false  | 0      | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | true   | 0      | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | false  | 1      | "groupBy"
    }
}
