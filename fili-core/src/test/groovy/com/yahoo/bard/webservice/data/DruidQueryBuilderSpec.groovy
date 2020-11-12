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
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder
import com.yahoo.bard.webservice.druid.model.datasource.DefaultDataSourceType
import com.yahoo.bard.webservice.druid.model.filter.AndFilter
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.InFilter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
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
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.ApiHaving
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders
import com.yahoo.bard.webservice.web.filters.ApiFilters

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DruidQueryBuilderSpec extends Specification {

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

    LogicalMetric lm1
    static LogicalMetricInfo lmi1 = new LogicalMetricInfo("m1")
    static LogicalMetricInfo lmi2 = new LogicalMetricInfo("m2")

    LogicalMetricInfo m1LogicalMetric = new LogicalMetricInfo("lm1")

    static final DruidFilterBuilder DRUID_FILTER_BUILDER = new DruidOrFilterBuilder()

    List<Interval> intervals
    static FilterBinders filterBinders = FilterBinders.instance


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

        LinkedHashSet<OrderByColumn> orderByColumns = [new OrderByColumn(lmi1.name, SortDirection.DESC)]
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
        lm1 = new LogicalMetricImpl(resources.simpleTemplateQuery, new NoOpResultSetMapper(), m1LogicalMetric)

        apiRequest.getTable() >> resources.lt12
        apiRequest.getGranularity() >> HOUR.buildZonedTimeGrain(UTC)
        apiRequest.getTimeZone() >> UTC
        apiRequest.getDimensions() >> ([resources.d1] as Set)
        apiRequest.getAllGroupingDimensions() >> ([resources.d1] as Set)

        // Is this correct? All filters use the same dimension, and since we are not merging with the existing set the result only picks up the last filter.
        ApiFilters apiFilters = new ApiFilters(
                apiFiltersByName.collectEntries {[(resources.d3): [it.value] as Set]} as Map<Dimension, Set<ApiFilter>>
        )

        apiRequest.getApiFilters() >> { apiFilters }
        apiRequest.withFilters(_) >> {
            ApiFilters newFilters ->
                apiFilters = newFilters
                apiRequest
        }
        apiRequest.getLogicalMetrics() >> ([lm1] as Set)
        apiRequest.getIntervals() >> intervals
        apiRequest.getTopN() >> Optional.empty()
        apiRequest.getSorts() >> ([] as Set)
        apiRequest.getCount() >> Optional.empty()
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
        1 * apiRequest.getAllGroupingDimensions()
    }

    @Unroll
    def "A #topNDruid query is built when there #isIsNot a having clause, and #cannot optimize"() {
        setup:
        apiRequest = Mock(DataApiRequest)

        apiRequest.getTopN() >> Optional.of(5)
        apiRequest.getSorts() >> ([new OrderByColumn(lmi1.name, SortDirection.DESC)] as Set)
        apiRequest.havings >> havingMap
        apiRequest.optimizeBackendQuery() >> canOptimize

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.getQueryType() == queryType

        where:
        queryType                 | havingMap                         | topNDruid | isIsNot  | cannot   | canOptimize
        DefaultQueryType.TOP_N    | [:]                               | "topN"    | "is not" | "can"    | true
        DefaultQueryType.GROUP_BY | [:]                               | "groupBy" | "is not" | "cannot" | false
        DefaultQueryType.GROUP_BY | [(resources.m1): [having] as Set] | "groupBy" | "is"     | "can"    | true
        DefaultQueryType.GROUP_BY | [(resources.m1): [having] as Set] | "groupBy" | "is"     | "cannot" | false

    }

    def "Test top level buildQuery with multiple dimensions/single sort top N query"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.dimensions >> ([resources.d1, resources.d2] as Set)
        apiRequest.topN >> Optional.of(5)
        apiRequest.sorts >> ([new OrderByColumn(lmi1.name, SortDirection.DESC)] as Set)

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.getQueryType() == DefaultQueryType.GROUP_BY
        1 * apiRequest.getAllGroupingDimensions()
    }

    def "Test top level buildQuery with single dimension/multiple sorts top N query"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.topN >> Optional.of(5)
        apiRequest.sorts >> ([
                new OrderByColumn(lmi1.name, SortDirection.DESC),
                new OrderByColumn(lmi2.name, SortDirection.ASC)
        ] as Set)

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.queryType == DefaultQueryType.GROUP_BY
        1 * apiRequest.getAllGroupingDimensions()
    }

    def "Test top level buildQuery with multiple dimension/multiple sorts top N query"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.dimensions >> ([resources.d1, resources.d2] as Set)
        apiRequest.topN >> Optional.of(5)
        apiRequest.sorts >> ([
                new OrderByColumn(lmi1.name, SortDirection.ASC),
                new OrderByColumn(lmi2.name, SortDirection.DESC)
        ] as Set)

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.queryType == DefaultQueryType.GROUP_BY
        1 * apiRequest.getAllGroupingDimensions()
    }

    @Unroll
    def "A #tsDruid query is built when there #isIsNot a having clause and the request #cancannot be optimized"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.dimensions >> ([] as Set)
        apiRequest.logicalMetrics >> ([resources.m1] as Set)
        apiRequest.havings >> havingMap
        apiRequest.queryHaving >> { DefaultDruidHavingBuilder.INSTANCE.buildHavings(havingMap) }
        apiRequest.optimizeBackendQuery() >> canOptimize

        initDefault(apiRequest)

        when:
        DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        then:
        dq?.queryType == queryType

        where:
        queryType                   | havingMap                         | tsDruid      | isIsNot    | cancannot | canOptimize
        DefaultQueryType.TIMESERIES | [:]                               | "timeSeries" | "is not"   | "can"     | true
        DefaultQueryType.GROUP_BY   | [:]                               | "timeSeries" | "is not"   | "cannot"  | false
        DefaultQueryType.GROUP_BY   | [(resources.m1): [having] as Set] | "groupBy"    | "is"       | "can"     | true
        DefaultQueryType.GROUP_BY   | [(resources.m1): [having] as Set] | "groupBy"    | "is"       | "cannot"  | false
    }

    @Unroll
    def "TopN maps to druid #query when nDim:#nDims, nesting:#nested, nSorts:#nSorts, topN flag:#flag, havingMap:#havingMap and canOptimize: #canOptimize"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.optimizeBackendQuery() >> canOptimize

        apiRequest.dimensions >> { nDims > 1 ? ([resources.d1, resources.d2] as Set) : [resources.d1] as Set }
        apiRequest.topN >> Optional.of(5)
        apiRequest.sorts >> {
            nSorts > 1 ?
                    [
                            new OrderByColumn(lmi1.name, SortDirection.DESC),
                            new OrderByColumn(lmi2.name, SortDirection.ASC)
                    ] as Set :
                    [new OrderByColumn("m1", SortDirection.DESC)] as Set
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
        queryType                 | havingMap                         | nDims | nested | nSorts | flag  | canOptimize   | query
        DefaultQueryType.TOP_N    | [:]                               | 1     | false  | 1      | true  | true          | "topN"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 1      | true  | false         | "groupBy"
        DefaultQueryType.GROUP_BY | [(resources.m1): [having] as Set] | 1     | false  | 1      | true  | true          | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 2     | false  | 1      | true  | true          | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | true   | 1      | true  | true          | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 2      | true  | true          | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 1      | false | true          | "groupBy"
        DefaultQueryType.GROUP_BY | [(resources.m1): [having] as Set] | 1     | false  | 1      | true  | false         | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 2     | false  | 1      | true  | false         | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | true   | 1      | true  | false         | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 2      | true  | false         | "groupBy"
        DefaultQueryType.GROUP_BY | [:]                               | 1     | false  | 1      | false | false         | "groupBy"
    }

    @Unroll
    def "TimeSeries maps to druid #query when nDim:#nDims, nesting:#nested, nSorts:#nSorts, havingMap:#havingMap and canOptimize: #canOptimize"() {
        setup:
        apiRequest = Mock(DataApiRequest)
        apiRequest.optimizeBackendQuery() >> canOptimize

        apiRequest.dimensions >> { (nDims > 0) ? [resources.d1] as Set : [] as Set }

        apiRequest.sorts >> {
            nSorts > 0 ?
                    [new OrderByColumn("m1", SortDirection.DESC)] as Set :
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
        queryType                   | havingMap                         | nDims | nested | nSorts | canOptimize | query
        DefaultQueryType.TIMESERIES | [:]                               | 0     | false  | 0      | true        | "timeSeries"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | false  | 0      | false       | "groupBy"
        DefaultQueryType.GROUP_BY   | [(resources.m1): [having] as Set] | 0     | false  | 0      | true        | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 1     | false  | 0      | true        | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | true   | 0      | true        | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | false  | 1      | true        | "groupBy"
        DefaultQueryType.GROUP_BY   | [(resources.m1): [having] as Set] | 0     | false  | 0      | false       | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 1     | false  | 0      | false       | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | true   | 0      | false       | "groupBy"
        DefaultQueryType.GROUP_BY   | [:]                               | 0     | false  | 1      | false       | "groupBy"
    }

    def "LogicalTable filters and ApiRequest filters merge properly"() {
        setup:
        // inject druid filter builder to test interactions against
        DruidFilterBuilder dfb = Mock()
        DruidQueryBuilder builder = new DruidQueryBuilder(
                resources.logicalDictionary,
                resolver,
                dfb,
                resources.druidHavingBuilder
        )

        // create logical table with test filter
        ApiFilter tableFilter = filterBinders.generateApiFilter("ageBracket|id-eq[1,2,3,4]", resources.dimensionDictionary)
        ApiFilters tableFilters = new ApiFilters([(resources.d3) : [tableFilter] as Set] as Map)
        LogicalTable baseTable = resources.lt12
        TableGroup tableGroup = baseTable.getTableGroup();
        tableGroup = new TableGroup(
                tableGroup.getPhysicalTables(),
                tableGroup.getApiMetricNames(),
                tableGroup.getDimensions(),
                tableFilters
        );
        LogicalTable table = new LogicalTable(
                baseTable.getName(),
                baseTable.getCategory(),
                baseTable.getLongName(),
                baseTable.getGranularity(),
                baseTable.getRetention(),
                baseTable.getDescription(),
                tableGroup,
                resources.metricDictionary
        )

        // create and prep api request
        apiRequest = Mock(DataApiRequest)
        apiRequest.getTable() >> table
        initDefault(apiRequest)

        // put table into table dictionary
        resources.logicalDictionary.put(TableIdentifier.create(apiRequest), table)

        // prep expected api filters
        ApiFilter requestFilter = apiRequest.getApiFilters().get(resources.d3).iterator().next()
        ApiFilters expectedApiFilters = new ApiFilters(
                [
                    (resources.d3): [tableFilter, requestFilter] as Set
                ] as Map
        )

        when:
        DruidAggregationQuery daq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)
        AndFilter andFilter = (AndFilter) daq.getFilter()

        then: "The combined ApiFilters are used to build the druid filter"
        1 * dfb.buildFilters({(ApiFilters) it == expectedApiFilters}) >> {
            ApiFilters filterMap ->
                System.println(filterMap)
                resources.druidFilterBuilder.buildFilters(filterMap)
        }

        and: "it now has the negative assertion from the request"

        andFilter != null
        andFilter.getFields().any() {
            it instanceof NotFilter && ((InFilter) ((NotFilter) it).field).values.contains("3")
        }

        and: "it also contains the positive assertion from the table"
        andFilter.getFields().any() {
            it instanceof InFilter && ((InFilter) it).values.equals(["1", "2", "3", "4"] as TreeSet)
        }
    }
}
