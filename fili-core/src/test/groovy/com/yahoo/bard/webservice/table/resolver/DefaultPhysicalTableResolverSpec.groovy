// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.web.DataApiRequest

import org.joda.time.Interval

import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.BinaryOperator

class DefaultPhysicalTableResolverSpec  extends Specification {
    @Shared QueryBuildingTestingResources resources
    @Shared DefaultPhysicalTableResolver resolver

    @Shared Set dimSet1
    @Shared Set dimSet3
    @Shared Set dimSet12
    @Shared Set dimSet13
    @Shared Set dimSet123
    @Shared Set dimSet14
    @Shared Set dimSet15
    @Shared Set dimSet124
    @Shared Set dimSetEmpty

    @Shared Set dimSet16
    @Shared Set dimSet17
    @Shared Set dimSet167
    @Shared Set dimSet26
    @Shared Set dimSet67

    @Shared Granularity all = AllGranularity.INSTANCE
    @Shared Set metricNamesSet0
    @Shared Set metricNamesSet1
    @Shared Set metricNamesSet12
    @Shared Set metricNamesSet45
    @Shared Set metricNamesSet126
    @Shared Set metricNamesSet123

    static boolean isInitialized = false
    static final GranularityParser granularityParser = new StandardGranularityParser()

    //TemplateDruidQuery query
    TemplateDruidQuery query
    List<Interval> intervalList = [resources.interval1]

    Map apiRequestPrototype;
    Map queryPrototype;

    def staticInitialize() {
        if (isInitialized) {
            return
        }
        isInitialized = true

        resources = new QueryBuildingTestingResources()
        resolver = new DefaultPhysicalTableResolver(new PartialDataHandler(), new DefaultingVolatileIntervalsService())
        dimSet1 = [resources.d1] as Set
        dimSet3 = [resources.d3] as Set
        dimSet12 = [resources.d1, resources.d2] as Set
        dimSet13 = [resources.d1, resources.d2] as Set
        dimSet123 = [resources.d1, resources.d2, resources.d3] as Set
        dimSet14 = [resources.d1, resources.d4] as Set
        dimSet15 = [resources.d1, resources.d5] as Set
        dimSet124 = [resources.d1, resources.d2, resources.d4] as Set
        dimSetEmpty = [] as Set

        dimSet16 = [resources.d1, resources.d6] as Set
        dimSet17 = [resources.d1, resources.d7] as Set
        dimSet167 = [resources.d1, resources.d6, resources.d7] as Set
        dimSet26 = [resources.d2, resources.d6] as Set
        dimSet67 = [resources.d6, resources.d7] as Set
        dimSet167 = [resources.d1, resources.d6, resources.d7] as Set

        metricNamesSet0 = [resources.m1.name]

        metricNamesSet123 = [resources.m1.name, resources.m2.name, resources.m3.name]
        metricNamesSet12 = [resources.m1.name, resources.m2.name]
        metricNamesSet45 = [resources.m4.name, resources.m5.name]
        metricNamesSet126 = [resources.m1.name, resources.m2.name, resources.m6.name]
        metricNamesSet1 = [resources.m1.name]
    }

    def setupSpec() {
        staticInitialize()
    }

    def setup() {
        apiRequestPrototype = [
                table: resources.lt12,
                dimensions: [],
                intervals: intervalList,
                granularity: DAY,
                filterDimensions: [],
                logicalMetrics: [] as Set
        ]

        queryPrototype = [
                filteredMetricDimensions: [] as Set,
                dependantFieldNames: metricNamesSet0,
                timeGrain: null
        ]
    }

    def metricsForNameSet(Set<String> metricNames) {
        metricNames.collect({ resources.metricDictionary.get(it)});
    }

    DataApiRequest buildDataApiRequest(Map<String, Object> prototype) {
        DataApiRequest apiRequest = Mock(DataApiRequest)
        Granularity granularity = prototype['granularity']
        granularity = granularity instanceof ZonelessTimeGrain ?
                ((ZonelessTimeGrain) granularity).buildZonedTimeGrain(UTC) :
                granularity
        apiRequest.table >> prototype['table']
        apiRequest.dimensions >> prototype['dimensions']
        apiRequest.intervals >> prototype['intervals']
        apiRequest.granularity >> granularity
        apiRequest.filterDimensions >> prototype['filterDimensions']
        apiRequest.logicalMetrics >> prototype['logicalMetrics']
        apiRequest.getFilters() >> Collections.emptyMap()
        return apiRequest
    }

    TemplateDruidQuery buildQuery(Map<String, Object> prototype) {
        TemplateDruidQuery query = Mock(TemplateDruidQuery)
        query.innermostQuery >> query
        query.dependentFieldNames >> prototype['dependantFieldNames']
        query.metricDimensions >> prototype['filteredMetricDimensions']
        query.timeGrain >> prototype['timeGrain']
        return query
    }

    @Unroll
    def "#table.name matches with #grain, #dimensions"() {
        setup:
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query)) == [table] as Set

        where:
        grain | dimensions | table
        DAY   | dimSet1    | resources.t1d
        HOUR  | dimSet1    | resources.t1h
        DAY   | dimSet1    | resources.t1h
        DAY   | dimSet123  | resources.t1d
        HOUR  | dimSet123  | resources.t1h
        HOUR  | dimSet1    | resources.t1hShort
        HOUR  | dimSet1    | resources.t2h
        HOUR  | dimSet124  | resources.t2h
        DAY   | dimSet1    | resources.t1d
        MONTH | dimSet1    | resources.t1d
        all   | dimSet1    | resources.t1d
        all   | dimSet1    | resources.t1h
        all   | dimSet123  | resources.t1d
        all   | dimSet123  | resources.t1h
    }

    @Unroll
    def "#table.name matches with zoned #grainName, #dimensions"() {
        setup:
        Granularity granularity = granularityParser.parseGranularity(grainName, UTC)
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = granularity
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query)) == [table] as Set

        where:
        grainName              | dimensions | table
        "day"                  | dimSet1    | resources.t1d
        "hour"                 | dimSet1    | resources.t1h
        "day"                  | dimSet1    | resources.t1h
        "day"                  | dimSet123  | resources.t1d
        "hour"                 | dimSet123  | resources.t1h
        "hour"                 | dimSet1    | resources.t1hShort
        "hour"                 | dimSet1    | resources.t2h
        "hour"                 | dimSet124  | resources.t2h
        "day"                  | dimSet1    | resources.t1d
        "month"                | dimSet1    | resources.t1d
        "all"                  | dimSet1    | resources.t1d
        "all"                  | dimSet1    | resources.t1h
        "all"                  | dimSet123  | resources.t1d
        "all"                  | dimSet123  | resources.t1h
    }

    @Unroll
    def "#table.name throws exception with #grain, #dimensions"() {
        setup:
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        when:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query))

        then:
        thrown(NoMatchFoundException)

        where:
        grain | dimensions | table
        HOUR  | dimSet1    | resources.t1d
        HOUR  | dimSet123  | resources.t1hShort
        HOUR  | dimSet123  | resources.t2h
        DAY   | dimSet124  | resources.t3d
    }

    @Unroll
    def "#table.name matches #dimensions containing non-agg dimensions"() {
        setup:
        Granularity granularity = all
        queryPrototype['dependantFieldNames'] = [] as Set
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['table'] = resources.ltna
        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = granularity
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet([] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query)) == [table] as Set

        where:
        dimensions | table
        dimSet16   | resources.tna1236d
        dimSet17   | resources.tna1237d
        dimSet167  | resources.tna167d
        dimSet26   | resources.tna1236d
    }

    @Unroll
    def "#table.name throws exception wth #dimensions containing non-agg dimensions"() {
        setup:
        Granularity granularity = all
        queryPrototype['dependantFieldNames'] = [] as Set
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['table'] = resources.ltna
        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = granularity
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet([] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        when:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query))

        then:
        thrown(NoMatchFoundException)

        where:
        dimensions | table
        dimSet16   | resources.tna167d
        dimSet17   | resources.tna167d
        dimSet3    | resources.tna1236d
        dimSet123  | resources.tna1236d
    }

    @Unroll
    def "#table.name matches with #metrics"() {
        setup:
        queryPrototype['dependantFieldNames'] = metrics
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query)) == [table] as Set

        where:
        grain | metrics           | table
        DAY   | metricNamesSet123 | resources.t1d
        HOUR  | metricNamesSet123 | resources.t1h
        MONTH | metricNamesSet123 | resources.t1d
        all   | metricNamesSet123 | resources.t1h

        DAY   | metricNamesSet12  | resources.t1d
        HOUR  | metricNamesSet12  | resources.t1h
        MONTH | metricNamesSet12  | resources.t1d
        all   | metricNamesSet12  | resources.t1h
    }

    @Unroll
    def "#table.name throws exception when matching with #grain, #metrics"() {
        queryPrototype['dependantFieldNames'] = metrics
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        when:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query))

        then:
        thrown(NoMatchFoundException)

        where:
        grain | metrics           | table
        DAY   | metricNamesSet126 | resources.t1d
        HOUR  | metricNamesSet126 | resources.t1h
        MONTH | metricNamesSet126 | resources.t1d
        all   | metricNamesSet126 | resources.t1h
    }

    @Unroll
    def "#table.name matches with #grain, #metrics"() {
        setup:
        queryPrototype['dependantFieldNames'] = metrics
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query)) == [table] as Set

        where:
        grain | metrics           | table
        DAY   | metricNamesSet123 | resources.t1d
        HOUR  | metricNamesSet123 | resources.t1h
        MONTH | metricNamesSet123 | resources.t1d
        all   | metricNamesSet123 | resources.t1h

        DAY   | metricNamesSet12  | resources.t1d
        HOUR  | metricNamesSet12  | resources.t1h
        MONTH | metricNamesSet12  | resources.t1d
        all   | metricNamesSet12  | resources.t1h
    }

    @Unroll
    def "#table.name throws exception with #grain, #metrics"() {
        setup:
        queryPrototype['dependantFieldNames'] = metrics
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        when:
        resolver.filter([table], new QueryPlanningConstraint(apiRequest, query))

        then:
        thrown(NoMatchFoundException)

        where:
        grain | metrics           | table
        DAY   | metricNamesSet126 | resources.t1d
        HOUR  | metricNamesSet126 | resources.t1h
        MONTH | metricNamesSet126 | resources.t1d
        all   | metricNamesSet126 | resources.t1h
    }

    @Unroll
    def "Between #table1.name and #table2.name, #expected.name is the better table"() {
        setup:
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        BinaryOperator betterTable = resolver.getBetterTableOperator(new QueryPlanningConstraint(apiRequest, query))

        expect:
        [table1, table2].stream().reduce(betterTable).get() == expected

        where:
        table1             | table2        | expected           | grain
        resources.t1d      | resources.t1h | resources.t1d      | DAY
        resources.t1d      | resources.t1h | resources.t1d      | MONTH
        resources.t1d      | resources.t1h | resources.t1d      | all
        resources.t1h      | resources.t1h | resources.t1h      | HOUR
        resources.t1hShort | resources.t1h | resources.t1hShort | HOUR
    }

    @IgnoreIf({!PARTIAL_DATA.isOn()})
    @Unroll
    def "select table with least missing data from group #table1.name #table2.name #grain #interval"() {
        setup:
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['intervals'] = [new Interval(interval)]
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        BinaryOperator betterTable = resolver.getBetterTableOperator(new QueryPlanningConstraint(apiRequest, query))

        expect:
        [(PhysicalTable) table1, (PhysicalTable) table2].stream().reduce(betterTable).get() == [table1, table1, table2].get(which)
        [(PhysicalTable) table2, (PhysicalTable) table1].stream().reduce(betterTable).get() == [table2, table1, table2].get(which)

        where:
        interval                | table1         | table2         | which | grain
        "2014-07/2014-08"       | resources.t4h1 | resources.t4h2 | 0     | MONTH
        "2014-07/2014-08"       | resources.t4h1 | resources.t4h2 | 0     | all

        "2014-07-02/2014-07-14" | resources.t4h1 | resources.t4h2 | 1     | HOUR
        "2014-07-02/2014-07-20" | resources.t4h1 | resources.t4h2 | 2     | HOUR
        "2014-07-07/2014-07-14" | resources.t4h1 | resources.t4h2 | 0     | HOUR

        "2014-07-02/2014-07-14" | resources.t4d1 | resources.t4d2 | 1     | DAY
        "2014-07-02/2014-07-21" | resources.t4d1 | resources.t4d2 | 2     | DAY
        "2014-07-07/2014-07-14" | resources.t4d1 | resources.t4d2 | 0     | DAY

        "2014-07-02/2014-07-14" | resources.t4h1 | resources.t4d2 | 1     | DAY
        "2014-07-02/2014-07-21" | resources.t4h1 | resources.t4d2 | 2     | DAY
        "2014-07-02/2014-07-21" | resources.t4h1 | resources.t4d2 | 2     | MONTH
        "2014-07-02/2014-07-21" | resources.t4h1 | resources.t4d2 | 2     | all
        "2014-07-07/2014-07-14" | resources.t4h1 | resources.t4d2 | 2     | DAY
        "2014-07-07/2014-07-14" | resources.t4h1 | resources.t4d2 | 2     | MONTH
        "2014-07-07/2014-07-14" | resources.t4h1 | resources.t4d2 | 2     | all

        "2014-07-02/2014-07-14" | resources.t4h2 | resources.t4d1 | 2     | DAY
        "2014-07-02/2014-07-21" | resources.t4h2 | resources.t4d1 | 1     | DAY
        "2014-07-07/2014-07-14" | resources.t4h2 | resources.t4d1 | 2     | DAY

        "2014-07-02/2014-07-14" | resources.t4h1 | resources.t4d1 | 2     | DAY
        "2014-07-02/2014-07-21" | resources.t4h1 | resources.t4d1 | 2     | DAY
        "2014-07-07/2014-07-14" | resources.t4h1 | resources.t4d1 | 2     | DAY

        "2014-07-02/2014-07-14" | resources.t4h2 | resources.t4d2 | 2     | DAY
        "2014-07-02/2014-07-21" | resources.t4h2 | resources.t4d2 | 2     | DAY
        "2014-07-07/2014-07-14" | resources.t4h2 | resources.t4d2 | 2     | DAY
    }

    @IgnoreIf({!PARTIAL_DATA.isOn()})
    @Unroll
    def "Multiinterval: Table #expected.name is selected with Granularity #grain, #intervals interval"() {
        setup:
        List<Interval> intervalList = intervals.collect { new Interval(it) }
        queryPrototype['dependantFieldNames'] = metricNamesSet123
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['intervals'] = intervalList
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.resolve(resources.tg1All.physicalTables, new QueryPlanningConstraint(apiRequest, query)) == expected

        where:
        intervals                  | grain || expected
        //  Fully satisfied  multiple intervals
        ["2015/2016", "2015/2016"] | MONTH || resources.partialSecond
        ["2014/2015", "2015/2016"] | MONTH || resources.wholeThird
        ["2015/2016", "2015/2016"] | all   || resources.partialSecond
        ["2014/2015", "2015/2016"] | all   || resources.wholeThird
        // One interval fully satisfied, one partially
        ["2015/2016", "2016/2017"] | MONTH || resources.partialSecond
        ["2014/2015", "2016/2017"] | MONTH || resources.wholeThird
        ["2015/2016", "2016/2017"] | all   || resources.emptyFirst
        ["2014/2015", "2016/2017"] | all   || resources.emptyFirst
    }

    @IgnoreIf({!PARTIAL_DATA.isOn()})
    @Unroll
    def "Table #expected.name is selected with Granularity #grain, #interval interval"() {
        setup:
        queryPrototype['dependantFieldNames'] = metricNamesSet123
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimSet1
        apiRequestPrototype['intervals'] = [new Interval(interval)]
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.resolve(resources.tg1All.physicalTables, new QueryPlanningConstraint(apiRequest, query)) == expected

        where:
        interval    | grain || expected
        // Fully satisfied data subset of the data
        "2015/2016" | MONTH || resources.partialSecond
        "2012/2016" | MONTH || resources.wholeThird
        "2015/2016" | all   || resources.partialSecond
        "2012/2016" | all   || resources.wholeThird
        // Partially satisfied
        "2015/2017" | MONTH || resources.partialSecond
        "2012/2017" | MONTH || resources.wholeThird
        "2015/2017" | all   || resources.emptyFirst
    }

    @IgnoreIf({!PARTIAL_DATA.isOn()})
    @Unroll
    def "Table #expected.name is selected with Granularity #grain, availability #availability, and volatility #volatility"() {
        given: "The local resolver that uses a volatility intervals service we can modify"
        resources.setupVolatileTables([
                [resources.volatileHourTable, new Interval(hourAvailable), new Interval(hourVolatile)],
                [resources.volatileDayTable, new Interval(dayAvailable), new Interval(dayVolatile)]
        ])
        PhysicalTableResolver localResolver = new DefaultPhysicalTableResolver(
                new PartialDataHandler(),
                resources.volatileIntervalsService
        )

        and: "The query information needed to resolve tables"
        queryPrototype['dependantFieldNames'] = [resources.m1.name]
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = [resources.d1]
        apiRequestPrototype['intervals'] = [new Interval("2015/2017")]
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect: "The table with more data available is preferred"
        localResolver.resolve([resources.volatileHourTable, resources.volatileDayTable], new QueryPlanningConstraint(apiRequest, query)) == expected

        where:
        hourAvailable           | hourVolatile            | dayAvailable            | dayVolatile             | grain || expected
        // Hour has more data available in the volatile request bucket (it goes further into the year)
        "2015-01-01/2016-12-05" | "2016-12-05/2016-12-06" | "2015-01-01/2016-12-01" | "2016-12-01/2017-01-01" | MONTH || resources.volatileHourTable
        // Day has more data available in the volatile request bucket (it starts earlier in the year)
        "2015-02-01/2016-12-05" | "2016-12-05/2016-12-06" | "2015-01-01/2016-12-01" | "2016-12-01/2017-01-01" | MONTH || resources.volatileDayTable
        // Hour has more data available in the volatile request bucket (it goes further into the year), request is day grain
        "2015-01-01/2016-12-05" | "2016-12-05/2016-12-06" | "2015-01-01/2016-12-01" | "2016-12-01/2017-01-01" | DAY   || resources.volatileHourTable
        // Hour has more data available in the volatile request bucket (it goes further into the year), request is all grain
        "2015-01-01/2016-12-05" | "2016-12-05/2016-12-06" | "2015-01-01/2016-12-01" | "2016-12-01/2017-01-01" | all   || resources.volatileHourTable

        availability = expected == resources.volatileHourTable ? hourAvailable : dayAvailable
        volatility = expected == resources.volatileHourTable ? hourVolatile : dayVolatile
    }

    @Unroll
    def "With grain #grain expected #expected.name for #tablegroup under dimensions #dimensions and metrics #metrics"() {
        queryPrototype['dependantFieldNames'] = metrics as Set
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.resolve(tablegroup.physicalTables, new QueryPlanningConstraint(apiRequest, query)) == expected

        where:
        grain | dimensions | tablegroup     | metrics             | expected
        DAY   | dimSet1    | resources.tg1h | metricNamesSet123   | resources.t1d
        HOUR  | dimSet1    | resources.tg1h | metricNamesSet123   | resources.t1hShort
        HOUR  | dimSet12   | resources.tg1h | metricNamesSet123   | resources.t1hShort
        HOUR  | dimSet123  | resources.tg1h | metricNamesSet123   | resources.t1h
        DAY   | dimSet123  | resources.tg1h | metricNamesSet123   | resources.t1d
        DAY   | dimSet123  | resources.tg1d | metricNamesSet123   | resources.t1d

        DAY   | dimSet124  | resources.tg2h | metricNamesSet45    | resources.t2h
        HOUR  | dimSet124  | resources.tg2h | metricNamesSet45    | resources.t2h
        DAY   | dimSet15   | resources.tg3d | [resources.m6.name] | resources.t3d
    }

    @Unroll
    def "Test select table from bad group "() {
        queryPrototype['dependantFieldNames'] = metricNamesSet123
        queryPrototype['filteredMetricDimensions'] = filteredDimensions
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        when:
        resolver.resolve(tablegroup.physicalTables, new QueryPlanningConstraint(apiRequest, query))

        then:
        thrown(NoMatchFoundException)

        where:
        grain | dimensions | tablegroup         | filteredDimensions
        HOUR  | dimSet1    | resources.tg1d     | []
        HOUR  | dimSet123  | resources.tg1Short | []
        DAY   | dimSet124  | resources.tg1h     | []
        DAY   | dimSet124  | resources.tg1d     | []
        HOUR  | dimSet15   | resources.tg3d     | []
        DAY   | []         | resources.tg1d     | dimSet124
    }

    @Unroll
    def "Test ordering is preserved in equal compares"() {
        TemplateDruidQuery query = buildQuery(queryPrototype);

        apiRequestPrototype['dimensions'] = dimensions
        apiRequestPrototype['granularity'] = grain
        apiRequestPrototype['logicalMetrics'] = metricsForNameSet(queryPrototype['dependantFieldNames'] as Set)
        DataApiRequest apiRequest = buildDataApiRequest(apiRequestPrototype)

        expect:
        resolver.resolve(tablegroup.physicalTables, new QueryPlanningConstraint(apiRequest, query)) == table

        where:
        grain | dimensions | tablegroup     | table
        HOUR  | dimSet1    | resources.tg4h | resources.t1h
        HOUR  | dimSet1    | resources.tg5h | resources.t2h
        HOUR  | dimSet12   | resources.tg4h | resources.t1h
        HOUR  | dimSet12   | resources.tg5h | resources.t2h
    }
}
