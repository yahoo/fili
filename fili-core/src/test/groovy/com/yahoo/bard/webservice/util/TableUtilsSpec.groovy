// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.filters.ApiFilters

import org.joda.time.Interval

import spock.lang.Ignore
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
        request.getApiFilters() >> new ApiFilters(filterDimensions.collectEntries {[(it): [] as Set]});
        query.metricDimensions >> metricFilterDimensions
        query.getDependentFieldNames() >> new HashSet<String>()
        expected = expected as LinkedHashSet

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
        request.getApiFilters() >> new ApiFilters(ds1.collectEntries {[(it): [] as Set]});
        query.metricDimensions >> ds1
        query.dependentFieldNames >> ([metric1, metric2, metric3] as LinkedHashSet)

        expect:
        TableUtils.getColumnNames(request, query) == [d1Name, metric1, metric2, metric3] as LinkedHashSet
    }

    def "metric name correctly is correctly represented as logicalName" () {
        setup:
        request.dimensions >> []
        request.getApiFilters() >> new ApiFilters()
        query.metricDimensions >> []
        query.dependentFieldNames >> ([metric1] as LinkedHashSet)

        expect:
        TableUtils.getColumnNames(request, query) == [metric1] as LinkedHashSet
    }

    @Ignore("The corresponding implementation has never been implemented.  This test is testing test code.")
    def "getConstrainedLogicalTableAvailability returns intervals constrained by availability"() {
        given: "Unconstrained intervals as [2017, 2021] and constrained as [2018, 2020]"
        Interval unconstrainedInterval1 = new Interval("2017/2019") // [2017, 2018, 2019]
        Interval constrainedInterval1 = new Interval("2018/2019")   // [      2018, 2019]
        Interval unconstrainedInterval2 = new Interval("2019/2021") // [2019, 2020, 2021]
        Interval constrainedInterval2 = new Interval("2019/2020")   // [2019, 2020,     ]

        AvailabilityTestingUtils.TestAvailability availability1 = new AvailabilityTestingUtils.TestAvailability(
                [Mock(DataSourceName)] as LinkedHashSet,
                ["availability": [unconstrainedInterval1] as LinkedHashSet]
        )
        AvailabilityTestingUtils.TestAvailability availability2 = new AvailabilityTestingUtils.TestAvailability(
                [Mock(DataSourceName)] as LinkedHashSet,
                ["availability": [unconstrainedInterval2] as LinkedHashSet]
        )

        PhysicalTableSchema physicalTableSchema = Mock(PhysicalTableSchema)
        physicalTableSchema.getPhysicalColumnName(_ as String) >> ""
        physicalTableSchema.getColumns() >> Collections.emptySet()

        ConfigPhysicalTable configPhysicalTable1 = Mock(ConfigPhysicalTable)
        ConfigPhysicalTable configPhysicalTable2 = Mock(ConfigPhysicalTable)
        configPhysicalTable1.getAvailability() >> availability1
        configPhysicalTable2.getAvailability() >> availability2

        configPhysicalTable1.getSchema() >> physicalTableSchema
        configPhysicalTable2.getSchema() >> physicalTableSchema

        QueryPlanningConstraint queryPlanningConstraint = Mock(QueryPlanningConstraint)
        queryPlanningConstraint.intervals >> [constrainedInterval1, constrainedInterval2]
        queryPlanningConstraint.getAllColumnNames() >> ([] as Set)

        configPhysicalTable1.getAvailableIntervals(_) >> { constraint ->
            return availability1.getAvailableIntervals(constraint)
        }
        configPhysicalTable2.getAvailableIntervals(_) >> { constraint ->
            return availability2.getAvailableIntervals(constraint)
        }

        ConstrainedTable constrainedTable1 = new ConstrainedTable(configPhysicalTable1, queryPlanningConstraint)
        ConstrainedTable constrainedTable2 = new ConstrainedTable(configPhysicalTable2, queryPlanningConstraint)
        configPhysicalTable1.withConstraint(_ as DataSourceConstraint) >> constrainedTable1
        configPhysicalTable2.withConstraint(_ as DataSourceConstraint) >> constrainedTable2

        TableGroup tableGroup = Mock(TableGroup)
        tableGroup.getPhysicalTables() >> ([configPhysicalTable1, configPhysicalTable2] as Set)

        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTable.getTableGroup() >> tableGroup


        when:
        SimplifiedIntervalList constrainedInterval = TableUtils.getConstrainedLogicalTableAvailability(
                logicalTable,
                Mock(QueryPlanningConstraint)
        )

        then:

        constrainedInterval == SimplifiedIntervalList.simplifyIntervals([constrainedInterval1, constrainedInterval2])
    }

    def "logicalTableAvailability returns union of all the intervals for the availability"() {
        given: "two intervals [2017, 2018] and [2018, 2019]"
        Interval interval1 = new Interval("2017/2018")
        Interval interval2 = new Interval("2018/2019")

        AvailabilityTestingUtils.TestAvailability availability1 = new AvailabilityTestingUtils.TestAvailability(
                [Mock(DataSourceName)] as LinkedHashSet,
                ["availability": [interval1]]
        )
        AvailabilityTestingUtils.TestAvailability availability2 = new AvailabilityTestingUtils.TestAvailability(
                [Mock(DataSourceName)] as LinkedHashSet,
                ["availability": [interval2]]
        )

        PhysicalTableSchema physicalTableSchema = Mock(PhysicalTableSchema)
        physicalTableSchema.getPhysicalColumnName(_ as String) >> ""
        physicalTableSchema.getColumns() >> Collections.emptySet()

        SimplifiedIntervalList simplifiedIntervalList1 = new SimplifiedIntervalList(Arrays.asList(interval1))
        SimplifiedIntervalList simplifiedIntervalList2 = new SimplifiedIntervalList(Arrays.asList(interval2))

        Column column1 = Mock(Column)
        Column column2 = Mock(Column)

        Map<Column, SimplifiedIntervalList> intervalMap1 = new HashMap<>()
        intervalMap1.put(column1, simplifiedIntervalList1)

        Map<Column, SimplifiedIntervalList> intervalMap2 = new HashMap<>()
        intervalMap2.put(column2, simplifiedIntervalList2)

        ConfigPhysicalTable configPhysicalTable1 = Mock(ConfigPhysicalTable)
        ConfigPhysicalTable configPhysicalTable2 = Mock(ConfigPhysicalTable)
        configPhysicalTable1.getAvailability() >> availability1
        configPhysicalTable1.getAllAvailableIntervals() >> intervalMap1
        configPhysicalTable2.getAllAvailableIntervals() >> intervalMap2
        configPhysicalTable2.getAvailability() >> availability2
        configPhysicalTable1.getSchema() >> physicalTableSchema
        configPhysicalTable2.getSchema() >> physicalTableSchema

        TableGroup tableGroup = Mock(TableGroup)
        tableGroup.getPhysicalTables() >> ([configPhysicalTable1, configPhysicalTable2] as LinkedHashSet)

        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTable.getTableGroup() >> tableGroup

        when:
        SimplifiedIntervalList intervals = TableUtils.logicalTableAvailability(
                logicalTable
        )

        then:
        intervals == SimplifiedIntervalList.simplifyIntervals([interval1, interval2])
    }
}
