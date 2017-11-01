// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils.TestAvailability

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification

class TablesServletSpec extends Specification {
    def "getConstrainedLogicalTableAvailability returns intervals constrained by availability"() {
        given: "Unconstrained intervals as [2017, 2021] and constrained as [2018, 2020]"
        Interval unconstrainedInterval1 = new Interval("2017/2019") // [2017, 2018, 2019]
        Interval constrainedInterval1 = new Interval("2018/2019")   // [      2018, 2019]
        Interval unconstrainedInterval2 = new Interval("2019/2021") // [2019, 2020, 2021]
        Interval constrainedInterval2 = new Interval("2019/2020")   // [2019, 2020,     ]

        TestAvailability availability1 = new TestAvailability(
                [Mock(DataSourceName)] as Set,
                ["availability": [unconstrainedInterval1] as Set]
        )
        TestAvailability availability2 = new TestAvailability(
                [Mock(DataSourceName)] as Set,
                ["availability": [unconstrainedInterval2] as Set]
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
        queryPlanningConstraint.intervals >> ([constrainedInterval1, constrainedInterval2] as Set)

        ConstrainedTable constrainedTable1 = new ConstrainedTable(configPhysicalTable1, queryPlanningConstraint)
        ConstrainedTable constrainedTable2 = new ConstrainedTable(configPhysicalTable2, queryPlanningConstraint)
        configPhysicalTable1.withConstraint(_ as DataSourceConstraint) >> constrainedTable1
        configPhysicalTable2.withConstraint(_ as DataSourceConstraint) >> constrainedTable2

        TableGroup tableGroup = Mock(TableGroup)
        tableGroup.getPhysicalTables() >> ([configPhysicalTable1, configPhysicalTable2] as Set)

        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTable.getTableGroup() >> tableGroup

        when:
        SimplifiedIntervalList constrainedInterval = TablesServlet.getConstrainedLogicalTableAvailability(
                logicalTable,
                Mock(QueryPlanningConstraint)
        )

        then:
        constrainedInterval == SimplifiedIntervalList.simplifyIntervals([constrainedInterval1, constrainedInterval2])
    }
}
