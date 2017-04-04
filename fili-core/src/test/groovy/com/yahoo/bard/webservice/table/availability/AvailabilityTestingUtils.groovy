// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import org.joda.time.Interval

import spock.lang.Specification
/**
 * Contains a collection of utility methods to aid in testing functionality that relies on table availability, like
 * partial data and volatility.
 */
class AvailabilityTestingUtils extends Specification {

    /**
     * Make the specified physical tables believe they have data available for the specified interval.
     *
     * @param jtb  The JerseyTestBinder instance for this test
     * @param interval  The interval the specified physical tables should think they have data for
     * @param namesOfTablesToPopulate The names of the physical tables whose availability should include the specified
     * interval, if empty then every table is made available for the specified interval, defaults to empty
     */
    static def populatePhysicalTableCacheIntervals(
            JerseyTestBinder jtb,
            Interval interval,
            Set<String> namesOfTablesToPopulate = [] as Set
    ) {
        Set<Interval> intervalSet = [interval] as Set

        PhysicalTableDictionary physicalTableDictionary = jtb.configurationLoader.physicalTableDictionary
        Set<String> tableNames = namesOfTablesToPopulate ?: physicalTableDictionary.keySet()

        physicalTableDictionary
                .findAll { tableName, _ -> tableName in tableNames}
                .each { _, ConcretePhysicalTable table ->
                    Map<String, Set<Interval>> allIntervals = table.getSchema().getPhysicalColumnNames().collectEntries {
                        [(it): intervalSet]
                    }
            // set new cache
                    table.setAvailability(new ConcreteAvailability(table.getTableName(), table.getSchema().getPhysicalColumnNames(), new TestDataSourceMetadataService(allIntervals)))
                }
    }
}
