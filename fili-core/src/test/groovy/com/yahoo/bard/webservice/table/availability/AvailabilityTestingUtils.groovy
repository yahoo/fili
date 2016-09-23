// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.metadata.SegmentMetadata
import com.yahoo.bard.webservice.table.PhysicalTable
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
                .each { _, PhysicalTable table ->
                    Map<String, Set<Interval>> metricIntervals = table.getColumns(MetricColumn.class)
                            .collectEntries {
                                    [(it.name): intervalSet]
                            }
                    Map<String, Set<Interval>> dimensionIntervals = table.getColumns(DimensionColumn.class)
                            .collectEntries {
                                    [(table.getPhysicalColumnName(it.getDimension().getApiName())): intervalSet]
                            }
                    // set new cache
                    table.resetColumns(
                            new SegmentMetadata(dimensionIntervals, metricIntervals),
                            jtb.configurationLoader.dimensionDictionary
                    )
                }
    }
}
