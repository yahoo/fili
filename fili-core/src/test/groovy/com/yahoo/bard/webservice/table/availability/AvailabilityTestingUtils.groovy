// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification

import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Contains a collection of utility methods to aid in testing functionality that relies on table availability, like
 * partial data and volatility.
 */
class AvailabilityTestingUtils extends Specification {

    static class TestAvailability implements Availability {
        final Set<DataSourceName> sourceDataSourceNames
        final Map<String, Set<Interval>> intervals

        TestAvailability(Set<DataSourceName> sourceDataSourceNames, Map<String, Set<Interval>> intervals) {
            this.sourceDataSourceNames = sourceDataSourceNames
            this.intervals = intervals
        }

        @Override
        Set<DataSourceName> getDataSourceNames() {
            return sourceDataSourceNames
        }

        @Override
        Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
            intervals.entrySet().collectEntries {
                [(it.key): new SimplifiedIntervalList(it.value)]
            }
        }
    }

    /**
     * Make the specified physical tables believe they have data available for the specified interval.
     *
     * @param jtb  The JerseyTestBinder instance for this test
     * @param interval  The interval the specified physical tables should think they have data for
     * @param namesOfTablesToPopulate The names of the physical tables whose availability should include the specified
     * interval, if empty then every table is made available for the specified interval, defaults to empty
     */
    static populatePhysicalTableCacheIntervals(
            JerseyTestBinder jtb,
            Interval interval,
            Set<String> namesOfTablesToPopulate = [] as Set
    ) {
        Set<Interval> intervalSet = [interval] as Set

        PhysicalTableDictionary physicalTableDictionary = jtb.configurationLoader.physicalTableDictionary
        Set<String> tableNames = namesOfTablesToPopulate ?: physicalTableDictionary.keySet()

        physicalTableDictionary
                .findAll { tableName, _ -> tableName in tableNames }
                .each { _, table ->
                    Map<String, Set<Interval>> metricIntervals = table.schema.getColumns(MetricColumn)
                            .collectEntries { [(it.name): intervalSet] }

                    // Below code assumes unique one-to-one mapping from logical to physical name in testing resource
                    Map<String, Set<Interval>> dimensionIntervals = table.schema.getColumns(DimensionColumn)
                            .collectEntries {
                        [(table.schema.getPhysicalColumnName(it.name)): intervalSet]
                    }

                    Map<String, Set<Interval>> allIntervals = Stream.concat(
                            dimensionIntervals.entrySet().stream(),
                            metricIntervals.entrySet().stream()
                    ).collect(Collectors.toMap({ it.key }, { it.value }))

                    // set new available interval cache
                    table.setAvailability(
                            new TestAvailability(table.getAvailability().getDataSourceNames(), allIntervals)
                    )
                }
    }
}
