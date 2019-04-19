// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PermissivePhysicalTableSpec extends Specification {
    @Shared PermissivePhysicalTable permissivePhysicalTable

    @Shared DimensionColumn disjointIntervalColumn
    @Shared MetricColumn leftAbuttingIntervalColumn, rightAbuttingIntervalColumn

    @Shared Interval leftAbuttingInterval
    @Shared Interval rightAbuttingInterval
    @Shared Interval disjointInterval
    @Shared Interval invisibleInterval

    def setupSpec() {
        disjointIntervalColumn = new DimensionColumn(
                new KeyValueStoreDimension(
                        "disjointIntervalColumn",
                        null,
                        [BardDimensionField.ID] as LinkedHashSet,
                        MapStoreManager.getInstance("dimension"),
                        ScanSearchProviderManager.getInstance("apiProduct")
                )
        )
        leftAbuttingIntervalColumn = new MetricColumn("leftAbuttingIntervalColumn")
        rightAbuttingIntervalColumn = new MetricColumn("rightAbuttingIntervalColumn")

        disjointInterval = new Interval("2018-01-01/2018-02-01")
        leftAbuttingInterval = new Interval("2017-01-01/2017-02-01")
        rightAbuttingInterval = new Interval("2017-02-01/2017-03-01")
        invisibleInterval = new Interval("2016-01-01/2016-02-01")

        permissivePhysicalTable = new PermissivePhysicalTable(
                TableName.of('test table'),
                DAY.buildZonedTimeGrain(UTC),
                [disjointIntervalColumn, leftAbuttingIntervalColumn, rightAbuttingIntervalColumn] as Set,
                [:],
                new TestDataSourceMetadataService([
                        (disjointIntervalColumn.getName())     : [disjointInterval] as Set,
                        (leftAbuttingIntervalColumn.getName()) : [leftAbuttingInterval] as Set,
                        (rightAbuttingIntervalColumn.getName()): [rightAbuttingInterval] as Set,
                        (new Column('invisible').getName())    : [invisibleInterval] as Set
                ])
        )
    }

    @Unroll
    def "The spanning intervals of #allColumnNames are returned without constraint even when individual columns are not fully covering the interval"() {
        setup:
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.getAllColumnNames() >> allColumnNames

        expect:
        permissivePhysicalTable.getAvailableIntervals(constraints) == new SimplifiedIntervalList(expected)

        where:
        allColumnNames                                                | expected
        ["disjointIntervalColumn"]                                    | [invisibleInterval, disjointInterval, leftAbuttingInterval, rightAbuttingInterval] as Set
        ["leftAbuttingIntervalColumn"]                                | [invisibleInterval, disjointInterval, leftAbuttingInterval, rightAbuttingInterval] as Set
        ["rightAbuttingIntervalColumn"]                               | [invisibleInterval, disjointInterval, leftAbuttingInterval, rightAbuttingInterval] as Set
        ["disjointIntervalColumn", "leftAbuttingIntervalColumn"]      | [invisibleInterval, disjointInterval, leftAbuttingInterval, rightAbuttingInterval] as Set
        ["disjointIntervalColumn",  "rightAbuttingIntervalColumn"]    | [invisibleInterval, disjointInterval, leftAbuttingInterval, rightAbuttingInterval] as Set
        ["leftAbuttingIntervalColumn", "rightAbuttingIntervalColumn"] | [invisibleInterval, disjointInterval, leftAbuttingInterval, rightAbuttingInterval] as Set
    }
}
