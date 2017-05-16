// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.web.DataApiRequest

import spock.lang.Specification

/**
 *  Tests for methods in SchemaPhysicalTableMatcherSpec
 */
class SchemaPhysicalTableMatcherSpec extends Specification {
    SchemaPhysicalTableMatcher schemaPhysicalTableMatcher
    DataApiRequest request = Mock(DataApiRequest)
    TemplateDruidQuery query = Mock(TemplateDruidQuery)

    PhysicalTable physicalTable
    LinkedHashSet<DimensionField> dimensionFields
    DimensionDictionary dimensionDictionary
    Set<Dimension> dimSet

    def setup() {
        // table containing a logical name for a dimension same as physical name for other dimension
        dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC] as LinkedHashSet
        dimSet = [

                new KeyValueStoreDimension(
                        "dimA",
                        "desc-dimA",
                        dimensionFields,
                        MapStoreManager.getInstance("dimA"),
                        ScanSearchProviderManager.getInstance("dimA"),
                        [] as Set
                ),
                new KeyValueStoreDimension(
                        "dimCommon",
                        "desc-dimCommon",
                        dimensionFields,
                        MapStoreManager.getInstance("dimCommon"),
                        ScanSearchProviderManager.getInstance("dimCommon"),
                        [] as Set
                ),
                new KeyValueStoreDimension(
                        "dimB",
                        "desc-dimB",
                        dimensionFields,
                        MapStoreManager.getInstance("dimB"),
                        ScanSearchProviderManager.getInstance("dimB"),
                        [] as Set
                ),
        ] as Set

        physicalTable = new StrictPhysicalTable(
                "test table",
                DAY.buildZonedTimeGrain(UTC),
                dimSet.collect {new DimensionColumn(it)}.toSet(),
                ['dimA':'druidDimA', 'dimCommon': 'druidDimC', 'dimB': 'dimCommon'],
                Mock(DataSourceMetadataService)
        )

        request.getGranularity() >> DAY.buildZonedTimeGrain(UTC)
        query.getInnermostQuery() >> query
        query.getDimensions() >> (['dimB'] as Set)
        query.getMetricDimensions() >> ([] as Set)
        query.getDependentFieldNames() >> ([] as Set)
        request.getFilterDimensions() >> []
        request.getDimensions() >> (dimSet)
        request.getFilters() >> Collections.emptyMap()
        request.getIntervals() >> []
        request.getLogicalMetrics() >> []

        dimensionDictionary = new DimensionDictionary(dimSet)
        schemaPhysicalTableMatcher = new SchemaPhysicalTableMatcher(
                new QueryPlanningConstraint(request, query)
        )
    }

    def "schema matcher resolves table containing a logical name for a dimension same as physical name for other dimension"() {
        expect:
        schemaPhysicalTableMatcher.test(physicalTable)
    }
}
