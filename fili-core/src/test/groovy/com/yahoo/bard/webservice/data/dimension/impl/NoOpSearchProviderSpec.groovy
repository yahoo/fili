// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.web.ApiFilter

import org.joda.time.DateTime
import spock.lang.Specification

class NoOpSearchProviderSpec extends Specification {
    KeyValueStoreDimension keyValueStoreDimension
    SearchProvider noOpSearchProvider

    private SystemConfig systemConfig = SystemConfigProvider.getInstance()

    def setup() {
        KeyValueStore keyValueStore = MapStoreManager.getInstance("other")
        noOpSearchProvider = NoOpSearchProviderManager.getInstance("other")

        LinkedHashSet<DimensionField> dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC]

        keyValueStoreDimension = new KeyValueStoreDimension("other", "other-description",dimensionFields, keyValueStore, noOpSearchProvider)
        keyValueStoreDimension.setLastUpdated(new DateTime(10000))
    }

    def "findAllDimensionRows returns an empty set"() {
        expect:
        noOpSearchProvider.findAllDimensionRows().empty
    }

    def "findAllOrderedDimensionRows returns an empty set"() {
        expect:
        noOpSearchProvider.findAllOrderedDimensionRows().empty
    }

    def "findFilteredDimensionRows with simple filters returns expected row"() {

        given: "A set of ApiFilters and an expected row"
        ApiFilter apiFilter = Mock(ApiFilter)
        apiFilter.getValues() >> ["row1Desc"]
        DimensionRow expectedRow = BardDimensionField.makeDimensionRow(keyValueStoreDimension, "row1Desc", "row1Desc")

        expect: "We get the expected row when we find filtered dimension rows"
        noOpSearchProvider.findFilteredDimensionRows([apiFilter] as Set) == [expectedRow] as Set<DimensionRow>
    }

    def "findFilteredDimensionRows with complex filters returns expected row"() {

        given: "A set of ApiFilters and an expected row"
        ApiFilter apiFilter1 = Mock(ApiFilter)
        apiFilter1.getValues() >> ["row1Desc"] // say filter query is like: dim|desc-in[row1Desc]

        ApiFilter apiFilter2 = Mock(ApiFilter)
        apiFilter2.getValues() >> ["row2"] // say filter query is like: dim|id-notin[row2]

        ApiFilter apiFilter3 = Mock(ApiFilter)
        apiFilter3.getValues() >> ["row3Name"] // say filter query is like: dim|name-contains[row3Name]

        DimensionRow expectedRow1 = BardDimensionField.makeDimensionRow(keyValueStoreDimension, "row1Desc", "row1Desc")
        DimensionRow expectedRow2 = BardDimensionField.makeDimensionRow(keyValueStoreDimension, "row2", "row2")
        DimensionRow expectedRow3 = BardDimensionField.makeDimensionRow(keyValueStoreDimension, "row3Name", "row3Name")

        expect: "We get the expected rows when we find filtered dimension rows with complex filters"
        noOpSearchProvider.findFilteredDimensionRows([apiFilter1, apiFilter2, apiFilter3] as Set) == [expectedRow1, expectedRow2, expectedRow3] as Set<DimensionRow>
    }

    def "getDimensionCardinality returns cardinality count"() {
        // other is configured with NoOpSearchProvider and its cardinality is package_name__query_weight_limit
        int queryWeightLimit = systemConfig.getIntProperty(systemConfig.getPackageVariableName("query_weight_limit"), 100000);

        expect:
        noOpSearchProvider.getDimensionCardinality() == queryWeightLimit
    }

    def "isHealthy returns true"() {
        expect:
        noOpSearchProvider.isHealthy() == true
    }
}
