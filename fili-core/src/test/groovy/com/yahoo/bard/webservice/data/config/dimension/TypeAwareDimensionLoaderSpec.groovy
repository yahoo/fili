// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import spock.lang.Specification

/**
 * Testing the TypeAwareDimensionLoader
 */
class TypeAwareDimensionLoaderSpec extends Specification {

    LinkedHashSet<DimensionConfig> lookupDimensionConfigurations
    LinkedHashSet<DimensionConfig> dimensionConfigurations
    DimensionDictionary dimensionDictionary

    def setup() {
        lookupDimensionConfigurations = new TestLookupDimensions().getAllDimensionConfigurations()
        dimensionConfigurations = new TestDimensions().getAllDimensionConfigurations()
        dimensionDictionary = new DimensionDictionary()
    }

    def "Test dimension loader for Lookup dimension"() {
        given: "A Type Aware Dimension Loader with a list of dimension configurations"
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(lookupDimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        dimensionDictionary.findByApiName("size").getClass() == LookupDimension.class
    }

    def "Test dimension loader for KeyValueStore dimension"() {
        given: "A Type Aware Dimension Loader with a list of dimension configurations"
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(dimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        dimensionDictionary.findByApiName("breed").getClass() == KeyValueStoreDimension.class
    }

    def "Test dimension loader for a dimension type that is not defined"() {
        given: "A Type Aware Dimension loader with a list of dimension configurations"
        DimensionConfig dimensionConfiguration = Mock(DimensionConfig)
        dimensionConfiguration.getApiName() >> "foo"
        dimensionConfiguration.getType() >> String.class
        lookupDimensionConfigurations.add(dimensionConfiguration)
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(lookupDimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        dimensionDictionary.findByApiName("foo") == null
    }
}
