// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy

import spock.lang.Specification

/**
 * Testing the TypeAwareDimensionLoader
 */
class TypeAwareDimensionLoadTaskSpec extends Specification {

    LinkedHashSet<DimensionConfig> dimensionConfigurations
    DimensionDictionary dimensionDictionary

    def setup() {
        dimensionConfigurations = new TestDimensions().getAllDimensionConfigurations()
        dimensionDictionary = new DimensionDictionary()
    }

    def "Test dimension loader for Lookup dimension"() {
        given: "A Type Aware Dimension LoadTask with a list of dimension configurations"
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(dimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        dimensionDictionary.findByApiName("breed").getClass() == LookupDimension.class
    }

    def "Test dimension loader for KeyValueStore dimension"() {
        given: "A Type Aware Dimension LoadTask with a list of dimension configurations"
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(dimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        dimensionDictionary.findByApiName("color").getClass() == KeyValueStoreDimension.class

    }

    def "Test dimension loader for dimension with storageStrategy none"() {
        given: "A Type Aware Dimension LoadTask with a list of dimension configurations"
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(dimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        dimensionDictionary.findByApiName("color").getStorageStrategy() == StorageStrategy.NONE
        dimensionConfigurations.find({it.apiName == 'color'}).getStorageStrategy() == StorageStrategy.NONE
    }

    def "Test dimension loader for a dimension type that is not defined"() {
        given: "A Type Aware Dimension loader with a list of dimension configurations"
        DimensionConfig dimensionConfiguration = Mock(DimensionConfig)
        dimensionConfiguration.getApiName() >> "foo"
        dimensionConfiguration.getType() >> String.class
        dimensionConfigurations.add(dimensionConfiguration)
        TypeAwareDimensionLoader typeAwareDimensionLoader = new TypeAwareDimensionLoader(dimensionConfigurations)

        when:
        typeAwareDimensionLoader.loadDimensionDictionary(dimensionDictionary)

        then:
        RuntimeException runtimeException = thrown(RuntimeException)
        runtimeException.message == "The dimension type 'class java.lang.String' for dimension 'foo' is invalid"
    }
}
