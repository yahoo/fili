// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.util

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE

import com.yahoo.bard.webservice.data.config.dimension.TestDimensions
import com.yahoo.bard.webservice.data.config.dimension.TestLookupDimensions
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.LookupExtractionFunction

import spock.lang.Specification

/**
 * Testing get extraction function returns the correct extraction function based on the given dimension type
 */
class GetExtractionFunctionSpec extends Specification {

    def "Should return no extraction function for KeyValueStoreDimension"() {
        setup:
        def dimConfig = new TestDimensions().getDimensionConfigurationsByApiName(SIZE).first()
        def dim = new KeyValueStoreDimension(dimConfig)

        expect:
        !ModelUtil.getExtractionFunction(dim).isPresent()
    }

    def "Should return no extraction function for LookupDimension with no namespace"() {
        setup:
        def dimConfig = new TestLookupDimensions().getDimensionConfigurationsByApiName(COLOR).first()
        def dim = new LookupDimension(dimConfig)

        expect:
        !ModelUtil.getExtractionFunction(dim).isPresent()
    }

    def "Should return lookup extraction function for LookupDimension with single namespace"() {
        setup:
        def dimConfig = new TestLookupDimensions().getDimensionConfigurationsByApiName(SHAPE).first()
        def dim = new LookupDimension(dimConfig)

        expect:
        ModelUtil.getExtractionFunction(dim).get() instanceof LookupExtractionFunction
    }

    def "Should return cascade extraction function for LookupDimension with multiple namespaces"() {
        setup:
        def dimConfig = new TestLookupDimensions().getDimensionConfigurationsByApiName(SIZE).first()
        def dim = new LookupDimension(dimConfig)

        expect:
        ModelUtil.getExtractionFunction(dim).get() instanceof CascadeExtractionFunction
    }
}
