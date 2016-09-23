// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import org.joda.time.DateTime

import spock.lang.Specification

class AllDimensionsLoadedHealthCheckSpec extends Specification {

    DimensionDictionary dimensionDictionary = new DimensionDictionary()
    AllDimensionsLoadedHealthCheck check = new AllDimensionsLoadedHealthCheck(dimensionDictionary);

    Dimension dim1 = buildNewDimension("dim1", dimensionDictionary)
    Dimension dim2 = buildNewDimension("dim2", dimensionDictionary)
    Dimension dim3 = buildNewDimension("dim3", dimensionDictionary)

    def "Constructing an AllDimensionsLoadedHealthCheck with a null dictionary throws an exception"() {
        when: "We construct an AllDimensionsLoadedHealthCheck with a null DimensionDictionary"
        def ignored = new AllDimensionsLoadedHealthCheck(null)

        then: "A NullPointerException is thrown"
        thrown NullPointerException
    }

    def "The health check passes when the dimension dictionary is empty"() {
        given: "A healthcheck with an empty dimension dictionary"
        check = new AllDimensionsLoadedHealthCheck(new DimensionDictionary());

        expect: "The health check passes"
        check.check().healthy
    }

    def "The health check passes when all dimensions have been updated"() {
        given: "Dimensions with lastUpdated set"
        dim1.setLastUpdated(new DateTime())
        dim2.setLastUpdated(new DateTime())
        dim3.setLastUpdated(new DateTime())

        expect: "The health check passes"
        check.check().healthy
    }

    def "The health check fails when one dimension has not been updated"() {
        given: "A Dimension with lastUpdated not set"
        dim1.setLastUpdated(new DateTime())
        dim2.setLastUpdated(null)
        dim3.setLastUpdated(new DateTime())

        expect: "The health check fails"
        !check.check().healthy
    }

    /**
     * Build a new dimension and register it with the given dictionary.
     * <p>
     * The created dimension uses a MapStore, a ScanSearchProvider, and has ID and Description fields.
     *
     * @param dimensionName Name to set for the ApiName, DruidName, Description, StoreName, and SearchProviderName
     * @param dimensionDictionary Dictionary to register the dimension with
     *
     * @return The newly built dimension
     */
    private static Dimension buildNewDimension(String dimensionName, DimensionDictionary dimensionDictionary) {
        Dimension newDimension = new KeyValueStoreDimension(
                dimensionName,
                dimensionName,
                [BardDimensionField.ID, BardDimensionField.DESC] as LinkedHashSet,
                MapStoreManager.getInstance(dimensionName),
                ScanSearchProviderManager.getInstance(dimensionName)
        )
        dimensionDictionary.add(newDimension)

        return newDimension
    }
}
