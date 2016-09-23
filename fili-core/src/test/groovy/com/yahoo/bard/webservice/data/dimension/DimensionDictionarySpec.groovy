// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import org.joda.time.DateTime
import spock.lang.Specification

class DimensionDictionarySpec extends Specification {

    def DateTime lastUpdated = new DateTime(10000)
    def DimensionDictionary dimensionDictionary
    def Set<Dimension> dimensions
    def LinkedHashSet<DimensionField> dimensionFields

    def setup() {
        dimensionFields = new LinkedHashSet<>();
        dimensionFields.add(BardDimensionField.ID);
        dimensionFields.add(BardDimensionField.DESC);

        dimensions = ["age_bracket", "gender", "user_country"].collect { String name ->
            new KeyValueStoreDimension(name, "desc-"+name, dimensionFields, MapStoreManager.getInstance(name), ScanSearchProviderManager.getInstance(name), [] as Set)
        } as Set
        dimensionDictionary = new DimensionDictionary(dimensions)
    }

    def "Get dimension by dimension name"() {
        expect:
        dimensionDictionary.findByApiName("age_bracket") == new KeyValueStoreDimension("age_bracket", "desc-age_bracket", dimensionFields, MapStoreManager.getInstance("age_bracket"), ScanSearchProviderManager.getInstance("age_bracket"), [] as Set)
    }

    def "Find all dimensions"() {
        expect:
        dimensionDictionary.findAll() == dimensions
    }

    def "Adding a new dimension returns true"() {
        expect:
        dimensionDictionary.add(new KeyValueStoreDimension("device_type", "desc-device_type", dimensionFields, MapStoreManager.getInstance("device_type"), ScanSearchProviderManager.getInstance("device_type"), [] as Set))
    }

    def "Adding an already existing dimension returns false"() {
        expect:
        !dimensionDictionary.add(new KeyValueStoreDimension("age_bracket", "desc-age_bracket", dimensionFields, MapStoreManager.getInstance("age_bracket"), ScanSearchProviderManager.getInstance("age_bracket"), [] as Set))
    }

    def "Updating an already existing dimension returns false"() {
        Dimension existing = dimensionDictionary.findByApiName("age_bracket")

        expect:
        !dimensionDictionary.add(new KeyValueStoreDimension("age_bracket", "desc-age_bracket", dimensionFields, MapStoreManager.getInstance("age_bracket"), ScanSearchProviderManager.getInstance("age_bracket"), [] as Set))

        and:
        dimensionDictionary.findByApiName("age_bracket").is(existing)
   }

    def "Add many dimensions at once"() {

        Set newDims = [
            new KeyValueStoreDimension("device_type", "desc-device_type", dimensionFields, MapStoreManager.getInstance("device_type"), ScanSearchProviderManager.getInstance("age_bracket"), [] as Set),
            new KeyValueStoreDimension("os", "desc-os", dimensionFields, MapStoreManager.getInstance("os"), ScanSearchProviderManager.getInstance("age_bracket"), [] as Set),
            ] as Set

        expect:
        dimensionDictionary.addAll(newDims)
        /* second time should be false */
        ! dimensionDictionary.addAll(newDims)

        and: "new item should be found"
        dimensionDictionary.findByApiName("device_type").description == "desc-device_type"
    }
}
