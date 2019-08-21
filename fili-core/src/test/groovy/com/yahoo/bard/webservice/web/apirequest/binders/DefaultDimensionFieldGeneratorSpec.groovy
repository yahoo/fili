// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.PathSegment

class DefaultDimensionFieldGeneratorSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict

    def setup() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        dimensionDict = new DimensionDictionary()
        KeyValueStoreDimension keyValueStoreDimension
        [ "locale", "one", "two", "three" ].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(
                    name,
                    "desc" + name,
                    dimensionFields,
                    MapStoreManager.getInstance(name),
                    ScanSearchProviderManager.getInstance(name)
            )
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }
    }

    def "check empty generateDimensions"() {

        Set<Dimension> dims = DimensionGenerator.DEFAULT_DIMENSION_GENERATOR.generateDimensions(
                new ArrayList<PathSegment>(),
                dimensionDict
        )

        expect:
        dims == [] as Set
    }

    def "check parsing generateDimensions"() {

        PathSegment one = Mock(PathSegment)
        PathSegment two = Mock(PathSegment)
        PathSegment three = Mock(PathSegment)
        Map emptyMap = new MultivaluedHashMap<>()

        one.getPath() >> "one"
        one.getMatrixParameters() >> emptyMap
        two.getPath() >> "two"
        two.getMatrixParameters() >> emptyMap
        three.getPath() >> "three"
        three.getMatrixParameters() >> emptyMap

        Set<Dimension> dims = DimensionGenerator.DEFAULT_DIMENSION_GENERATOR.generateDimensions([one, two, three], dimensionDict)

        HashSet<Dimension> expected =
                ["one", "two", "three"].collect { String name ->
                    Dimension dim = dimensionDict.findByApiName(name)
                    assert dim?.apiName == name
                    dim
                }

        expect:
        dims == expected
    }
}
