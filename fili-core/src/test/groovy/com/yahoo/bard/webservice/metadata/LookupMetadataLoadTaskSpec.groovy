// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService

import spock.lang.Specification

class LookupMetadataLoadTaskSpec extends Specification {
    DruidWebService druidClient

    LookupDimension lookupDimension
    DimensionDictionary dimensionDictionary

    LookupMetadataLoadTask lookupLoadTask

    def setup() {
        druidClient = new TestDruidWebService()
        druidClient.jsonResponse = {
            """
                {
                    "loadedLookup": {
                        "loaded": true
                    },
                    "pendingLookup": {
                        "loaded": false
                    }
                }
            """
        }

        lookupDimension = Mock(LookupDimension)
        lookupDimension.getNamespaces() >> ["loadedLookup", "pendingLookup", "LookupNotInDruid"]

        lookupLoadTask = new LookupMetadataLoadTask(druidClient, new DimensionDictionary([lookupDimension] as Set))
    }

    def "LookupLoadTask, when runs, finds pending lookups"() {
        when:
        lookupLoadTask.run()

        then:
        lookupLoadTask.getPendingLookups() == ["pendingLookup", "LookupNotInDruid"] as Set
    }
}
