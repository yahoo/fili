// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService

import spock.lang.Specification

class RegisteredLookupMetadataLoadTaskSpec extends Specification {
    DruidWebService druidClient

    RegisteredLookupDimension registeredLookupDimension
    DimensionDictionary dimensionDictionary

    RegisteredLookupMetadataLoadTask lookupLoadTask

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

        registeredLookupDimension = Mock(RegisteredLookupDimension)
        registeredLookupDimension.getLookups() >> ["loadedLookup", "pendingLookup", "LookupNotInDruid"]

        lookupLoadTask = new RegisteredLookupMetadataLoadTask(druidClient, new DimensionDictionary([registeredLookupDimension] as Set))
    }

    def "LookupLoadTask, when runs, finds pending lookups"() {
        when:
        lookupLoadTask.run()

        then:
        lookupLoadTask.getPendingLookups() == ["pendingLookup", "LookupNotInDruid"] as Set
    }

    def "getTiers() separates tiers by comma"() {
        expect:
        lookupLoadTask.getTiers("__default,tier1,tier2,tier3") ==
                (["__default", "tier1", "tier2", "tier3"] as Set)
    }
}
