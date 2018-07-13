// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService

import spock.lang.Specification

class RegisteredLookupMetadataLoadTaskSpec extends Specification {
    DruidWebService druidClient
    RegisteredLookupDimension dimension

    def setup() {
        druidClient = new TestDruidWebService()
        dimension = Mock(RegisteredLookupDimension)
    }

    def "When all lookups are loaded initially, load task reports no pending lookups"() {
        setup: "Druid has no pending lookups initially"
        druidClient.jsonResponse = {
            """
                {
                    "loadedLookup": {
                        "loaded": true
                    },
                    "pendingLookup": {
                        "loaded": true
                    }
                }
            """
        }

        and: "Given a load task that looks at lookups existing in Druid"
        dimension.getRegisteredLookupExtractionFns() >> [
                new RegisteredLookupExtractionFunction("loadedLookup"),
                new RegisteredLookupExtractionFunction("pendingLookup")
        ]

        RegisteredLookupMetadataLoadTask lookupLoadTask = new RegisteredLookupMetadataLoadTask(
                druidClient,
                new DimensionDictionary([dimension] as Set)
        )

        when: "Load task check lookup load status"
        lookupLoadTask.run()

        then: "All lookups are reported being 'loaded'"
        lookupLoadTask.getPendingLookups().empty
    }

    def "LookupLoadTask, when runs, finds and updates pending lookups"() {
        setup: "Druid has pending lookups initially"
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

        and: "Given a load task that looks at lookups existing in Druid"
        dimension.getRegisteredLookupExtractionFns() >> [
                new RegisteredLookupExtractionFunction("loadedLookup"),
                new RegisteredLookupExtractionFunction("pendingLookup")
        ]

        RegisteredLookupMetadataLoadTask lookupLoadTask = new RegisteredLookupMetadataLoadTask(
                druidClient,
                new DimensionDictionary([dimension] as Set)
        )

        when: "Load task check lookup load status"
        lookupLoadTask.run()

        then: "Load task finds the pending lookup"
        lookupLoadTask.getPendingLookups() == ["pendingLookup"] as Set

        when: "Druid has loaded all lookups, including the pending lookup"
        druidClient.jsonResponse = {
            """
                {
                    "loadedLookup": {
                        "loaded": true
                    },
                    "pendingLookup": {
                        "loaded": true
                    }
                }
            """
        }

        and: "Load task check lookup load status again"
        lookupLoadTask.run()

        then: "All lookups are reported being 'loaded'"
        lookupLoadTask.getPendingLookups().empty

        when: "A lookup fails to load again in Druid"
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

        and: "Load task check lookup load status again"
        lookupLoadTask.run()

        then: "Load task reports the pending lookup again"
        lookupLoadTask.getPendingLookups() == ["pendingLookup"] as Set
    }

    def "Load tasks report missing Druid lookups as pending lookup"() {
        setup: "Druid has all lookups loaded"
        druidClient.jsonResponse = {
            """
                {
                    "loadedLookup": {
                        "loaded": true
                    }
                }
            """
        }

        and: "Load task is asking for a lookup that's not configured in Druid"
        dimension.getRegisteredLookupExtractionFns() >> [
                new RegisteredLookupExtractionFunction("LookupNotInDruid")
        ]
        RegisteredLookupMetadataLoadTask lookupLoadTask = new RegisteredLookupMetadataLoadTask(
                druidClient,
                new DimensionDictionary([dimension] as Set)
        )

        when: "Load task check lookup load status"
        lookupLoadTask.run()

        then: "Load task reports the missing lookup as 'pending'"
        lookupLoadTask.getPendingLookups() == ["LookupNotInDruid"] as Set
    }

    def "getTiers() separates tiers by comma"() {
        expect:
        RegisteredLookupMetadataLoadTask.getTiers("__default,tier1,tier2,tier3") ==
                (["__default", "tier1", "tier2", "tier3"] as Set)
    }
}
