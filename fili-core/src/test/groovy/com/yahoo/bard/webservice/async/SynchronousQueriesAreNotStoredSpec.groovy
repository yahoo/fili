// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.util.JsonSlurper
/**
 * Verifies that synchronous queries are not stored in the JobStore.
 */
class SynchronousQueriesAreNotStoredSpec extends AsyncFunctionalSpec {
    @Override
    LinkedHashMap<String, Closure<String>> getResultsToTargetFunctions() {
        return [
                data: {"data/shapes/day"},
                jobs: {"jobs"}
        ]
    }

    @Override
    LinkedHashMap<String, Closure<Void>> getResultAssertions() {
        return [
                data: {assert it.status == 200},
                jobs: {
                    assert !new JsonSlurper().parseText(it.readEntity(String)).jobs
                }
        ]
    }

    @Override
    LinkedHashMap<String, Closure<Map<String, List<String>>>> getQueryParameters() {
        return [
                data: {[
                        metrics: ["height"],
                        asyncAfter: ["never"],
                        dateTime: ["2016-08-30/2016-08-31"]
                ]},
                jobs: {[
                        //'greg' is the UserId hard-coded in TestBinderFactory::buildJobRowBuilder and used for
                        //all queries created in the test environment.
                        filters: ["userId-eq[greg]"]
                ]}
        ]
    }

    @Override
    Closure<String> getFakeDruidResponse() {
        return {"[]"}
    }
}
