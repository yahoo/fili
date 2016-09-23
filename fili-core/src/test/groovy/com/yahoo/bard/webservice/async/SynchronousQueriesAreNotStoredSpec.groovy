package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.util.JsonSlurper

/**
 * Verifies that synchronous queries are not stored in the JobStore.
 */
class SynchronousQueriesAreNotStoredSpec extends AsyncFunctionalSpec {
    @Override
    Map<String, Closure<String>> getResultsToTargetFunctions() {
        return [
                data: {"data/shapes/day"},
                jobs: {"jobs"}
        ]
    }

    @Override
    Map<String, Closure<Void>> getResultAssertions() {
        return [
                data: {assert it.status == 200},
                jobs: {
                    assert !new JsonSlurper().parseText(it.readEntity(String)).jobs
                }
        ]
    }

    @Override
    Map<String, Closure<Map<String, List<String>>>> getQueryParameters() {
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
