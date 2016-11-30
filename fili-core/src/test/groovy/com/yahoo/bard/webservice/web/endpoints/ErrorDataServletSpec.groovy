// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.config.BardFeatureFlag.TOP_N
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_NOT_IN_TABLE
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_INVALID
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_MISSING
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_ZERO_LENGTH
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_MISSING
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_DIRECTION_INVALID
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_IN_QUERY_FORMAT
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_SORTABLE_FORMAT
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_UNDEFINED
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_SCHEMA_UNDEFINED
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNKNOWN_GRANULARITY

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigException
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.web.ErrorMessageFormat

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.core.Response

@Timeout(30)    // Fail test if hangs
class ErrorDataServletSpec extends Specification {

    private static final SystemConfig systemConfig = SystemConfigProvider.getInstance()

    private static final String DRUID_URL_SETTING = systemConfig.getPackageVariableName("non_ui_druid_broker")

    @Shared boolean topNStatus
    static String saveDruidURL
    JerseyTestBinder jtb = new JerseyTestBinder(DataServlet.class)
    JsonSlurper jsonSlurper = new JsonSlurper()
    TestDruidWebService uiTestWebService
    TestDruidWebService nonUiTestWebService

    String standardGoodPathElements = "data/shapes/week/color"

    String standardGoodDruidResponse =
    """[
      {
        "version" : "v1",
        "timestamp" : "2014-09-01T00:00:00.000Z",
        "event" : {
          "color" : "Foo",
          "depth" : 10
        }
      }
]"""

    String standardGoodDruidQuery =
    """
{
    "queryType" : "groupBy",
    "dataSource" : {
        "name" : "color_shapes",
        "type" : "table"
    },
    "dimensions" : [ "color" ],
    "aggregations" : [ {
        "name" : "depth",
        "fieldName" : "depth",
        "type" : "longSum"
    } ],
    "postAggregations" : [],
    "intervals" : [ "2014-09-01T00:00:00.000Z/2014-09-08T00:00:00.000Z" ],
    "granularity": ${BaseDataServletComponentSpec.getTimeGrainString("week")},
    "context" : {
        "timeout" : 123
    }
}
"""

    def setupSpec() {
        // Stash the druid URL set in the properties so we can replace it when we're done
        try {
            saveDruidURL = systemConfig.getStringProperty(DRUID_URL_SETTING)
        } catch (SystemConfigException ignored) {
            saveDruidURL = null
        }

        // If there's no URL property set, set the property to a test URL
        if (saveDruidURL == null) {
            systemConfig.setProperty(DRUID_URL_SETTING, "http://localhost:9998/druid")
        }

        topNStatus = TOP_N.isOn();
        TOP_N.setOn(true)
    }

    def cleanupSpec() {
        if (saveDruidURL == null) {
            systemConfig.clearProperty(DRUID_URL_SETTING)
        } else {
            systemConfig.setProperty(DRUID_URL_SETTING, saveDruidURL)
        }

        TOP_N.setOn(topNStatus)
    }

    def setup() {
        // Create the test web container to test the resources
        uiTestWebService = jtb.uiDruidWebService
        nonUiTestWebService = jtb.nonUiDruidWebService
        assert jtb.nonUiDruidWebService instanceof DruidWebService
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }


    def "Valid druid request passes"() {
        setup:
        DruidServiceConfig oldConfig = nonUiTestWebService.serviceConfig
        nonUiTestWebService.serviceConfig = new DruidServiceConfig("Non-Ui Broker", oldConfig.url, 123, oldConfig.priority)
        nonUiTestWebService.jsonResponse = {standardGoodDruidResponse}
        String expectedDruidQuery = standardGoodDruidQuery

        expect:
        Response r = jtb.getHarness().target(standardGoodPathElements)
                .queryParam("metrics","depth")
                .queryParam("dateTime","2014-09-01%2F2014-09-08")
                .request().get()
        r.getStatus() == 200
        GroovyTestUtils.compareJson(jtb.nonUiDruidWebService.jsonQuery, expectedDruidQuery)

        cleanup:
        // Reset the timeout
        nonUiTestWebService.serviceConfig = oldConfig
    }

    def "Invalid time grain fails"() {
        String badTimeGrain = "fortnight"
        String message = UNKNOWN_GRANULARITY.format(badTimeGrain)

        String jsonFailure =
            """{
                    "status":400,
                    "statusName": "Bad Request",
                    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
                    "description":"${message}",
                    "druidQuery":null,
                    "requestId": "SOME UUID"
            }"""

        when:
        Response r = jtb.getHarness().target("data/shapes/${badTimeGrain}/color")
                .queryParam("metrics","length")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Missing metrics fails"() {
        String jsonFailure =
            """{
                    "status":400,
                    "statusName": "Bad Request",
                    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
                    "description":"${METRICS_MISSING.format()}",
                    "druidQuery":null,
                    "requestId": "SOME UUID"
            }"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Empty metrics fails"() {
        String jsonFailure =
            """{
                    "status":400,
                    "statusName": "Bad Request",
                    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
                    "description":"${METRICS_MISSING.format()}",
                    "druidQuery":null,
                    "requestId": "SOME UUID"
            }"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Metric not in logical table fails"() {
        String message = METRICS_NOT_IN_TABLE.format("[limbs]", "shapes")

        String jsonFailure =
            """{
                    "status":400,
                    "statusName": "Bad Request",
                    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
                    "description":"${message}",
                    "druidQuery":null,
                    "requestId": "SOME UUID"
            }"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","limbs")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Dimension not in logical table fails"() {
        String message = DIMENSIONS_NOT_IN_TABLE.format("[species]", "shapes")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/species")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad table name fails"() {
        String message = TABLE_UNDEFINED.format("badtable")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/badtable/day/color")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad dimension fails"() {
        String message = DIMENSIONS_UNDEFINED.format("[bad, worse]")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/bad/worse")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad metric fails"() {
        String message = METRICS_UNDEFINED.format("[bad, worse]")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","bad,worse")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad accept format fails"() {
        String message = ACCEPT_FORMAT_INVALID.format("bad")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("format","bad")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad filter fails"() {

        String message = FILTER_INVALID.format("bad")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("filters","bad")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)

    }

    def "Bad sort direction fails"() {
        String message = SORT_DIRECTION_INVALID.format("DOWN")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","height|DOWN")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad sort metric fails"() {
        String message = SORT_METRICS_UNDEFINED.format("[bad, worse]")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","bad|ASC,worse|DESC")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Sort metric not in query fails"() {
        String message = SORT_METRICS_NOT_IN_QUERY_FORMAT.format("[width]")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","width|ASC")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Sort metric not sortable fails"() {
        String message = SORT_METRICS_NOT_SORTABLE_FORMAT.format("[rowNum]")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height,rowNum")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","rowNum|ASC")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Bad count fails"() {
        String message = INTEGER_INVALID.format(count, "count")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("count", count)
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)

        where:
        count << [
            "bad",
            "",
            "3.",
            "-3",
            " 12",
            "13 ",
        ]
    }

    def "Bad topN fails"() {
        String message = INTEGER_INVALID.format(topN, "topN")

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("topN", topN)
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)

        where:
        topN << [
            "bad",
            "",
            "3.",
            "-3",
            " 12",
            "13 "
        ]
    }

    def "Missing interval fails"() {
        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${INTERVAL_MISSING.format()}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","depth")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Empty intervals fails"() {
        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${INTERVAL_MISSING.format()}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","color")
                .queryParam("dateTime","")
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Negative duration interval fails"() {
        String badInterval = "2014-09-30/2014-09-01"
        String message = INTERVAL_INVALID.format(
                badInterval,
                "The end instant must be greater than the start instant"
        )
        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""

        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","depth")
                .queryParam("dateTime", badInterval)
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Zero length interval fails"() {
        String interval = "2014-09-01/2014-09-01"
        String message = INTERVAL_ZERO_LENGTH.format(interval)

        String jsonFailure =
                """{"status":400,
    "statusName": "Bad Request",
    "reason":"com.yahoo.bard.webservice.web.BadApiRequestException",
    "description":"${message}",
    "druidQuery":null,
    "requestId": "SOME UUID"
    }
"""
        when:
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","depth")
                .queryParam("dateTime", interval)
                .request().get()

        then:
        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "No matching physical table due to misconfiguration fails"() {
        List dimensions = []
        List metrics = ["dayAvgLimbs"]
        TimeGrain innerMostTimeGrain = DAY
        String tableGroupName = "monthly"

        String message = TABLE_SCHEMA_UNDEFINED.format(
                tableGroupName,
                dimensions,
                metrics,
                innerMostTimeGrain.name
        )

        String jsonFailure =
        """
            {"description":"${message}",
            "druidQuery":null,
            "reason":"com.yahoo.bard.webservice.table.resolver.NoMatchFoundException",
            "status":500,"statusName":"Internal Server Error",
            "requestId": "SOME UUID"
            }
        """

        when:
        Response r = jtb.getHarness().target("data/monthly/month/")
                .queryParam("metrics","dayAvgLimbs")
                .queryParam("dateTime", "2014-06-01%2F2014-07-01")
                .request().get()

        then:
        r.getStatus() == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), jsonFailure)
    }

    def "Test forced BAD_REQUEST produces correct failure"() {
        setup:
        String jsonFailure =
                """{ "status": 400, "statusName":"Bad Request", "reason" : "Bad Request", "description" : "description"}"""

        jtb.nonUiDruidWebService.setFailure(jsonFailure)

        expect:
        // pageViews will respond with error
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","depth")
                .queryParam("dateTime","2014-09-01%2F2014-09-08")
                .request().get()

        r.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()

        Map map = jsonSlurper.parseText(r.readEntity(String.class))
        // do not check apiRequest property
        map.remove("druidQuery")
        map.remove("requestId")
        GroovyTestUtils.compareObjects(map, jsonSlurper.parseText(jsonFailure))
    }

    def "Test forced NOT_ACCEPTABLE produces correct error"() {
        setup:
        String jsonFailure =
                """{ "status": 406, "statusName": "Not Acceptable", "reason" : "Not Acceptable", "description" : ""}"""
        nonUiTestWebService.setFailure(jsonFailure)

        expect:
        // pageViews will respond with error
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","depth")
                .queryParam("dateTime","2014-09-01%2F2014-09-08")
                .request().get()

        r.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()

        Map map = jsonSlurper.parseText(r.readEntity(String.class))
        // do not check druidQuery property
        map.remove("druidQuery")
        map.remove("requestId")
        GroovyTestUtils.compareObjects(map, jsonSlurper.parseText(jsonFailure))
    }

    def "Test forced INTERNAL_SERVER_ERROR produces correct error"() {
        setup:
        // this special test causes uncaught exception in test servlet
        String jsonFailure =
                """{ "status": 500, "statusName": "Internal Server Error", "reason" : "Internal Server Error", "description" : ""}"""
        nonUiTestWebService.setFailure(jsonFailure)

        // pageViews will respond with error
        Response r = jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","depth")
                .queryParam("dateTime","2014-09-01%2F2014-09-08")
                .request().get()

        Map map = jsonSlurper.parseText(r.readEntity(String.class))

        // do not check druidQuery or description
        map.remove("druidQuery")
        map.remove("requestId")
        map.put("description","")

        expect:
        r.getStatus() == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
        GroovyTestUtils.compareObjects(map, jsonSlurper.parseText(jsonFailure))
    }

    def "Test heavy monthly query returns 507 error"() {
        setup:
        int statusCode = 507
        String reason = ErrorMessageFormat.WEIGHT_CHECK_FAILED.logFormat(30000, 20000)
        String description = ErrorMessageFormat.WEIGHT_CHECK_FAILED.format()
        String statusName = "507"
        String expectedJson = buildFailureJson( statusCode, statusName, reason, description)
        nonUiTestWebService.weightResponse = """[{"version":"v1","timestamp":"2014-09-01T00:00:00.000Z","event":{"count":30000}}]"""
        nonUiTestWebService.setFailure(statusCode, statusName, reason, description)

        // create 10 dimensionRows per dimension to get past worst case estimate
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        [
            "size",
            "shape",
            "color"
        ].each { String dimensionId ->
            dimensionStore.findByApiName(dimensionId).with {
                (1..10).each { int num ->
                    addDimensionRow(BardDimensionField.makeDimensionRow(it, dimensionId+num, dimensionId+num+"_desc"))
                }
            }
        }

        when: "Count is too high"
        Response r = jtb.getHarness().target("data/shapes/month/size/shape/color/other")
                .queryParam("metrics","users")
                .queryParam("dateTime","2014-09-01%2F2014-11-01")
                .queryParam("filters","color|id-in[color1]")
                .request().get()

        then: "Return 507"
        r.getStatus() == statusCode

        Map map = jsonSlurper.parseText(r.readEntity(String.class))
        // do not check druidQuery or description
        map.remove("druidQuery")
        map.remove("requestId")
        GroovyTestUtils.compareObjects(map, jsonSlurper.parseText(expectedJson))
    }

    def "Test heavy daily query produces 507 error"() {
        setup:
        int statusCode = 507
        String reason = ErrorMessageFormat.WEIGHT_CHECK_FAILED.logFormat(429820, 100000)
        String description = ErrorMessageFormat.WEIGHT_CHECK_FAILED.format()
        String statusName = "507"
        String expectedJson = buildFailureJson( statusCode, statusName, reason, description)
        nonUiTestWebService.weightResponse = """[{"version":"v1","timestamp":"2014-09-01T00:00:00.000Z","event":{"count":429820}}]"""
        nonUiTestWebService.setFailure(statusCode, statusName, reason, description)

        // create 10 dimensionRows per dimension to get past worst case estimate
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        [
            "size",
            "shape",
            "color"
        ].each { String dimensionId ->
            dimensionStore.findByApiName(dimensionId).with {
                (1..10).each { int num ->
                    addDimensionRow(BardDimensionField.makeDimensionRow(it, dimensionId+num, dimensionId+num+"_desc"))
                }
            }
        }

        when: "Count is too high"
        Response r = jtb.getHarness().target("data/shapes/day/color/shape/size/other")
                .queryParam("metrics","users")
                .queryParam("dateTime","2014-09-01%2F2014-09-30")
                .queryParam("filters","color|id-in[color1]")
                .request().get()

        then: "Return 507"
        r.getStatus() == statusCode

        Map map = jsonSlurper.parseText(r.readEntity(String.class))
        // do not check druidQuery or description
        map.remove("druidQuery")
        map.remove("requestId")
        GroovyTestUtils.compareObjects(map, jsonSlurper.parseText(expectedJson))
    }

    def "Test empty result returns correct 200 result"() {
        setup:
        nonUiTestWebService.jsonResponse = {"[]"}
        nonUiTestWebService.weightResponse = "[]"

        // create 10 dimensionRows per dimension to get past worst case estimate
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        [
            "size",
            "shape",
            "color"
        ].each { String dimensionId ->
            dimensionStore.findByApiName(dimensionId).with {
                (1..10).each { int num ->
                    addDimensionRow(BardDimensionField.makeDimensionRow(it, dimensionId+num, dimensionId+num+"_desc"))
                }
            }
        }

        when: "No rows returned"
        Response r = jtb.getHarness().target("data/shapes/day/size/shape/color/other")
                .queryParam("metrics","depth")
                .queryParam("dateTime","2014-09-01%2F2014-09-30")
                .request().get()

        then: "Return the empty result query"
        r.getStatus() == 200
    }

    /**
     * Failure JSON builder.
     *
     * @param status HTTP status code
     * @param reason Reason for the failure
     * @param description Description of the failure
     *
     * @return A JSON string of the format of our failure messages
     */
    private static String buildFailureJson( def status, def statusName, def reason, def description) {
        """{
            "status" : $status,
            "statusName" : "$statusName",
            "reason" : "$reason",
            "description" : "$description"
        }"""
    }
}
