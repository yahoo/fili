// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import com.yahoo.bard.webservice.util.JsonSlurper;

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Mocks Druid responses for testing.
 */
@Path("druid")
@Singleton
@Consumes("application/json")
public class TestDruidServlet {

    static String jsonResponse = "[]";
    static Response.Status statusCode = Status.OK;
    static public String jsonQuery = "";
    static public String weightResponse = "";

    /**
     * Allows test to set the Druid response.
     *
     * @param json  the response body in JSON
     */
    public static void setResponse(String json) {
        setResponse(json, 200);
    }

    /**
     * Allows test to set the Druid response.
     *
     * @param json  the response body in JSON
     * @param code  the response status code as integer
     */
    public static void setResponse(String json, int code) {
        // validate JSON string first
        new JsonSlurper().parseText(json);

        jsonResponse = json;
        statusCode = Status.fromStatusCode(code);
    }

    /**
     * Allows test to set the Druid failure response.
     *
     * @param json  inbound test
     */
    public static void setFailure(String json) {
        @SuppressWarnings("unchecked")
        Map<String, String> failure = (Map<String, String>) new JsonSlurper().parseText(json);

        //extract only status code and description from expected response string
        statusCode = Status.valueOf(failure.get("status"));
        jsonResponse = failure.get("description");
    }

    /**
     * Entry point for mocked Druid queries.
     *
     * @param query  incoming query
     *
     * @return statusCode and response set by either of the /test methods above
     */
    @POST
    @Consumes("application/json")
    public Response post(String query) {
        jsonQuery = query;

        // Respond to WeightEvaluationQuery
        if (query.contains("\"ignored\"")) {
            return Response.status(Status.OK).entity(weightResponse).build();
        }

        // validate query JSON
        new JsonSlurper().parseText(query);

        if (statusCode == Status.INTERNAL_SERVER_ERROR) {
            throw new RuntimeException();
        }

        return Response.status(statusCode).entity(jsonResponse).build();
    }
}
