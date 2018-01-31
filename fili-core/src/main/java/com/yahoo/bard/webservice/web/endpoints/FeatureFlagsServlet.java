// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.config.FeatureFlag;
import com.yahoo.bard.webservice.config.FeatureFlagRegistry;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.FeatureFlagRequest;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Web service endpoint to return the current status of feature flags.
 */
@Path("flags")
@Singleton
public class FeatureFlagsServlet extends EndpointServlet {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagsServlet.class);

    private final FeatureFlagRegistry flags;

    /** Constructor.
     *
     * @param flags  Feature Flags to know about
     * @param objectMappers  JSON tools
     */
    @Inject
    public FeatureFlagsServlet(FeatureFlagRegistry flags, ObjectMappersSuite objectMappers) {
        super(objectMappers);
        this.flags = flags;
    }

    /**
     * FeatureFlag-specific ApiRequest.
     */
    class FeatureFlagApiRequest extends ApiRequestImpl {
        /**
         * Constructor.
         *
         * @param format  Format of the request
         * @param perPage  How many items to show per page
         * @param page  Which page to show
         * @param uriInfo  URL information about the request
         */
        FeatureFlagApiRequest(String format, String perPage, String page, UriInfo uriInfo) {
            super(format, perPage, page, uriInfo);
        }
    }

    /**
     * Bean for representing a feature flag.
     */
    class FeatureFlagEntry {
        public final String name;
        public final Boolean value;

        /**
         * Constructor.
         *
         * @param name  Name of the feature flag
         * @param value  Value of the feature flag
         */
        FeatureFlagEntry(String name, Boolean value) {
            this.name = name;
            this.value = value;
        }
    }

    /**
     * Get the status of all feature flags.
     *
     * @param perPage the number per page to return
     * @param page the page to start from
     * @param format the format to use
     * @param uriInfo the injected UriInfo
     *
     * @return Response Format:
     * <pre><code>
     * {
     *      "feature flags": [
     *         {
     *              "name": "feature flag name",
     *              "value": {@literal <true | false>}
     *         },
     *         ...
     *     ]
     * }
     * </code></pre>
     */
    @GET
    @Timed
    public Response getFeatureFlagStatusAll(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @Context UriInfo uriInfo
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new FeatureFlagRequest("all"));

            FeatureFlagApiRequest apiRequest = new FeatureFlagApiRequest(format, perPage, page, uriInfo);

            List<FeatureFlagEntry> status = flags.getValues().stream()
                    .map(flag -> new FeatureFlagEntry(flag.getName(), flag.isOn()))
                    .collect(Collectors.toList());

            Stream<FeatureFlagEntry> result = apiRequest.getPage(status);

            Response response = formatResponse(
                    apiRequest,
                    result,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "feature flags" : "rows",
                    Arrays.asList("name", "value")
            );
            LOG.debug("Feature Flags Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            responseSender = () -> Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Get the status of a specific feature flag.
     *
     * @param flagName The feature flag
     * @param format  The format to return results in
     * @param uriInfo  The injected UriInfo
     *
     * @return Response Format:
     * <pre><code>
     * {
     *     "feature flags": [
     *          "name": "feature flag name",
     *          "value": {@literal <true | false>}
     *     ]
     * }
     * </code></pre>
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{flagName}")
    public Response getFeatureFlagStatus(
            @PathParam("flagName") String flagName,
            @QueryParam("format") String format,
            @Context UriInfo uriInfo
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new FeatureFlagRequest(flagName));

            FeatureFlagApiRequest apiRequest = new FeatureFlagApiRequest(format, "", "", uriInfo);

            FeatureFlag flag = flags.forName(flagName);
            FeatureFlagEntry status = new FeatureFlagEntry(flag.getName(), flag.isOn());

            String output = objectMappers.getMapper().writeValueAsString(status);

            LOG.debug("Feature Flags Endpoint Response: {}", output);
            responseSender = () -> Response.status(Response.Status.OK).entity(output).build();
        } catch (JsonProcessingException e) {
            String msg = ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format(e.getMessage());
            LOG.error(msg, e);
            responseSender = () -> Response.status(INTERNAL_SERVER_ERROR).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            responseSender = () -> Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }
}
