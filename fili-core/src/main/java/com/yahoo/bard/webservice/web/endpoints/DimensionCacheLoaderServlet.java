// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.DimensionUpdateDate;
import com.yahoo.bard.webservice.web.PATCH;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Web service endpoint to update the Dimension rows and dimension lastUpdated field.
 */
@Path("cache")
@Singleton
public class DimensionCacheLoaderServlet {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionCacheLoaderServlet.class);

    private final DimensionDictionary dimensionDictionary;
    private final ObjectMapper mapper;
    private final DataCache<?> dataCache;

    /**
     * Constructor.
     *
     * @param dimensionDictionary  Set of dimensions to be aware of
     * @param dataCache  A cache that we can clear if told to
     * @param objectMappers  Shares mappers for dealing with JSON
     */
    @Inject
    public DimensionCacheLoaderServlet(
            DimensionDictionary dimensionDictionary,
            @NotNull DataCache<?> dataCache,
            ObjectMappersSuite objectMappers
    ) {
        this.mapper = objectMappers.getMapper();
        this.dimensionDictionary = dimensionDictionary;
        this.dataCache = dataCache;
    }

    /**
     * Endpoint to update a dimensions lastUpdated field.
     *
     * @param dimensionName  name of the dimension whose lastUpdated is to be modified. (path parameter)
     * @param json  post data json containing the lastUpdated datetime. Expected JSON string:
     * <pre><code>
     * {
     *     "name":"{@literal <dimensionName>}",
     *     "lastUpdated":"{@literal <ISO_8601_datetime_string>}"
     * }
     * </code></pre>
     *
     * @return OK(200) if successfully updated else Bad Request(400)
     */
    @POST
    @Timed
    @Path("/dimensions/{dimensionName}")
    @Consumes("application/json")
    public Response updateDimensionLastUpdated(@PathParam("dimensionName") String dimensionName, String json) {
        LOG.debug("Updating lastUpdated for dimension:{} using json: {}", dimensionName, json);
        try {
            // if dimension is not located return bad request response
            Dimension dimension = dimensionDictionary.findByApiName(dimensionName);
            if (dimension == null) {
                String message = String.format("Dimension %s cannot be found.", dimensionName);
                LOG.debug(message);
                return Response.status(NOT_FOUND).entity(message).build();
            }

            // Extract LastUpdated as string from the post data
            DimensionUpdateDate dimensionUpdateDate = mapper.readValue(
                    json,
                    new TypeReference<DimensionUpdateDate>() { /* Empty class */ }
            );

            DateTime lastUpdated = dimensionUpdateDate.getLastUpdated();
            if (lastUpdated == null) {
                String message = "lastUpdated value not in json";
                LOG.error(message);
                return Response.status(BAD_REQUEST).entity(message).build();
            }
            dimension.setLastUpdated(lastUpdated);

            LOG.debug("Successfully updated lastUpdated {} for dimension: {}", lastUpdated, dimensionName);
            return Response.status(Status.OK).build();
        } catch (IOException e) {
            String message = "Failed to process lastUpdated date";
            LOG.error(message, e);
            return Response.status(INTERNAL_SERVER_ERROR).entity(message).build();
        }
    }

    /**
     * Get the lastUpdated date for a particular dimension.
     *
     * @param dimensionName  Name of the dimension to get the last updated date for.
     *
     * @return Response Format:
     * <pre><code>
     * {
     *     "name":"{@literal <dimensionName>}",
     *     "lastUpdated":"{@literal <ISO_8601_datetime_string>}"
     * }
     * </code></pre>
     */
    @GET
    @Timed
    @Path("/dimensions/{dimensionName}")
    @Consumes("application/json")
    public Response getDimensionLastUpdated(@PathParam("dimensionName") String dimensionName) {
        // if dimension is not located return bad request response
        Dimension dimension = dimensionDictionary.findByApiName(dimensionName);
        if (dimension == null) {
            String message = String.format("Dimension %s cannot be found.", dimensionName);
            LOG.debug(message);
            return Response.status(NOT_FOUND).entity(message).build();
        }

        Map<String, String> result = new LinkedHashMap<>();
        result.put("name", dimensionName);
        result.put("lastUpdated", dimension.getLastUpdated() == null ? null : dimension.getLastUpdated().toString());

        try {
            return Response.status(Status.OK).entity(mapper.writeValueAsString(result)).build();
        } catch (IOException e) {
            String message = String.format("Error retrieving lastUpdated for %s", dimensionName);
            LOG.error(message, e);
            return Response.status(INTERNAL_SERVER_ERROR).entity(message).build();
        }
    }

    /**
     * Endpoint to add/replace dimension rows.
     * <p>
     * If a row having the same ID already exists, it will be overwritten.
     *
     * @param dimensionName  name of the dimension whose dimension rows are to be modified. (path parameter)
     * @param json  post data json containing a list of json objects which describe dimension rows
     * <pre><code>
     * {
     *     "dimensionRows": [
     *         { "id":"usa", "description":"United_States_of_America" },
     *         { "id":"can", "description":"Canada" }
     *     ]
     * }
     * </code></pre>
     *
     * @return OK(200) if successfully added/replaced else Bad Request(400)
     */
    @POST
    @Timed
    @Path("/dimensions/{dimensionName}/dimensionRows")
    @Consumes("application/json; charset=utf-8")
    public Response addReplaceDimensionRows(@PathParam("dimensionName") String dimensionName, String json) {
        LOG.debug("Replacing {} dimension rows with a json payload {} characters long", dimensionName, json.length());
        try {
            // if dimension is not located return bad request response
            Dimension dimension = dimensionDictionary.findByApiName(dimensionName);
            if (dimension == null) {
                String message = String.format("Dimension %s cannot be found.", dimensionName);
                LOG.debug(message);
                return Response.status(NOT_FOUND).entity(message).build();
            }

            // extract dimension rows from the post data
            Map<String, LinkedHashSet<LinkedHashMap<String, String>>> rawDimensionRows = mapper.readValue(
                    json,
                    new TypeReference<Map<String, LinkedHashSet<LinkedHashMap<String, String>>>>() { /* Empty class */ }
            );

            Set<DimensionRow> dimensionRows = rawDimensionRows.get("dimensionRows").stream()
                    .map(dimension::parseDimensionRow)
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            dimension.addAllDimensionRows(dimensionRows);

            LOG.debug("Successfully added/replaced {} row(s) for dimension: {}", dimensionRows.size(), dimensionName);
            return Response.status(Status.OK).build();
        } catch (IOException e) {
            String message = "Failed to add/replace dimension rows";
            LOG.error(message, e);
            return Response.status(INTERNAL_SERVER_ERROR).entity(message).build();
        }
    }

    /**
     * Endpoint to add/update dimension rows, with update semantics.
     * <p>
     * If (by ID) a given row doesn't exist, this acts like POST above.
     * If (by ID) a given row *does* exist, this will perform a merge of the new row into the old row.
     * Given the two existing rows
     * <pre><code>
     * {
     *     "dimensionRows": [
     *         { "id":"usa", "description":"States_United_of_America", "population":"320", "gdp":"" },
     *         { "id":"can", "description":"Canada", "population":"35", "gdp":"" }
     *     ]
     * }
     * </code></pre>
     * a PATCH with
     * <pre><code>
     * {
     *     "dimensionRows": [
     *         { "id":"usa", "description":"United_States_of_America", "gdp":"16.77" },
     *         { "id":"can", "description":"Canada", "gdp":"1.83" },
     *         { "id":"uk", "description":"United Kingdom", "gdp":"2.68" }
     *     ]
     * }
     * </code></pre>
     * will result in
     * <pre><code>
     * {
     *     "dimensionRows": [
     *         { "id":"usa", "description":"United_States_of_America", "population":"320", "gdp":"16.77" },
     *         { "id":"can", "description":"Canada", "population":"35", "gdp":"1.83" },
     *         { "id":"uk", "description":"United Kingdom", "population":"", "gdp":"2.68" }
     *     ]
     * }
     * </code></pre>
     * Notice that the population field is not touched, that description and gdp are both updated, and that
     * United Kingom is added (with an empty population).
     *
     * @param dimensionName  name of the dimension whose dimension rows are to be modified. (path parameter)
     * @param json  post data json containing a list of json objects which describe dimension rows
     * <pre><code>
     * {
     *     "dimensionRows": [
     *         { "id":"usa", "description":"United_States_of_America" },
     *         { "id":"can", "description":"Canada" }
     *     ]
     * }
     * </code></pre>
     *
     * @return OK(200) if successfully added/updated else Bad Request(400)
     */
    @PATCH
    @Timed
    @Path("/dimensions/{dimensionName}/dimensionRows")
    @Consumes("application/json")
    public Response addUpdateDimensionRows(@PathParam("dimensionName") String dimensionName, String json) {
        LOG.debug("Updating {} dimension rows with a json payload {} characters long", dimensionName, json.length());
        try {
            // if dimension is not located return bad request response
            Dimension dimension = dimensionDictionary.findByApiName(dimensionName);
            if (dimension == null) {
                String message = String.format("Dimension %s cannot be found.", dimensionName);
                LOG.debug(message);
                return Response.status(NOT_FOUND).entity(message).build();
            }

            // extract dimension rows form the post data
            Map<String, LinkedHashSet<LinkedHashMap<String, String>>> rawDimensionRows = mapper.readValue(
                    json,
                    new TypeReference<Map<String, LinkedHashSet<LinkedHashMap<String, String>>>>() { /* Empty class */ }
            );

            DimensionField key = dimension.getKey();
            Set<DimensionRow> dimensionRows = new LinkedHashSet<>();
            for (Map<String, String> fieldnameValueMap: rawDimensionRows.get("dimensionRows")) {
                DimensionRow newRow = dimension.parseDimensionRow(fieldnameValueMap);
                DimensionRow oldRow = dimension.findDimensionRowByKeyValue(newRow.get(key));
                if (oldRow == null) {
                    // It didn't exist before, so add it directly
                    dimensionRows.add(newRow);
                } else {
                    // The row existed before, so do an update on the existing row's data
                    for (DimensionField field : dimension.getDimensionFields()) {
                        // only overwrite if the field was in the original JSON
                        if (fieldnameValueMap.containsKey(field.getName())) {
                            oldRow.put(field, newRow.get(field));
                        }
                        dimensionRows.add(oldRow);
                    }
                }
            }
            dimension.addAllDimensionRows(dimensionRows);

            LOG.debug("Successfully added/updated {} row(s) for dimension: {}", dimensionRows.size(), dimensionName);
            return Response.status(Status.OK).build();
        } catch (IOException e) {
            String message = "Failed to add/update dimension rows";
            LOG.error(message, e);
            return Response.status(INTERNAL_SERVER_ERROR).entity(message).build();
        }
    }

    /**
     * Endpoint to update cache status.
     *
     * @param json  post data json containing the lastUpdated datetime.
     * <p>
     * Expected JSON string:
     * <pre><code>
     *     {"lastUpdated":"{@literal <ISO_8601_datetime_string>}"}
     * </code>
     * </pre>
     *
     * @return OK(200) if successfully updated else Bad Request(400)
     */
    @PUT
    @Timed
    @Path("/cacheStatus")
    @Consumes("application/json")
    public Response updateDimensionLastUpdated(String json) {

        LOG.debug("Update cacheStatus using json: {}", json);
        Map<String, String> postDataMap;
        try {
            // Extract LastUpdated as string from the post data
            postDataMap = mapper.readValue(json, new TypeReference<Map<String, String>>() { /* Empty class */ });

            if (!postDataMap.containsKey("cacheStatus")) {
                String message = String.format("Missing cacheStatus in json: %s", json);
                LOG.error(message);
                return Response.status(BAD_REQUEST).entity(message).build();
            }
            // TODO tie this to cache status management when implemented

            LOG.debug("Successfully updated cacheStatus to: {}", postDataMap.get("cacheStatus"));
            return Response.status(Status.OK).build();
        } catch (IOException e) {
            String message = "Failed to update lastUpdated. Unable to process payload";
            LOG.error(message, e);
            return Response.status(BAD_REQUEST).entity(message).build();
        }
    }

    /**
     * Get the status of the cache.
     *
     * @return the cache status message as a Response
     */
    @GET
    @Timed
    @Path("/cacheStatus")
    public Response getMsg() {
        Map<String, String> cacheStatus = new LinkedHashMap<>();
        // TODO tie this cache status when implemented
        cacheStatus.put("cacheStatus", "Active");
        try {
            return Response.status(Status.OK).entity(mapper.writeValueAsString(cacheStatus)).build();
        } catch (JsonProcessingException e) {
            String message = "Unable to serialize cache status";
            LOG.error(message, e);
            return Response.status(INTERNAL_SERVER_ERROR).entity(message).build();
        }
    }

    /**
     * clears cache.
     * <p>
     * {@code curl -X DELETE "http://localhost:4080/v1/cache/data"}
     *
     * @return OK
     */
    @DELETE
    @Timed
    @Path("/data")
    public Response deleteDataCache() {
        dataCache.clear();
        return Response.status(Status.OK).build();
    }
}
