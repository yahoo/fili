// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.fasterxml.jackson.core.JsonGenerator;

import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import org.joda.time.Interval;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An wrapper for JsonResponseWriter and JsonApiResponseWriter. Hosts functions and variables that are frequently used
 * by the two children writers.
 */
public abstract class JsonAndJsonApiResponseWriter implements ResponseWriter {

    private final ObjectMappersSuite objectMappers;

    /**
     * Constructor for both Json and JsonApi format serializer.
     *
     * @param objectMappers  ObjectMappersSuite object needed for JsonFactory
     */
    public JsonAndJsonApiResponseWriter(ObjectMappersSuite objectMappers) {
        this.objectMappers = objectMappers;
    }

    protected ObjectMappersSuite getObjectMappers() {
        return objectMappers;
    }

    /**
     * Builds the meta object for the JSON response. The meta object is only built if there were missing intervals, or
     * the results are being paginated.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @param generator  The JsonGenerator used to build the JSON response.
     * @param missingIntervals  The set of intervals that do not contain data.
     * @param volatileIntervals  The set of intervals that have volatile data.
     * @param responseData  The ResponseData object containing pagination and paginationLinks.
     *
     * @throws IOException if the generator throws an IOException.
     */
    public void writeMetaObject(
            ApiRequest request,
            JsonGenerator generator,
            Collection<Interval> missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            ResponseData responseData
    ) throws IOException {
        Pagination pagination = responseData.getPagination();
        boolean paginating = pagination != null;

        // If partial data is being used at all, send the missing interval data
        boolean haveMissingIntervals =
                (BardFeatureFlag.PARTIAL_DATA.isOn() ||
                        BardFeatureFlag.PARTIAL_DATA_PROTECTION.isOn() ||
                        BardFeatureFlag.PARTIAL_DATA_QUERY_OPTIMIZATION.isOn()) &&
                !missingIntervals.isEmpty();
        boolean haveVolatileIntervals = volatileIntervals != null && ! volatileIntervals.isEmpty();

        if (!paginating && !haveMissingIntervals && !haveVolatileIntervals) {
            return;
        }

        generator.writeObjectFieldStart("meta");

        if (request instanceof DataApiRequest) {
            DataApiRequest dataApiRequest = (DataApiRequest) request;
            Set<LogicalMetric> logicalMetricSet = dataApiRequest.getLogicalMetrics();
            generator.writeObjectFieldStart("schema");
            for (LogicalMetric lm : logicalMetricSet) {
                LogicalMetricInfo lmi = lm.getLogicalMetricInfo();
                if (lmi != null) {
                    generator.writeObjectFieldStart(lmi.getName());

                    if (lmi.getType() != null) {
                        generator.writeStringField("type", String.format("%1$s", lmi.getType().getType()));
                        if (lmi.getType().getSubType() != null) {
                            generator.writeStringField("subtype",
                                    String.format("%1$s", lmi.getType().getSubType()));
                        }
                    }

                    if (lmi.getType().getTypeMetadata() != null && lmi.getType().getTypeMetadata().size() != 0) {
                        for (Map.Entry<String, String> entry : lmi.getType().getTypeMetadata().entrySet()) {
                            generator.writeObjectField(entry.getKey(), entry.getValue());
                        }
                    }

                    generator.writeEndObject();
                }
            }
            generator.writeEndObject();
        }

        // Add partial data info into the metadata block if needed.
        if (haveMissingIntervals) {
            generator.writeObjectField(
                    "missingIntervals",
                    responseData.buildIntervalStringList(missingIntervals)
            );
        }

        // Add volatile intervals.
        if (haveVolatileIntervals) {
            generator.writeObjectField(
                    "volatileIntervals",
                    responseData.buildIntervalStringList(volatileIntervals)
            );
        }

        // Add pagination information if paginating.
        if (paginating) {
            generator.writeObjectFieldStart("pagination");

            for (Map.Entry<String, URI> entry : responseData.getPaginationLinks().entrySet()) {
                generator.writeObjectField(entry.getKey(), entry.getValue());
            }

            generator.writeNumberField("currentPage", pagination.getPage());
            generator.writeNumberField("rowsPerPage", pagination.getPerPage());
            generator.writeNumberField("numberOfResults", pagination.getNumResults());

            generator.writeEndObject();
        }

        generator.writeEndObject();
    }
}
