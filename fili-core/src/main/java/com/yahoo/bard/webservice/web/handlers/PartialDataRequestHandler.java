// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.MISSING_INTERVALS_CONTEXT_KEY;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.metric.mappers.PartialDataResultSetMapper;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * A request handler that builds responses which filter partial data.
 * <p>
 * Partial data request handler puts a list of all missing intervals for a group by into the context and injects a
 * mapper into the response processor
 */
public class PartialDataRequestHandler implements DataRequestHandler {

    public static final String PARTIAL_DATA_HEADER = "partialData";

    protected final @NotNull DataRequestHandler next;
    protected final @NotNull PhysicalTableDictionary physicalTableDictionary;
    protected final @NotNull PartialDataHandler partialDataHandler;

    /**
     * Wrap the response processor in a partial data check.
     *
     * @param next The next request handler to invoke
     * @param physicalTableDictionary  the repository of slice data
     * @param partialDataHandler the PartialDataHandler to use
     */
    public PartialDataRequestHandler(
            DataRequestHandler next,
            PhysicalTableDictionary physicalTableDictionary,
            PartialDataHandler partialDataHandler
    ) {
        this.next = next;
        this.physicalTableDictionary = physicalTableDictionary;
        this.partialDataHandler = partialDataHandler;
    }

    @Override
    public boolean handleRequest(
        RequestContext context,
        DataApiRequest request,
        DruidAggregationQuery<?> druidQuery,
        ResponseProcessor response
    ) {
        if (!(response instanceof MappingResponseProcessor)) {
            throw new IllegalStateException("Partial data request handler requires a mapping response.");
        }
        MappingResponseProcessor mappingResponse = (MappingResponseProcessor) response;

        // Gather the tables from the query
        Set<PhysicalTable> physicalTables = druidQuery.getInnermostQuery().getDataSource().getNames().stream()
                .map(physicalTableDictionary::get)
                .collect(Collectors.toSet());

        // Gather the missing intervals
        SimplifiedIntervalList missingIntervals = partialDataHandler.findMissingTimeGrainIntervals(
                TableUtils.getColumnNames(request, druidQuery),
                physicalTables,
                new SimplifiedIntervalList(request.getIntervals()),
                request.getGranularity()
        );

        if (! missingIntervals.isEmpty()) {
            ResponseContext responseContext = response.getResponseContext();

            responseContext.put(MISSING_INTERVALS_CONTEXT_KEY.getName(), missingIntervals);

            if (BardFeatureFlag.PARTIAL_DATA.isOn()) {
                PartialDataResultSetMapper mapper = new PartialDataResultSetMapper(
                        missingIntervals,
                        () -> VolatileDataRequestHandler.getVolatileIntervalsWithDefault(responseContext)
                );
                mappingResponse.getMappers().add(0, mapper);
            }
            mappingResponse.getHeaders().add(PARTIAL_DATA_HEADER, true);
            responseContext.put(PARTIAL_DATA_HEADER, true);
        }

        return next.handleRequest(context, request, druidQuery, mappingResponse);
    }

    /**
     * Return the missing intervals from the context.
     *
     * @param context  The map containing the missing intervals if any
     *
     * @return the missing intervals from the request or an empty list
     */
    public static SimplifiedIntervalList getPartialIntervalsWithDefault(Map<String, Serializable> context) {
        return (SimplifiedIntervalList) context.computeIfAbsent(
                MISSING_INTERVALS_CONTEXT_KEY.getName(),
                (ignored) -> new SimplifiedIntervalList()
        );
    }
}
