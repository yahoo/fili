// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SLICE_UNDEFINED;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.metadata.SegmentInfo;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ForTesting;
import com.yahoo.bard.webservice.web.SlicesApiRequest;
import com.yahoo.bard.webservice.web.endpoints.DimensionsServlet;
import com.yahoo.bard.webservice.web.endpoints.SlicesServlet;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Slices API Request Implementation binds, validates, and models the parts of a request to the Slices endpoint.
 */
public class SlicesApiRequestImpl extends ApiRequestImpl implements SlicesApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(SlicesApiRequestImpl.class);

    private final Set<Map<String, String>> slices;
    private final Map<String, Object> slice;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param sliceName  string corresponding to the slice name specified in the URL
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param tableDictionary  cache containing all the valid physical table objects.
     * @param dataSourceMetadataService  a resource holding the available datasource metadata
     * @param uriInfo  The URI of the request object.
     *
     * @throws BadApiRequestException is thrown in the following scenarios:
     * <ol>
     *     <li>Invalid slice in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public SlicesApiRequestImpl(
            String sliceName,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            PhysicalTableDictionary tableDictionary,
            DataSourceMetadataService dataSourceMetadataService,
            UriInfo uriInfo
    ) throws BadApiRequestException {
        super(format, perPage, page, uriInfo);
        this.slices = generateSlices(tableDictionary, uriInfo);

        this.slice = sliceName != null ? generateSlice(
                sliceName,
                tableDictionary,
                dataSourceMetadataService,
                uriInfo
        ) : null;

        LOG.debug(
                "Api request: \nSlices: {},\nFormat: {}\nPagination: {}",
                this.slices,
                this.format,
                this.paginationParameters
        );
    }

    /**
     * No argument constructor, meant to be used only for testing.
     *
     * @deprecated it's not a good practice to have testing code here. This constructor will be removed entirely.
     */
    @Deprecated
    @ForTesting
    protected SlicesApiRequestImpl() {
        super();
        this.slices = null;
        this.slice = null;
    }

    /**
     * Generates the set of all available slices.
     *
     * @param tableDictionary  Physical table dictionary contains the map of valid table names to table objects.
     * @param uriInfo  The URI of the request object.
     *
     * @return Set of slice objects.
     * @throws BadApiRequestException if the physical table dictionary is empty.
     */
    protected Set<Map<String, String>> generateSlices(PhysicalTableDictionary tableDictionary, UriInfo uriInfo)
            throws BadApiRequestException {
        if (tableDictionary.isEmpty()) {
            String msg = EMPTY_DICTIONARY.logFormat("Slices cannot be found. Physical Table");
            throw new BadApiRequestException(msg);
        }

        Set<Map<String, String>> generated = tableDictionary.entrySet().stream()
                .map(
                        e -> {
                            Map<String, String> res = new LinkedHashMap<>();
                            res.put("name", e.getKey());
                            res.put(
                                    "timeGrain",
                                    e.getValue().getSchema().getTimeGrain().getName().toLowerCase(Locale.ENGLISH)
                            );
                            res.put("uri", SlicesServlet.getSliceDetailUrl(e.getKey(), uriInfo));
                            return res;
                        }
                ).collect(Collectors.toCollection(LinkedHashSet::new));

        LOG.trace("Generated set of slices: {}", generated);

        return generated;
    }

    /**
     * Generates a slice object for a given slice name.
     *
     * @param sliceName  string corresponding to the slice name specified in the URL
     * @param tableDictionary  Physical table dictionary contains the map of valid table names to table objects.
     * @param dataSourceMetadataService  a resource holding the available datasource metadata
     * @param uriInfo  The URI of the request object.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException if an invalid slice is requested or the physical table dictionary is empty.
     */
    protected Map<String, Object> generateSlice(
            String sliceName,
            PhysicalTableDictionary tableDictionary,
            DataSourceMetadataService dataSourceMetadataService,
            UriInfo uriInfo
    ) throws BadApiRequestException {
        if (tableDictionary.isEmpty()) {
            String msg = EMPTY_DICTIONARY.logFormat("Slices cannot be found. Physical Table");
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        PhysicalTable table = tableDictionary.get(sliceName);

        if (table == null) {
            String msg = SLICE_UNDEFINED.logFormat(sliceName);
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        Map<Column, SimplifiedIntervalList> columnCache = table.getAllAvailableIntervals();
        Set<Map<String, Object>> dimensionsResult = new LinkedHashSet<>();
        Set<Map<String, Object>> metricsResult = new LinkedHashSet<>();

        columnCache.entrySet().forEach(
                e -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("intervals", e.getValue());

                    Column key = e.getKey();
                    if (key instanceof DimensionColumn) {
                        Dimension dimension = ((DimensionColumn) key).getDimension();
                        String dimensionApiName = dimension.getApiName();
                        row.put("name", dimensionApiName);
                        row.put("factName", table.getPhysicalColumnName(dimensionApiName));
                        row.put("uri", DimensionsServlet.getDimensionUrl(dimension, uriInfo));
                        dimensionsResult.add(row);
                    } else {
                        row.put("name", key.getName());
                        metricsResult.add(row);
                    }
                }
        );

        Set<SortedMap<DateTime, Map<String, SegmentInfo>>> sliceMetadata = dataSourceMetadataService.getSegments(
                table.getDataSourceNames()
        );

        Map<String, Object> generated = new LinkedHashMap<>();
        generated.put("name", sliceName);
        generated.put("timeGrain", table.getSchema().getTimeGrain().getName());
        generated.put("timeZone", table.getSchema().getTimeGrain().getTimeZoneName());
        generated.put("dimensions", dimensionsResult);
        generated.put("metrics", metricsResult);
        generated.put("segmentInfo", generateSegmentMetadataView(sliceMetadata));

        LOG.trace("Generated slice: {}", generated);

        return generated;
    }

    /**
     * Create a simplifying view of segment info for a particular slice.
     *
     * @param sliceMetadata  The raw set of maps of datetime to maps of segment names to segment details
     *
     * @return A simplified Map of dateTime to set of segment identifiers
     */
    private static Map<DateTime, Set<String>> generateSegmentMetadataView(
            Set<SortedMap<DateTime, Map<String, SegmentInfo>>> sliceMetadata
    ) {
        return sliceMetadata.stream().flatMap(it -> it.entrySet().stream()).collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().keySet()
        ));
    }

    @Override
    public Set<Map<String, String>> getSlices() {
        return this.slices;
    }

    @Override
    public Map<String, Object> getSlice() {
        return this.slice;
    }
}
