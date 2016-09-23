// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;

import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Tables API Request. Such an API Request binds, validates, and models the parts of a request to the tables endpoint.
 */
public class TablesApiRequest extends ApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(TablesApiRequest.class);
    public static final String REQUEST_MAPPER_NAMESPACE = "tablesApiRequestMapper";

    private final Set<LogicalTable> tables;
    private final LogicalTable table;
    private final Granularity granularity;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param uriInfo  The URI of the request object.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @throws BadApiRequestException is thrown in the following scenarios:
     * <ol>
     *     <li>Invalid table in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public TablesApiRequest(
            String tableName,
            String granularity,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        super(format, perPage, page, uriInfo);

        this.tables = generateTables(tableName, bardConfigResources.getLogicalTableDictionary());

        if (tableName != null && granularity != null) {
            this.granularity = generateGranularity(granularity, bardConfigResources.getGranularityParser());
            this.table = generateTable(tableName, this.granularity, bardConfigResources.getLogicalTableDictionary());
        } else {
            this.table = null;
            this.granularity = null;
        }

        LOG.debug(
                "Api request: Tables: {},\nGranularity: {},\nFormat: {}\nPagination: {}",
                this.tables,
                this.granularity,
                this.format,
                this.paginationParameters
        );
    }

    /**
     * No argument constructor, meant to be used only for testing.
     */
    @ForTesting
    protected TablesApiRequest() {
        super();
        this.tables = null;
        this.table = null;
        this.granularity = null;
    }

    /**
     * Extracts the list of logical table names from the url table path segment and generates a set of logical table
     * objects based on it.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param tableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException if an invalid table is requested or the logical table dictionary is empty.
     */
    protected Set<LogicalTable> generateTables(String tableName, LogicalTableDictionary tableDictionary)
            throws BadApiRequestException {
        Set<LogicalTable> generated = tableDictionary.values().stream()
                .filter(logicalTable -> tableName == null || tableName.equals(logicalTable.getName()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Check if logical tables exist with the requested logical table name
        if (generated.isEmpty()) {
            String msg;
            if (tableDictionary.isEmpty()) {
                msg = EMPTY_DICTIONARY.logFormat("Logical Table");
            } else {
                msg = TABLE_UNDEFINED.logFormat(tableName);
            }
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated set of logical tables: {}", generated);
        return generated;
    }

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param tableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException Invalid table exception if the table dictionary returns a null.
     */
    protected LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary tableDictionary
    ) throws BadApiRequestException {
        LogicalTable generated = tableDictionary.get(new TableIdentifier(tableName, granularity));

        // check if requested logical table grain pair exists
        if (generated == null) {
            String msg = TABLE_GRANULARITY_MISMATCH.logFormat(granularity, tableName);
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated logical table: {} with granularity {}", generated, granularity);
        return generated;
    }

    public Set<LogicalTable> getTables() {
        return this.tables;
    }

    public LogicalTable getTable() {
        return this.table;
    }

    public Granularity getGranularity() {
        return this.granularity;
    }
}
