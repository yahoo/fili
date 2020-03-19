// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.GRANULARITY;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.INTERVALS;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Default generator implementation for {@link LogicalTable}. Logical table binding is dependent on {@link Granularity},
 * so ensure that the query granularity has already been bound before attempting to bind the logical table using this
 * generator.
 */
public class DefaultLogicalTableGenerator implements Generator<LogicalTable> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogicalTableGenerator.class);

    /**
     * Binds the logical table for this query.
     *
     * Throws {@link UnsatisfiedApiRequestConstraintsException} if the granularity has not yet been bound.
     * Throws {@link BadApiRequestException} if no granularity was specified in the query.
     *
     * @param builder  The builder object representing the in progress {@link DataApiRequest}. Previously constructed
     *                 resources are available through this object.
     * @param params  The request parameters sent by the client.
     * @param resources  Resources used to build the request, such as the
     *                   {@link com.yahoo.bard.webservice.data.config.ResourceDictionaries}.
     * @return the bound logical table
     */
    @Override
    public LogicalTable bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        if (!builder.isGranularityInitialized()) {
            throw new UnsatisfiedApiRequestConstraintsException(
                    INTERVALS.getResourceName(),
                    Collections.singleton(GRANULARITY.getResourceName())
            );
        }

        if (!builder.getGranularityIfInitialized().isPresent()) {
            throw new BadApiRequestException("Granularity is required for all data queries, but was not present in " +
                    "the request. Please add granularity to your query and try again.");
        }

        return generateTable(
                params.getLogicalTable().orElse(""),
                builder.getGranularityIfInitialized().get(),
                resources.getLogicalTableDictionary()
        );
    }

    @Override
    public void validate(
            final LogicalTable entity,
            final DataApiRequestBuilder builder,
            final RequestParameters params,
            final BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param logicalTableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException Invalid table exception if the table dictionary returns a null.
     */
    public static LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        LogicalTable generated = logicalTableDictionary.get(new TableIdentifier(tableName, granularity));

        // check if requested logical table grain pair exists
        if (generated == null) {
            String msg = TABLE_GRANULARITY_MISMATCH.logFormat(granularity, tableName);
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated logical table: {} with granularity {}", generated, granularity);
        return generated;
    }
}
