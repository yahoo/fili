package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogicalTableGenerator implements Generator<LogicalTable> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogicalTableGenerator.class);

    @Override
    public LogicalTable bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        if (builder.getGranularity() == null) {
            // TODO standardize this error message inside the exception and have exception only consume the dependency names
            throw new UnsatisfiedApiRequestConstraintsException("Intervals depends on Granularity, but granularity " +
                    "has not been generated yet. Ensure the granularity generation stage always runs before the " +
                    "interval generation stage");
        }
        if (!builder.getGranularity().isPresent()) {
            throw new BadApiRequestException("Granularity is required for all data queries, but was not present in" +
                    "the request. Please add granularity to your query and try again.");
        }

        return generateTable(
                params.getLogicalTable().orElse(""),
                builder.getGranularity().get(),
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
