// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.provider.descriptor.LogicalTableDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.PhysicalTableDescriptor;
import com.yahoo.bard.webservice.data.config.provider.impl.FieldNameImpl;
import com.yahoo.bard.webservice.data.config.provider.impl.TableNameImpl;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.table.TableGroup;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A TableLoader from configuration.
 */
public class ConfiguredTableLoader extends BaseTableLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfiguredTableLoader.class);

    protected final List<LogicalTableDescriptor> logicalTables;
    protected final Set<DimensionConfig> dimensions;
    protected final Map<String, PhysicalTableDescriptor> physicalTableConfig = new HashMap<>();
    protected final Map<String, ApiMetricName> metricNames;

    /**
     * Construct a table loader from configuration.
     *
     * @param logicalTables  the logical tables
     * @param physicalTables  the physical tables
     * @param dimensions  the dimensions
     * @param metricNames  dictionary of API metric names
     */
    public ConfiguredTableLoader(
            List<LogicalTableDescriptor> logicalTables,
            List<PhysicalTableDescriptor> physicalTables,
            Set<DimensionConfig> dimensions,
            Map<String, ApiMetricName> metricNames
    ) {
        this.logicalTables = logicalTables;
        for (PhysicalTableDescriptor conf : physicalTables) {
            physicalTableConfig.put(conf.getName(), conf);
        }
        this.dimensions = dimensions;
        this.metricNames = metricNames;
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        for (LogicalTableDescriptor logicalTable : logicalTables) {

            // Compute the set of all physical fields on this logical table's physical tables
            Set<FieldName> physicalFields = logicalTable.getPhysicalTables().stream()
                    .map(physicalTable -> physicalTableConfig.get(physicalTable).getMetrics())
                    .flatMap(Collection::stream)
                    .map(FieldNameImpl::new)
                    .collect(Collectors.toSet());

            // Build the actual physical tables for this logical table
            Set<PhysicalTableDefinition> physicalTableDefs = logicalTable.getPhysicalTables().stream()
                    .map(tableName -> buildPhysicalTable(physicalTableConfig.get(tableName)))
                    .collect(Collectors.toSet());

            // Build the set of api metric names
            Set<ApiMetricName> apiMetricNames = logicalTable.getMetrics().stream()
                    .map(metricNames::get)
                    .collect(Collectors.toSet());

            if (apiMetricNames.size() < logicalTable.getMetrics().size()) {
                LOG.warn(
                        "Logical table missing metrics! requested metrics: {}; defined metrics {}",
                        logicalTable.getMetrics(),
                        apiMetricNames
                );
            }

            TableGroup tableGroup = buildTableGroup(
                    logicalTable.getName(),
                    apiMetricNames,
                    physicalFields,
                    physicalTableDefs,
                    dictionaries
            );

            loadLogicalTableWithGranularities(
                    logicalTable.getName(),
                    tableGroup,
                    new HashSet<>(logicalTable.getTimeGrains()),
                    dictionaries
            );
        }
    }

    /**
     * Build a physical table from its configuration.
     *
     * @param physicalTableDescriptor  The physical table configuration
     *
     * @return a PhysicalTableDefinition
     */
    public PhysicalTableDefinition buildPhysicalTable(PhysicalTableDescriptor physicalTableDescriptor) {

        Set<DimensionConfig> dimSet = dimensions.stream()
                .filter(dim -> physicalTableDescriptor.getDimensions().contains(dim.getApiName()))
                .collect(Collectors.toSet());

        // Warn if we didn't find a DimensionConfig for any dimensions configured in physical table defn
        if (dimSet.size() < physicalTableDescriptor.getDimensions().size()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Unable to build physical table [");
            builder.append(physicalTableDescriptor.getName());
            builder.append("]. Dimensions missing from dimension configuration: (");

            Set<String> missingDimensions = new HashSet<>(physicalTableDescriptor.getDimensions());
            missingDimensions.removeAll(
                    dimensions.stream()
                            .map(DimensionConfig::getApiName)
                            .collect(Collectors.toSet())
            );

            for (String dimName : missingDimensions) {
                builder.append(dimName);
                builder.append(",");
            }

            // delete extra comma
            builder.deleteCharAt(builder.length() - 1);
            builder.append(").");
            LOG.warn(builder.toString());
        }

        // FIXME: Needs proper configuration.
        ZonedTimeGrain grain = new ZonedTimeGrain(
                (ZonelessTimeGrain) physicalTableDescriptor.getGranularity(),
                DateTimeZone.UTC
        );
        return new PhysicalTableDefinition(new TableNameImpl(physicalTableDescriptor.getName()), grain, dimSet);
    }
}
