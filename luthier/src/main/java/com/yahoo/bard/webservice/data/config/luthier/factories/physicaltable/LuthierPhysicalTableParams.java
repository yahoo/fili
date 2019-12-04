// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.physicaltable;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;

import java.util.Map;
import java.util.Set;

/**
 * Parameter value object.
 */
@SuppressWarnings({"checkstyle:javadocmethod"})
public class LuthierPhysicalTableParams {
    public TableName tableName;
    public ZonedTimeGrain timeGrain;
    public Set<Column> columns;
    public Map<String, String> logicalToPhysicalColumnNames;
    public DataSourceMetadataService metadataService;

    public LuthierPhysicalTableParams(
            TableName tableName,
            ZonedTimeGrain timeGrain,
            Set<Column> columns,
            Map<String, String> logicalToPhysicalColumnNames,
            DataSourceMetadataService metadataService
    ) {
        this.tableName = tableName;
        this.timeGrain = timeGrain;
        this.columns = columns;
        this.logicalToPhysicalColumnNames = logicalToPhysicalColumnNames;
        this.metadataService = metadataService;
    }

    public static LuthierPhysicalTableParamsBuilder builder() {
        return new LuthierPhysicalTableParamsBuilder();
    }

    /**
     * Builder class.
     */
    public static class LuthierPhysicalTableParamsBuilder {

        public TableName tableName;
        public ZonedTimeGrain timeGrain;
        public Set<Column> columnSet;
        public Map<String, String> columnNameMappings;
        public DataSourceMetadataService dataSourceMetadataService;

        public LuthierPhysicalTableParamsBuilder tableName(TableName tableName) {
            this.tableName = tableName;
            return this;
        }

        public LuthierPhysicalTableParamsBuilder timeGrain(ZonedTimeGrain zonedTimeGrain) {
            this.timeGrain = zonedTimeGrain;
            return this;
        }

        public LuthierPhysicalTableParamsBuilder columns(Set<Column> columnSet) {
            this.columnSet = columnSet;
            return this;
        }

        public LuthierPhysicalTableParamsBuilder logicalToPhysicalColumnNames(
                Map<String, String> columnNameMappings
        ) {
            this.columnNameMappings = columnNameMappings;
            return this;
        }

        public LuthierPhysicalTableParamsBuilder metadataService(DataSourceMetadataService dataSourceMetadataService) {
            this.dataSourceMetadataService = dataSourceMetadataService;
            return this;
        }

        public LuthierPhysicalTableParams build() {
            return new LuthierPhysicalTableParams(
                    tableName,
                    timeGrain,
                    columnSet,
                    columnNameMappings,
                    dataSourceMetadataService
            );
        }
    }
}
