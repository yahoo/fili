// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;

import lombok.Builder;

import java.util.Map;
import java.util.Set;

/**
 * Parameter value object.
 */
@Builder
public class LuthierPhysicalTableParams {
    public TableName tableName;
    public ZonedTimeGrain timeGrain;
    public Set<Column> columns;
    public Map<String, String> logicalToPhysicalColumnNames;
    public DataSourceMetadataService metadataService;
}
