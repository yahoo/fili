// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;

public class NoopDataSourceMetadataLoadTask extends DataSourceMetadataLoadTask {
    /**
     * Datasource metadata loader fetches data from the druid coordinator and updates the datasource metadata service.
     *
     * @param physicalTableDictionary The physical tables with data sources to update
     * @param metadataService         The service that will store the metadata loaded by this loader
     * @param druidWebService         The druid webservice to query
     * @param mapper                  Object mapper to parse druid metadata
     */
    public NoopDataSourceMetadataLoadTask(
            final PhysicalTableDictionary physicalTableDictionary,
            final DataSourceMetadataService metadataService,
            final DruidWebService druidWebService,
            final ObjectMapper mapper
    ) {
        super(physicalTableDictionary, metadataService, druidWebService, mapper);
    }

    @Override
    public void run() {
        lastRunTimestamp.set(DateTime.now());
    }
}
