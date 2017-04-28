// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A mock DataSourceMetadataService implementation for testing purposes only.
 */
public class TestDataSourceMetadataService extends DataSourceMetadataService {

    public Map<String, Set<Interval>> testAvailableIntervals;

    /**
     * Constructor.
     *
     * @param testData interval data to be injected into this test metadata service with key as column
     */
    public TestDataSourceMetadataService(Map<String, Set<Interval>> testData) {
        super();
        this.testAvailableIntervals = testData.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Constructor.
     */
    public TestDataSourceMetadataService() {
        this(Collections.emptyMap());
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAvailableIntervalsByDataSource(DataSourceName dataSourceName) {
        return testAvailableIntervals.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new SimplifiedIntervalList(entry.getValue())
                        )
                );
    }
}
