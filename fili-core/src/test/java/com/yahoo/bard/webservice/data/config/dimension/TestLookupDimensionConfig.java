// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;

import java.util.List;
import java.util.Locale;

import javax.validation.constraints.NotNull;

/**
 * Hold all of the Configuration information for the look up dimensions.
 */
public class TestLookupDimensionConfig extends TestDimensionConfig implements LookupDimensionConfig {
    private List<String> namespaces;

    /**
     * Constructor.
     *
     * @param dimensionConfig Configuration properties for dimensions
     * @param namespaces List of namespaces used for Lookup
     */
    public TestLookupDimensionConfig (@NotNull DimensionConfig dimensionConfig, List<String> namespaces) {
        super(
                TestApiDimensionName.valueOf(dimensionConfig.getApiName().toUpperCase(Locale.ENGLISH)),
                dimensionConfig.getPhysicalName(),
                dimensionConfig.getKeyValueStore(),
                dimensionConfig.getSearchProvider(),
                dimensionConfig.getFields(),
                dimensionConfig.getDefaultDimensionFields()
        );
        this.setNamespaces(namespaces);
    }

    /**
     * Sets the namespace chain.
     *
     * @param namespaces The list of namespaces
     */
    private void setNamespaces(List<String> namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public List<String> getNamespaces() {
        return this.namespaces;
    }
}
