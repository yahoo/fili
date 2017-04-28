// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Bard test lookup dimension configuration.
 */
public class TestLookupDimensions extends TestDimensions {

    /**
     * Constructor.
     */
    public TestLookupDimensions() {
        super();

        for (TestApiDimensionName apiName : TestApiDimensionName.values()) {
            DimensionConfig dimensionConfig = dimensionNameConfigs.get(apiName);
            LookupDimensionConfig lookupDimensionConfig = new TestLookupDimensionConfig(
                    dimensionConfig,
                    dimensionNamespaces(dimensionConfig.getApiName().toUpperCase(Locale.ENGLISH))
            );
            dimensionNameConfigs.put(apiName, lookupDimensionConfig);
        }
    }

    /**
     * Build the namespaces chain based on the dimension name.
     * <p>
     * SIZE will contain multiple namespaces
     * SHAPE will contain single namespace
     * other dimensions will contain empty namespace
     *
     * @param dimensionName The look up dimension for which the namespace chain is to be built
     *
     * @return list of namespaces for the look up
     */
    private List<String> dimensionNamespaces(String dimensionName) {
        switch (dimensionName) {
            case "SIZE":
                return Arrays.asList("SPECIES", "BREED", "GENDER");
            case "SHAPE":
                return Collections.singletonList("SPECIES");
            default:
                return Collections.emptyList();
        }
    }
}
