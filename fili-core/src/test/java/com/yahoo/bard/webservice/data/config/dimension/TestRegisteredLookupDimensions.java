// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Bard test registered dimension configuration.
 */
public class TestRegisteredLookupDimensions extends TestDimensions {

    /**
     * Constructor.
     */
    public TestRegisteredLookupDimensions() {
        super();

        for (TestApiDimensionName apiName : TestApiDimensionName.values()) {
            DimensionConfig dimensionConfig = dimensionNameConfigs.get(apiName);
            RegisteredLookupDimensionConfig lookupDimensionConfig = new TestRegisteredLookupDimensionConfig(
                    dimensionConfig,
                    dimensionLookups(dimensionConfig.getApiName().toUpperCase(Locale.ENGLISH))
            );
            dimensionNameConfigs.put(apiName, lookupDimensionConfig);
        }
    }

    /**
     * Build the lookup chain based on the dimension name.
     * <p>
     * BREED will contain multiple lookups
     * SPECIES will contain single lookup
     * other dimensions will contain empty lookup
     *
     * @param dimensionName The look up dimension for which the lookup chain is to be built
     *
     * @return list of lookups for the look up
     */
    private List<ExtractionFunction> dimensionLookups(String dimensionName) {
        switch (dimensionName) {
            case "BREED":
                return Arrays.asList(
                        new RegisteredLookupExtractionFunction("BREED__SPECIES"),
                        new RegisteredLookupExtractionFunction("BREED__OTHER"),
                        new RegisteredLookupExtractionFunction("BREED__COLOR")
                );
            case "SPECIES":
                return Collections.singletonList(new RegisteredLookupExtractionFunction("SPECIES__BREED"));
            default:
                return Collections.emptyList();
        }
    }
}
