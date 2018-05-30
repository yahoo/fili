// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl;

import com.yahoo.bard.webservice.data.dimension.rfc_dimension.ApiDimensionSchema;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.DimensionDescriptor;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.IndexedDomain;

public class SimpleDimension implements Dimension {

    String apiName;

    DimensionDescriptor dimensionDescriptor;

    ApiDimensionSchema schema;

    IndexedDomain domain;

    public SimpleDimension(
            String apiName,
            IndexedDomain domain,
            ApiDimensionSchema schema
    ) {
        this.apiName = apiName;
        this.domain = domain;
        this.schema = schema;
        this.dimensionDescriptor = new DefaultDimensionDescriptor(apiName);
    }

    public SimpleDimension(
            String apiName,
            IndexedDomain domain,
            ApiDimensionSchema schema,
            DimensionDescriptor dimensionDescriptor
    ) {
        this.apiName = apiName;
        this.domain = domain;
        this.schema = schema;
        this.dimensionDescriptor = dimensionDescriptor;
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public ApiDimensionSchema getSchema() {
        return schema;
    }

    @Override
    public DimensionDescriptor getDimensionDescriptor() {
        return dimensionDescriptor;
    }

    @Override
    public IndexedDomain getDomain() {
        return domain;
    }

    @Override
    public boolean isAggregatable() {
        return true;
    }

    public static class DefaultDimensionDescriptor implements DimensionDescriptor {

        String longName;
        String description;
        String category;

        public DefaultDimensionDescriptor(String name) {
            this(name, name, DimensionDescriptor.DEFAULT_CATEGORY);
        }

        public DefaultDimensionDescriptor(String longName, final String description, final String category) {
            this.description = description;
            this.category = category;
            this.longName = longName;
        }

        @Override
        public String getLongName() {
            return longName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public String getCategory() {
            return category;
        }
    }
}
