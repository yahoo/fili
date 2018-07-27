// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.slurper.webservice.data.config.JsonObject;

import javax.validation.constraints.NotNull;
import java.util.LinkedHashSet;

/**
 * Dimension object for parsing to json.
 */
public class DimensionObject extends JsonObject {

    private final String apiName;
    private final String physicalName;
    private final String description;
    private final String longName;
    private final String category;
    private final LinkedHashSet<DimensionField> fields;

    /**
     * Construct a dimension instance from dimension name and
     * only using default dimension fields.
     *
     * @param apiName  The API Name is the external, end-user-facing name for the dimension.
     * @param physicalName  The internal, physical name for the dimension.
     * @param description  A description of the dimension and its meaning.
     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
     * @param category  The Category is the external, end-user-facing category for the dimension.
     * @param fields  The set of fields for this dimension, this set of field will also be used for the default fields.
     */
    public DimensionObject(
            @NotNull String apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields
    )  {
        this.apiName = apiName;
        this.physicalName = physicalName;
        this.description = description;
        this.longName = longName;
        this.category = category;
        this.fields = fields;
    }

    /**
     * The API Name is the external, end-user-facing name for the dimension. This is the name of this dimension in API
     * requests.
     *
     * @return User facing name for this dimension
     */
    public String getApiName() {
        return apiName;
    }

    /**
     * The Long Name is the external, end-user-facing long  name for the dimension.
     *
     * @return User facing long name for this dimension
     */
    public String getLongName() {
        return longName;
    }

    /**
     * The Category is the external, end-user-facing category for the dimension.
     *
     * @return User facing category for this dimension
     */
    public String getCategory() {
        return category;
    }

    /**
     * The internal, physical name for the dimension. This field (if set) is used as the only physical name.
     *
     * @return The name of the druid dimension
     */
    public String getPhysicalName() {
        return physicalName;
    }

    /**
     * The description for this dimension.
     *
     * @return A description of the dimension and its meaning
     */
    public String getDescription() {
        return description;
    }

    /**
     * The set of fields for this dimension.
     *
     * @return The set of all dimension fields for this dimension
     */
    public LinkedHashSet<DimensionField> getFields() {
        return fields;
    }
}
