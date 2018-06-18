package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;

public interface DimensionConfigAPI extends DimensionName {

    /**
     * Set dimensions info.
     */
    void setApiName(String apiName);

    void setLongName(String longName);

    void setDescription(String description);

    void setCategory(String category);

    void setFields(WikiDimensionFieldConfigTemplate fields);

    /**
     * Get dimensions info.
     */
    String getApiName();

    String getLongName();

    String getDescription();

    String getCategory();

    WikiDimensionFieldConfigTemplate getFields();

}
