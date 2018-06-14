package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;

import java.util.LinkedHashSet;

public interface DimensionConfigAPI extends DimensionName {

    /**
     * Set dimensions info.
     */
    void setApiName(String apiName);
    void setLongName(String longName);
    void setDescription(String description);
    void setCategory(String category);
    void setFields(WikiDimensionField fields);

    /**
     * Get dimensions info.
     */
    String getApiName();
    String getLongName();
    String getDescription();
    String getCategory();
    WikiDimensionField getFields();

}
