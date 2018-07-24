package com.yahoo.slurper.webservice.data.config.names;

import com.yahoo.bard.webservice.util.EnumUtils;

public enum  WikiApiDimensionName {
    PAGE,
    LANGUAGE,
    USER,
    UNPATROLLED,
    NEW_PAGE,
    ROBOT,
    ANONYMOUS,
    NAMESPACE,
    CONTINENT,
    COUNTRY,
    REGION,
    CITY;

    private final String camelName;

    /**
     * Constructor.
     */
    WikiApiDimensionName() {
        this.camelName = EnumUtils.camelCase(name());
    }

    /**
     * View the dimension name.
     *
     * @return The camelCase version of the dimension.
     */
    public String asName() {
        return camelName;
    }
}
