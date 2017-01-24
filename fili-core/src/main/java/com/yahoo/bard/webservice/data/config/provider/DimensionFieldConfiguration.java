package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.Objects;

/**
 * Everything you need to know about a Dimension Field.
 *
 * Combined with DimensionConfiguration, can be used to construct an actual
 * DimensionConfig object.
 *
 * Happens to implement DimensionField for convenience.
 */
public class DimensionFieldConfiguration implements DimensionField {
    protected final String name;
    protected final String description;


    // Include by default in query results (overrideable in dimension)
    protected final boolean queryIncludedByDefault;

    // Include by default in dimension configurations (overrideable in dimension)
    protected final boolean dimensionIncludedByDefault;

    /**
     * Construct a new dimension field configuration object.
     *
     * @param name  the field name
     * @param description  The field description
     * @param queryIncludedByDefault  Whether this field is included by default in query results
     * @param dimensionIncludedByDefault  Whether this field is included by default on all dimensions
     */
    public DimensionFieldConfiguration(String name, String description, boolean queryIncludedByDefault, boolean dimensionIncludedByDefault) {
        this.name = name;
        this.description = description;
        this.queryIncludedByDefault = queryIncludedByDefault;
        this.dimensionIncludedByDefault = dimensionIncludedByDefault;
    }

    /**
     * Get the field name.
     *
     * @return  The name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the field description.
     *
     * @return  The description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Return true if the metric should be included in query results by default.
     *
     * @return  True if included by default
     */
    public boolean isQueryIncludedByDefault() {
        return queryIncludedByDefault;
    }

    /**
     * Return true if the metric should be included in all dimensions by default.
     *
     * @return  True if included by default
     */
    public boolean isDimensionIncludedByDefault() {
        return dimensionIncludedByDefault;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof DimensionFieldConfiguration)) {
            return false;
        }

        DimensionFieldConfiguration conf = (DimensionFieldConfiguration) other;
        return Objects.equals(name, conf.name) &&
                Objects.equals(description, conf.description) &&
                Objects.equals(queryIncludedByDefault, conf.queryIncludedByDefault) &&
                Objects.equals(dimensionIncludedByDefault, conf.dimensionIncludedByDefault);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, queryIncludedByDefault, dimensionIncludedByDefault);
    }
}
