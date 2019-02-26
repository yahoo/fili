// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Filtered DimensionSpec using a List of filter values on Druid multi-dimensional value dimensions.
 */
public class ListFilteredDimensionSpec extends DimensionSpec {
    private final DimensionSpec delegate;
    private final List<String> values;
    private final Boolean isWhitelist;

    /**
     * Constructor.
     *
     * @param delegate  dimension spec provided by user.
     * @param values  list of values to be filtered on.
     * @param isWhitelist  choose whitelist or blacklist, defaults to isWhiteList true.
     */
    public ListFilteredDimensionSpec(DimensionSpec delegate, List<String> values, Boolean isWhitelist) {
        super(DefaultDimensionSpecType.LIST_FILTERED);
        this.delegate = delegate;
        this.values = Collections.unmodifiableList(values);
        this.isWhitelist = isWhitelist;
    }

    /**
     * Convenience Constructor,
     * defaults: isWhitelist=true.
     *
     * @param delegate  dimension spec provided by user.
     * @param values  list of values to be filtered on.
     */
    public ListFilteredDimensionSpec(DimensionSpec delegate, List<String> values) {
        this(delegate, values, true);
    }

    public DimensionSpec getDelegate() {
        return delegate;
    }

    public List<String> getValues() {
        return values;
    }

    public Boolean getIsWhitelist() {
        return isWhitelist;
    }

    @JsonIgnore
    @Override
    public Optional<Dimension> getConfigDimension() {
        return getDelegate().getConfigDimension();
    }

    // CHECKSTYLE:OFF
    public ListFilteredDimensionSpec withDelegate(DimensionSpec delegate) {
        return new ListFilteredDimensionSpec(delegate, values, isWhitelist);
    }

    public ListFilteredDimensionSpec withValues(List<String> values) {
        return new ListFilteredDimensionSpec(delegate, values, isWhitelist);
    }

    public ListFilteredDimensionSpec withIsWhiteList(Boolean isWhitelist) {
        return new ListFilteredDimensionSpec(delegate, values, isWhitelist);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delegate, values, isWhitelist);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        ListFilteredDimensionSpec other = (ListFilteredDimensionSpec) obj;
        return super.equals(obj) &&
                Objects.equals(delegate, other.delegate) &&
                Objects.equals(values, other.values) &&
                Objects.equals(isWhitelist, other.isWhitelist);
    }
}
