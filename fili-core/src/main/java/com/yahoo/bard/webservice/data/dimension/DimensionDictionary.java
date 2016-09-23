// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;

import javax.inject.Singleton;

/**
 * Dimension dictionary.
 */
@Singleton
public class DimensionDictionary {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionDictionary.class);

    /**
     * Maps for API and Druid names to Dimension.
     * Warning! Not synchronized!
     */
    private final LinkedHashMap<String, Dimension> apiNameToDimension;

    /**
     * Constructor.
     */
    public DimensionDictionary() {
        apiNameToDimension = new LinkedHashMap<>();
    }

    /**
     * Constructor.
     *
     * @param dimensions  Set of Dimension values to add
     */
    public DimensionDictionary(Set<Dimension> dimensions) {
        this();
        addAll(dimensions);
    }

    /**
     * Find a Dimension given a dimension api name.
     *
     * @param dimensionName  API name to search
     *
     * @return the first dimension found (if exists)
     */
    public Dimension findByApiName(String dimensionName) {
        return apiNameToDimension.get(dimensionName);
    }

    /**
     * Get all dimensions available in dimension dictionary.
     *
     * @return a set of dimensions
     */
    public Set<Dimension> findAll() {
        return Collections.unmodifiableSet(new HashSet<>(apiNameToDimension.values()));
    }

    /**
     * Adds the specified element to the dictionary if it is not already present.
     *
     * @param dimension  element to add to dictionary
     *
     * @return <tt>true</tt> if the dictionary did not already contain the specified dimension
     * @see Set#add(Object)
     */
    public boolean add(Dimension dimension) {
        if (apiNameToDimension.containsKey(dimension.getApiName())) {
            return false;
        }
        Dimension oldDimension = apiNameToDimension.put(dimension.getApiName(), dimension);
        if (oldDimension != null) {
            // should never happen unless multiple loaders are running in race-condition
            ConcurrentModificationException e = new ConcurrentModificationException();
            LOG.error("Multiple loaders updating DimensionDictionary", e);
            throw e;
        }
        return true;
    }

    /**
     * Adds all of the dimensions in the specified collection to the dictionary.
     *
     * @param dimensions  collection of dimensions to add
     *
     * @return <tt>true</tt> if the dictionary changed as a result of the call
     * @see Set#addAll(Collection)
     */
    public boolean addAll(Collection<? extends Dimension> dimensions) {
        boolean flag = false;
        for (Dimension dimension : dimensions) {
            flag = add(dimension) || flag;
        }
        return flag;
    }

    @Override
    public String toString() {
        return "Dimension Dictionary: " + apiNameToDimension;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(apiNameToDimension);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj instanceof DimensionDictionary) {
            DimensionDictionary that = (DimensionDictionary) obj;
            return apiNameToDimension.equals(that.apiNameToDimension);
        }
        return false;
    }
}
