// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.virtualcolumns;

import com.yahoo.bard.webservice.druid.model.VirtualColumnType;

/**
 * This is marker a interface to reference Virtual Columns.
 */
public interface VirtualColumn {

    /**
     * Get the name of the virtual column in the response.
     *
     * @return the name of the virtual column in the response
     */
    String getName();

    /**
     * Get the type of the virtual column in the response.
     *
     * @return the type of the virtual column in the response
     */
    VirtualColumnType getType();

    /**
     * Get the output type of the virtual column in the response.
     *
     * @return the output type of the virtual column in the response
     */
    String getOutputType();

    /**
     * Makes a copy of this virtual column with the current name replaced by the provided name.
     *
     * @param name The new output name for virtual column
     * @return the updated copy
     */
    VirtualColumn withName(String name);


    /**
     * Compares the object with the virtual column object.
     *
     * @param o The object being compared
     * @return whether the objects are equal
     */
    boolean virtualColumnEquals(Object o);
}
