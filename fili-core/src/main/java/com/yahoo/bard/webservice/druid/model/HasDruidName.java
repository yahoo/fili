// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

/**
 * A marker interface for things which have a druid name.
 */
@FunctionalInterface
public interface HasDruidName {
    /**
     * Get the Druid name.
     *
     * @return the Druid name.
     */
    String getDruidName();
}
