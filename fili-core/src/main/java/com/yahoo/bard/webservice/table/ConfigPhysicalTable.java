// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.table.availability.Availability;

/**
 * This interface limits direct access to availability to tables intended for use during the configuration lifecycle.
 */
public interface ConfigPhysicalTable extends PhysicalTable  {

    /**
     * Get the value of the backing availability instance for this physical table.
     *
     * @return The availability or a runtime exception if there isn't one.
     */
    Availability getAvailability();
}
