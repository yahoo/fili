// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.table.availability.Availability;

/**
 * Availability is used only exposed configuration lifecycle of physical tables.
 * This subinterface limits the exposure of availability information.
 */
public interface ConfigPhysicalTable extends PhysicalTable  {

    /**
     * Get the value of the actual availability for this physical table.
     *
     * @return The current actual physical availability or a runtime exception if there isn't one yet.
     */
    Availability getAvailability();
}
