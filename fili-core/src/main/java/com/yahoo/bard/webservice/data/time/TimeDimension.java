// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.impl.SimpleVirtualDimension;

public class TimeDimension extends SimpleVirtualDimension {
    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * The time dimension name if any is supplied.
     */
    public static final String TIME_DIMENSION_NAME =
            SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName(
            "time_dimension"));

    public static final TimeDimension INSTANCE = new TimeDimension();

    /**
     * Constructor.
     */
    private TimeDimension() {
        super(TIME_DIMENSION_NAME);
    }
}
