// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import org.glassfish.hk2.utilities.Binder;

/**
 * A binder factory builds a custom binder for the jersey application.
 */
public interface BinderFactory {

    /**
     * Build an hk2 Binder instance.  This binder should bind all data dictionaries after loading them,
     * as well as UI/NonUI web services, Partial Data polling and Health Check metrics
     *
     * @return A binder instance
     */
    Binder buildBinder();

    /**
     * Allows additional app-specific Jersey feature registration and config.
     *
     * @param resourceConfig  Resource config to use for accessing the configuration
     */
    void afterRegistration(ResourceConfig resourceConfig);
}
