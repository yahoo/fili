// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.codahale.metrics.health.HealthCheckRegistry;

/**
 * EVIL - a RegistryFactory:  An object which builds an object which
 * builds objects.  The factory provides a single instance of a Codahale
 * HealthCheckRegistry to those who want it.  This is not injected by HK2 because
 * it cannot be injected into a ServletContextListener (Injections here don't work)
 * where it must be referenced.
 */
public class HealthCheckRegistryFactory {

    private static HealthCheckRegistry registry = null;

    /**
     * Get the global registry.
     *
     * @return the registry
     */
    public static synchronized HealthCheckRegistry getRegistry() {
        if (registry == null) {
            registry = new HealthCheckRegistry();
        }
        return registry;
    }
}
