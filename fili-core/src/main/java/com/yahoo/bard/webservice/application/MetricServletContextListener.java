// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlet.InstrumentedFilter;
import com.codahale.metrics.servlets.MetricsServlet;

import javax.servlet.ServletContextEvent;

/**
 * Servlet Context Listener which initializes the Codahale metrics registry
 * used by the Codahale metrics filter and the metrics servlet.
 */
public class MetricServletContextListener extends MetricsServlet.ContextListener {

    @Override
    public void contextInitialized(ServletContextEvent event) {

        /*
         * There is a codahale listener which does this, but the logic is done here to reduce the number
         * of context listeners.
         */
        event.getServletContext().setAttribute(InstrumentedFilter.REGISTRY_ATTRIBUTE, getMetricRegistry());
        super.contextInitialized(event);
    }

    @Override
    protected MetricRegistry getMetricRegistry() {
        return MetricRegistryFactory.getRegistry();
    }
}
