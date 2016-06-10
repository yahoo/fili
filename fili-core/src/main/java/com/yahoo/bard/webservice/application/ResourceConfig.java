// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter;
import com.yahoo.bard.webservice.web.filters.HealthCheckFilter;
import com.yahoo.bard.webservice.web.filters.RateLimitFilter;
import com.yahoo.bard.webservice.web.filters.ResponseCorsFilter;

import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.codahale.metrics.logback.InstrumentedAppender;

import org.glassfish.hk2.utilities.Binder;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

/**
 * The resource configuration for the bard web applications, especially core filters.
 */
public class ResourceConfig extends org.glassfish.jersey.server.ResourceConfig {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final String BINDING_FACTORY = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("resource_binder")
    );

    public ResourceConfig() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        // Build the binder using the configured factory
        Class<?> binderClass = Class.forName(BINDING_FACTORY);
        BinderFactory binderFactory = (BinderFactory) binderClass.newInstance();
        Binder binder = binderFactory.buildBinder();

        // Register Instrumentation
        register(new InstrumentedResourceMethodApplicationListener(MetricRegistryFactory.getRegistry()));
        registerMetricsAppender();

        register(binder);
        registerFilters();

        // Call post-registration hook to allow for additional registration
        binderFactory.afterRegistration(this);
    }

    protected void registerFilters() {
        int i = 1;
        // Register BardLoggingFilter
        register(BardLoggingFilter.class, i++);

        // Register CORS filter
        register(ResponseCorsFilter.class, i++);

        // Register Rate Limit Filter
        register(RateLimitFilter.class, i++);

        // Register HealthCheckFilter
        register(HealthCheckFilter.class, i++);
    }

    /**
     * Create a Codahale metric appender and add to ROOT logger
     */
    private void registerMetricsAppender() {
        // Get the root logger
        LoggerContext factory = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = factory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);

        // Create the metrics appender and give it the root logger's context
        InstrumentedAppender appender = new InstrumentedAppender(MetricRegistryFactory.getRegistry());
        appender.setContext(rootLogger.getLoggerContext());
        appender.start();

        // Add the appender to the root logger
        rootLogger.addAppender(appender);
    }
}
