// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter;
import com.yahoo.bard.webservice.web.filters.HealthCheckFilter;
import com.yahoo.bard.webservice.web.filters.QueryParameterNormalizationFilter;
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

    private final String bindingFactory = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("resource_binder")
    );

    /**
     * Constructor.
     *
     * @throws ClassNotFoundException if a class was not found when attempting to load it
     * @throws InstantiationException if a class was not able to be instantiated
     * @throws IllegalAccessException if there was a problem accessing something due to security restrictions
     */
    public ResourceConfig() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        // Build the binder using the configured factory
        Class<? extends BinderFactory> binderClass = Class.forName(bindingFactory).asSubclass(BinderFactory.class);
        BinderFactory binderFactory = binderClass.newInstance();
        Binder binder = binderFactory.buildBinder();

        // Register Instrumentation
        register(new InstrumentedResourceMethodApplicationListener(MetricRegistryFactory.getRegistry()));
        registerMetricsAppender();

        register(binder);
        registerFilters();

        // Call post-registration hook to allow for additional registration
        binderFactory.afterRegistration(this);
    }

    /**
     * Register the built-in filters.
     */
    protected void registerFilters() {
        // Register BardLoggingFilter
        register(BardLoggingFilter.class, 1);

        // Register CORS filter
        register(ResponseCorsFilter.class, 2);

        // Register Rate Limit Filter
        register(RateLimitFilter.class, 3);

        // Register query parameter normalization Filter
        register(QueryParameterNormalizationFilter.class, 4);

        // Register HealthCheckFilter
        register(HealthCheckFilter.class, 5);
    }

    /**
     * Create a Codahale metric appender and add to ROOT logger.
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
