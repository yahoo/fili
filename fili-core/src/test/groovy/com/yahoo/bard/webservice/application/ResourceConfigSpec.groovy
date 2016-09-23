// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter
import com.yahoo.bard.webservice.web.filters.HealthCheckFilter
import com.yahoo.bard.webservice.web.filters.QueryParameterNormalizationFilter
import com.yahoo.bard.webservice.web.filters.RateLimitFilter
import com.yahoo.bard.webservice.web.filters.ResponseCorsFilter

import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener

import org.glassfish.hk2.utilities.Binder

import spock.lang.Specification

import java.util.function.Consumer

/**
 * Test that the resource configuration correctly registers all filters correctly
 */
public class ResourceConfigSpec extends Specification {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String BINDER_KEY = SYSTEM_CONFIG.getPackageVariableName("resource_binder")

    // A mock representing the binder produced by the BinderFactory
    static Binder binder

    // A mock to arbitrarily accept events for testing
    static Consumer clicker

    Set<Class> filters
    Class<org.glassfish.jersey.server.ResourceConfig> resourceConfigClass;


    def setup() {
        SYSTEM_CONFIG.setProperty(BINDER_KEY, MockingBinderFactory.canonicalName)
        clicker = Mock(Consumer)
        binder = Mock(Binder)
        resourceConfigClass = ResourceConfig

        filters =  [HealthCheckFilter, RateLimitFilter, BardLoggingFilter, ResponseCorsFilter, QueryParameterNormalizationFilter] as Set
    }

    def cleanup() {
        binder = null
        clicker = null
        SYSTEM_CONFIG.clearProperty(BINDER_KEY)
    }

    static Binder getBinder() {
        return binder
    }

    static Consumer getClicker() {
        return clicker
    }

    def "Test instantiation triggers initialization and binding lifecycles"() {
        when:
        ResourceConfig config = resourceConfigClass.newInstance()

        then:
        config.classes.containsAll(filters)
        config.getInstances().contains(binder)
        config.getInstances().stream().anyMatch { it.class == InstrumentedResourceMethodApplicationListener }

        1 * clicker.accept(MockingBinderFactory.INIT)
        1 * clicker.accept(MockingBinderFactory.BUILD_BIND)
        1 * clicker.accept(_ as ResourceConfig)
    }
}
