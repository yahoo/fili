// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory
import com.yahoo.bard.webservice.application.HealthCheckServletContextListener
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ResourceConfig
import com.yahoo.bard.webservice.web.endpoints.DataServlet
import com.yahoo.bard.webservice.web.endpoints.DimensionCacheLoaderServlet

import com.codahale.metrics.health.HealthCheck
import com.codahale.metrics.health.HealthCheckRegistry

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.ServiceUnavailableException
import javax.ws.rs.core.Response.Status

@Timeout(30)    // Fail test if hangs
class HealthCheckFilterSpec extends Specification {

    JerseyTestBinder jtb = new JerseyTestBinder(
            HealthCheckFilter,
            ResponseCorsFilter,
            DataServlet,
            DimensionCacheLoaderServlet
    )
    HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry()

    def setup() {
        cleanHealthCheckRegistry(registry)
    }

    def cleanup() {
        cleanHealthCheckRegistry(registry)

        // Release the test web container
        jtb.tearDown()
    }

    def "ResourceConfig registers HealthCheckFilter"() {
        expect: "The HealthCheckFilter is registered"
        new ResourceConfig().registeredClasses.contains(HealthCheckFilter)
    }

    def "HealthCheckServletContextListener surfaces the global registry the rest of the system uses"() {
        expect:
        new HealthCheckServletContextListener().healthCheckRegistry == HealthCheckRegistryFactory.registry
    }

    def "Healthy get passes"() {
        given: "A healthy health-check is registered"
        HealthCheck mockHealthyCheck = Mock(HealthCheck)
        mockHealthyCheck.execute() >> HealthCheck.Result.healthy()
        registry.register("mockHealthyCheck", mockHealthyCheck)

        expect: "Normal request comes back fine"
        jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-06-11%2F2014-06-12")
                .request()
                .get(String.class)

        and: "Preflight request comes back fine"
        jtb.getHarness().target("data/shapes/week/color")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-06-01%2F2014-06-01")
                .request()
                .options()
                .getStatus()  == Status.OK.getStatusCode()
    }

    def "Unhealthy get throws ServiceUnavailableException"() {
        given: "An unhealthy health-check is registered"
        HealthCheck mockUnhealthyCheck = Mock(HealthCheck)
        mockUnhealthyCheck.execute() >> HealthCheck.Result.unhealthy(new Throwable())
        registry.register("mockUnhealthyCheck", mockUnhealthyCheck)

        when: "Normal request fails with unhealthy check"
        jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-06-11%2F2014-06-12")
                .request()
                .get(String.class)

        then:
        thrown ServiceUnavailableException

        expect: "Preflight request fails with unhealthy check"
        jtb.getHarness().target("data/shapes/day/color")
                .queryParam("metrics","width")
                .queryParam("dateTime","2014-06-11%2F2014-06-12")
                .request()
                .options()
                .getStatus() == Status.SERVICE_UNAVAILABLE.getStatusCode()
    }

    def "Filter is not applied to /cache"() {
        given: "An unhealthy health-check is registered"
        HealthCheck mockUnhealthyCheck = Mock(HealthCheck)
        mockUnhealthyCheck.execute() >> HealthCheck.Result.unhealthy(new Throwable())
        registry.register("mockUnhealthyCheck", mockUnhealthyCheck)

        expect: "Normal request comes back fine"
        jtb.getHarness().target("cache/cacheStatus")
                .request()
                .get(String.class)
    }

    private static void cleanHealthCheckRegistry(HealthCheckRegistry registry) {
        registry.names.each {
            registry.unregister(it)
        }
    }
}
