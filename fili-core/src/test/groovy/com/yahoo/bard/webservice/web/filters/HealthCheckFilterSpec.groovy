// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory
import com.yahoo.bard.webservice.application.HealthCheckServletContextListener
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ResourceConfig
import com.yahoo.bard.webservice.application.healthchecks.VersionHealthCheck
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import com.codahale.metrics.health.HealthCheck
import com.codahale.metrics.health.HealthCheckRegistry

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.ServiceUnavailableException
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.UriInfo

@Timeout(30)    // Fail test if hangs
class HealthCheckFilterSpec extends Specification {

    private static SystemConfig systemConfig = SystemConfigProvider.getInstance()

    static JerseyTestBinder jtb
    HealthCheckRegistry registry

    String originalVersionKey = systemConfig.getStringProperty(VersionHealthCheck.VERSION_KEY)

    def setupSpec() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(HealthCheckFilter.class, ResponseCorsFilter.class, DataServlet.class)
    }

    def setup() {
        registry = HealthCheckRegistryFactory.getRegistry()
        def names = registry.getNames()
        names.each() {
            registry.unregister(it)
        }
    }

    def cleanup() {
        registry.unregister("VersionHealthCheck")
        systemConfig.resetProperty(VersionHealthCheck.VERSION_KEY, originalVersionKey)
    }
    def cleanupSpec() {
        // Release the test web container
        jtb.tearDown()
    }

    def "Healthy get passes"() {
        setup:
        // add healthy health check
        HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry()
        HealthCheck mockHealthyCheck = Mock(HealthCheck)
        mockHealthyCheck.execute() >> { HealthCheck.Result.HEALTHY }
        assert mockHealthyCheck.execute()?.isHealthy() == true

        registry.register("mockHealthyCheck", mockHealthyCheck )

        expect:
        jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().get(String.class)

        and: "preflightResponse passes"
        Response r = jtb.getHarness().target("data/shapes/week/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-01%2F2014-06-01")
            .request().options()
        r.getStatus() == Response.Status.OK.getStatusCode()

        cleanup:
        if (registry != null ) {
            registry.unregister("mockHealthyCheck")
        }
    }

    def "No defined HealthChecks fails"() {
        when:
        jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().get(String.class)

        then:
        thrown ServiceUnavailableException
    }

    def "Unhealthy get throws ServiceUnavailableException"() {
        setup:
        // add unhealthy health check
        HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry()
        HealthCheck mockUnhealthyCheck = Mock(HealthCheck)
        mockUnhealthyCheck.execute() >> { HealthCheck.Result.unhealthy(new Throwable()) }
        assert mockUnhealthyCheck.execute()?.isHealthy() == false

        registry.register("mockUnhealthyCheck", mockUnhealthyCheck )

        when: "get fails with unhealthy check"
        jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().get(String.class)

        then:
        thrown ServiceUnavailableException

        when: "preflightResponse fails with unhealthy check"
        Response r = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().options()

        then:
        r.getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode()

        cleanup:
        registry.unregister("mockUnhealthyCheck")
    }

    /**
     * Provides validation and coverage of ResourceConfig
     */
    def "Check ResourceConfig registers HealthCheckFilter"() {
        setup:
        ResourceConfig resourceConfig = new ResourceConfig()

        // add healthy health check
        HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry()
        HealthCheck mockHealthyCheck = Mock(HealthCheck)
        mockHealthyCheck.execute() >> { HealthCheck.Result.HEALTHY }
        assert mockHealthyCheck.execute()?.isHealthy() == true

        registry.register("mockHealthyCheck", mockHealthyCheck )

        when:
        Set<Class> classes = resourceConfig.getRegisteredClasses()

        then: "HealthCheck filter is registered"
        classes.contains( HealthCheckFilter.class )
        classes.contains( ResponseCorsFilter.class )

        when: "HealthCheck still passes"
        Response r = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().get()

        then:
        r.getStatus() == Response.Status.OK.getStatusCode()
        r.getHeaderString("Access-Control-Allow-Credentials") == "true"

        cleanup:
        registry?.unregister("mockHealthyCheck")
    }

    def "Healthy /data check passes"() {
        setup:
        HealthCheckFilter filter = Spy(HealthCheckFilter)
        filter.isHealthy() >> { true }

        ContainerRequestContext requestContext = Mock(ContainerRequestContext)
        UriInfo uriInfo = Mock(UriInfo)
        uriInfo.getAbsolutePath() >> { new URI("http://localhost:9998/v1/data/shapes/day/color?metrics=width") }
        requestContext.getUriInfo() >> { uriInfo }

        Response theResponse
        requestContext.abortWith(_) >> { Response response -> theResponse = response }

        expect:
        filter.filter(requestContext)
        theResponse == null
    }

    def "Unhealthy /data check fails"() {
        setup:
        HealthCheckFilter filter = Spy(HealthCheckFilter)
        filter.isHealthy() >> { false }

        ContainerRequestContext requestContext = Mock(ContainerRequestContext)
        UriInfo uriInfo = Mock(UriInfo)
        uriInfo.getAbsolutePath() >> { new URI("http://localhost:9998/v1/data/shapes/day/color?metrics=width") }
        requestContext.getUriInfo() >> { uriInfo }

        Response theResponse
        requestContext.abortWith(_) >> { Response response -> theResponse = response }

        expect:
        filter.filter(requestContext)
        theResponse.statusInfo == Status.SERVICE_UNAVAILABLE
    }

    def "No filter for /cache"() {
        setup:
        HealthCheckFilter filter = Spy(HealthCheckFilter)
        filter.isHealthy() >> { false }

        ContainerRequestContext requestContext = Mock(ContainerRequestContext)
        UriInfo uriInfo = Mock(UriInfo)
        uriInfo.getAbsolutePath() >> { new URI("http://localhost:9998/v1/cache/") }
        requestContext.getUriInfo() >> { uriInfo }

        Response theResponse
        requestContext.abortWith(_) >> { Response response -> theResponse = response }

        expect:
        filter.filter(requestContext)
        theResponse == null
    }

    /* tests for VersionHealthCheck */

    def "Healthly version check passes"() {
        setup:
        // Set expected values
        String oldVersion = systemConfig.getStringProperty(VersionHealthCheck.VERSION_KEY, null)
        String oldGitSha = systemConfig.getStringProperty(VersionHealthCheck.GIT_SHA_KEY, null)
        systemConfig.setProperty(VersionHealthCheck.VERSION_KEY, "1.2.3.4")
        systemConfig.setProperty(VersionHealthCheck.GIT_SHA_KEY, "b5d0be3956a18e128ff4192ca150f329f5e8f5c1")

        // register healthy version health check
        HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry()
        systemConfig.setProperty(VersionHealthCheck.VERSION_KEY, "1.2.3.4")
        registry.register("VersionHealthCheck", new VersionHealthCheck() )

        expect:
        def healthCheck = registry.runHealthCheck("VersionHealthCheck")
        healthCheck.getMessage() == "1.2.3.4:b5d0be3956a18e128ff4192ca150f329f5e8f5c1"
        healthCheck.isHealthy()

        and: "get /data passes in presence of VersionHealthCheck"
        jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().get(String.class)

        cleanup:
        systemConfig.resetProperty(VersionHealthCheck.VERSION_KEY, oldVersion)
        systemConfig.resetProperty(VersionHealthCheck.GIT_SHA_KEY, oldGitSha)
    }

    def "Unset version check fails"() {
        setup: "register unhealthy version health check"

        // Get the registry
        HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry()

        // Get the configuration, stash any old value to restore later, and set a known value
        String propertyName = "package_name"
        String oldProperty = systemConfig.getStringProperty(propertyName)

        systemConfig.setProperty(propertyName, "test2")

        // Re-register this health check to make sure it's clean and under our control
        String registeredName = "VersionHealthCheck"
        registry.unregister(registeredName)
        registry.register(
                registeredName,
                new VersionHealthCheck(
                        systemConfig.getPackageVariableName("version"),
                        systemConfig.getPackageVariableName("gitsha")
                )
        )

        expect: "The health check isn't healthy"
        def healthCheck = registry.runHealthCheck(registeredName)
        healthCheck.isHealthy() == false

        and: "The message is what we expect"
        healthCheck.getMessage() == "${VersionHealthCheck.VERSION_KEY} not set"

        and: "get /data fails in presence of VersionHealthCheck"
        Response r = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-11%2F2014-06-12")
            .request().get()
        r.getStatus() == Status.SERVICE_UNAVAILABLE.getStatusCode()

        cleanup: "Remove version health check and replace old property value"
        registry.unregister("VersionHealthCheck")
        systemConfig.resetProperty(propertyName, oldProperty)
    }

    def "Check HealthCheckServletContextListener"() {
        when:
        HealthCheckServletContextListener cl = new HealthCheckServletContextListener()

        then:
        cl.getHealthCheckRegistry() == HealthCheckRegistryFactory.getRegistry()
    }
}
