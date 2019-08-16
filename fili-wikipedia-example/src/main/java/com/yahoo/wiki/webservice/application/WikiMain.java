// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.HealthCheckServletContextListener;
import com.yahoo.bard.webservice.application.MetricServletContextListener;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions;

import com.codahale.metrics.servlet.InstrumentedFilter;
import com.codahale.metrics.servlets.AdminServlet;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;

import javax.servlet.DispatcherType;

/**
 * Launch Bard in Embedded Jetty.
 */
public class WikiMain {
    private static final Logger LOG = LoggerFactory.getLogger(WikiMain.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String FILI_PORT = SYSTEM_CONFIG.getPackageVariableName("fili_port");

    /**
     * Makes the dimensions passthrough.
     * <p>
     * This method sends a lastUpdated date to each dimension in the dimension cache, allowing the health checks
     * to pass without having to set up a proper dimension loader. For each dimension, d, the following query is
     * sent to the /v1/cache/dimensions/d endpoint:
     * {
     *     "name": "d",
     *     "lastUpdated": "2016-01-01"
     * }
     *
     * @param port  The port through which we access the webservice
     *
     * @throws IOException If something goes terribly wrong when building the JSON or sending it
     */
    private static void markDimensionCacheHealthy(int port) throws IOException {
        try (AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient()) {
            for (DimensionConfig dimensionConfig : new WikiDimensions().getAllDimensionConfigurations()) {
                String dimension = dimensionConfig.getApiName();
                BoundRequestBuilder boundRequestBuilder = asyncHttpClient.preparePost("http://localhost:" + port +
                        "/v1/cache/dimensions/" + dimension)
                        .addHeader("Content-type", "application/json")
                        .setBody(
                                String.format("{%n \"name\":\"%s\",%n \"lastUpdated\":\"2016-01-01\"%n}", dimension)
                        );

                ListenableFuture<Response> responseFuture = boundRequestBuilder.execute();
                try {
                    Response response = responseFuture.get();
                    LOG.debug("Mark Dimension Cache Updated Response: {}", response);
                } catch (InterruptedException | ExecutionException e) {
                    LOG.warn("Failed while marking dimensions healthy", e);
                }
            }
        }
    }

    /**
     * Run the Wikipedia application.
     *
     * @param args  command line arguments
     * @throws Exception if the server fails to start or crashes
     */
    public static void main(String[] args) throws Exception {
        int port = SYSTEM_CONFIG.getIntProperty(FILI_PORT);

        Server server = new Server(port);
        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

        servletContextHandler.addEventListener(new MetricServletContextListener());
        servletContextHandler.addEventListener(new HealthCheckServletContextListener());

        servletContextHandler.setContextPath("/");
        servletContextHandler.setResourceBase("src/main/webapp");

        //Activate codahale metrics
        FilterHolder instrumentedFilterHolder = new FilterHolder(InstrumentedFilter.class);
        instrumentedFilterHolder.setName("instrumentedFilter");
        instrumentedFilterHolder.setAsyncSupported(true);
        servletContextHandler.addFilter(instrumentedFilterHolder, "/*", EnumSet.noneOf(DispatcherType.class));

        // Static resource handler
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setResourceBase("src/main/webapp");

        // Add the handlers to the server
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { resourceHandler, servletContextHandler });
        server.setHandler(handlers);

        ServletHolder servletHolder = servletContextHandler.addServlet(ServletContainer.class, "/v1/*");
        servletHolder.setInitOrder(1);
        servletHolder.setInitParameter(
                "javax.ws.rs.Application",
                "com.yahoo.bard.webservice.application.ResourceConfig");
        servletHolder.setInitParameter(
                "jersey.config.server.provider.packages",
                "com.yahoo.bard.webservice.web.endpoints");

        servletContextHandler.addServlet(AdminServlet.class, "/*");

        server.start();

        markDimensionCacheHealthy(port);
    }
}
