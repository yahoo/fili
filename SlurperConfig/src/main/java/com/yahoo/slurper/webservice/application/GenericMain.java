// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.application;

import com.codahale.metrics.servlet.InstrumentedFilter;
import com.codahale.metrics.servlets.AdminServlet;
import com.yahoo.bard.webservice.application.HealthCheckServletContextListener;
import com.yahoo.bard.webservice.application.MetricServletContextListener;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;

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

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;

/**
 * Launch Bard in Embedded Jetty.
 */
public class GenericMain {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String FILI_PORT = SYSTEM_CONFIG.getPackageVariableName("fili_port");

    private static final Logger LOG = LoggerFactory.getLogger(GenericMain.class);
    private static final String DRUID_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/config/druid/";

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
        AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient();
        for (DimensionConfig dimensionConfig : new ExternalDimensionsLoader(
                new ExternalConfigLoader(),
                DRUID_CONFIG_FILE_PATH
        ).getAllDimensionConfigurations()) {
            String dimension = dimensionConfig.getApiName();
            BoundRequestBuilder boundRequestBuilder = asyncHttpClient.preparePost("http://localhost:" + port +
                    "/v1/cache/dimensions/" + dimension)
                    .addHeader("Content-type", "application/json")
                    .setBody(
                            String.format("{\n \"name\":\"%s\",\n \"lastUpdated\":\"2016-01-01\"\n}", dimension)
                    );

            ListenableFuture<Response> responseFuture = boundRequestBuilder.execute();
            try {
                Response response = responseFuture.get();
                LOG.debug("Mark Dimension Cache Updated Response: ", response);
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("Failed while marking dimensions healthy", e);
            }
        }
    }

    /**
     * Run a generic setup which mirrors all information from druid into fili configuration.
     *
     * @param args  Command line arguments.
     *
     * @throws Exception if the server fails to start or crashes.
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
        handlers.setHandlers(new Handler[] {resourceHandler, servletContextHandler});
        server.setHandler(handlers);

        ServletHolder servletHolder = servletContextHandler.addServlet(ServletContainer.class, "/v1/*");
        servletHolder.setInitOrder(1);
        servletHolder.setInitParameter(
                "javax.ws.rs.Application",
                "com.yahoo.bard.webservice.application.ResourceConfig"
        );
        servletHolder.setInitParameter(
                "jersey.config.server.provider.packages",
                "com.yahoo.bard.webservice.web.endpoints"
        );

        servletContextHandler.addServlet(AdminServlet.class, "/*");

        server.start();
        markDimensionCacheHealthy(port);
    }
}
