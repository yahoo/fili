// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.HealthCheckServletContextListener;
import com.yahoo.bard.webservice.application.MetricServletContextListener;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.codahale.metrics.servlet.InstrumentedFilter;
import com.codahale.metrics.servlets.AdminServlet;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.EnumSet;

import javax.servlet.DispatcherType;

/**
 * Launch Bard in Embedded Jetty.
 */
public class GenericMain {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String FILI_PORT = SYSTEM_CONFIG.getPackageVariableName("fili_port");

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
    }
}
