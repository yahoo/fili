// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

/**
 * This class represents the configurable parameters for a particular Druid service endpoint.
 */
public class DruidServiceConfig {

    private final String name;
    private final String url;
    private final Integer timeout;
    private final Integer priority;

    /**
     * Build the Druid Service Config.
     *
     * @param name  The name of the webservice
     * @param url  The URL for the webservice
     * @param timeout  The timeout in milliseconds
     * @param priority  The priority to be sent to the druid router
     */
    public DruidServiceConfig(String name, String url, Integer timeout, Integer priority) {
        this.name = name;
        this.url = url;
        this.timeout = timeout;
        this.priority = priority;
    }

    /**
     * The URL for the primary servlet of the druid service.
     *
     * @return an URL as a string
     */
    public String getUrl() {
        return url;
    }

    /**
     * The timeout for requests to druid.
     *
     * @return timeout in milliseconds
     */
    public Integer getTimeout() {
        return timeout;
    }

    /**
     * The priority value attached to druid request queries used in the metadata block of the query.
     *
     * @return an integer priority value
     */
    public Integer getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "Druid Service config for " + name +
                ": url: " + url +
                ", timeout: " + timeout +
                ", priority: " + priority + ".";
    }

    /**
     * Return the name and the URL of this druid service.
     *
     * @return A string representing the name and the URL of the druid service.
     */
    public String getNameAndUrl() {
        return name + " " + url;
    }
}
