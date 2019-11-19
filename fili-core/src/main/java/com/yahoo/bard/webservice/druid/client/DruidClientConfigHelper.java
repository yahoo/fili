// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Helper to fetch druid url and timeout settings.
 */
public class DruidClientConfigHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DruidClientConfigHelper.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * The routing priority for all queries.
     */
    private static final String DRUID_PRIORITY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_priority");

    /**
     * The url for the broker vip which serves all queries.
     */
    public static final String DRUID_BROKER_URL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_broker");

    /**
     * The url for the coordinator vip which serves low latency queries.
     */
    public static final String DRUID_COORD_URL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_coord");
    /**
     * The timeout setting for all queries.
     */
    private static final String DRUID_REQUEST_TIMEOUT_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_request_timeout");

    /**
     * The default timeout for queries.
     */
    private static final int DRUID_REQUEST_TIMEOUT_DEFAULT = Math.toIntExact(TimeUnit.MINUTES.toMillis(10));

    /**
     * Fetches the druid Priority.
     *
     * @return druid priority
     */
    public static Integer getDruidPriority() {
        String priority = SYSTEM_CONFIG.getStringProperty(DRUID_PRIORITY_KEY, "1");
        return Integer.parseInt(priority);
    }

    /**
     * Fetches the druid URL.
     *
     * @return druid URL
     */
    public static String getDruidUrl() {
        String url = SYSTEM_CONFIG.getStringProperty(DRUID_BROKER_URL_KEY, null);
        validateUrl(url);
        return url;
    }

    /**
     * Fetches the URL of the druid coordinator.
     *
     * @return druid coordinator URL
     */
    public static String getDruidCoordUrl() {
        return SYSTEM_CONFIG.getStringProperty(DRUID_COORD_URL_KEY, null);
    }

    /**
     * Fetches the druid request timeout.
     *
     * @return druid request timeout
     */
    public static Integer getDruidTimeout() {
        Integer time = fetchDruidResponseTimeOut(DRUID_REQUEST_TIMEOUT_KEY);
        return time;
    }

    /**
     * Create a druid service configuration object.
     *
     * @return a druid service configuration object with all configuration parameters set
     */
    public static DruidServiceConfig getServiceConfig() {
        return new DruidServiceConfig("Broker", getDruidUrl(), getDruidTimeout(), getDruidPriority());
    }

    /**
     * Create a druid service configuration object for the metadata service.
     *
     * @return a druid service configuration object with all configuration parameters set
     */
    public static DruidServiceConfig getMetadataServiceConfig() {
        return new DruidServiceConfig(
                "Coordinator",
                getDruidCoordUrl(),
                getDruidTimeout(),
                getDruidPriority()
        );
    }

    /**
     * Get the Druid response timeout value for the given system property.
     *
     * @param timeOutSysProp  The system property to read the timeout from
     *
     * @return the timeout
     */
    private static Integer fetchDruidResponseTimeOut(String timeOutSysProp) {
        try {
            return SYSTEM_CONFIG.getIntProperty(timeOutSysProp, DRUID_REQUEST_TIMEOUT_DEFAULT);
        } catch (SystemConfigException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Validate the format of url to see if it is a pontential validate url.
     *
     * @param url  The url to check against
     */
    private static void validateUrl(String url) {
        try {
            new URL(url);
        } catch (MalformedURLException e) {
            String message = String.format("Invalid druid host url provided: %s", url);
            LOG.error(message);
            throw new IllegalArgumentException(message, e);
        }
    }
}
