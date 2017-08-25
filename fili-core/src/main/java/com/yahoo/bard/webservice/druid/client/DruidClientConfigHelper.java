// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Helper to fetch druid url and timeout settings.
 */
public class DruidClientConfigHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DruidClientConfigHelper.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * The routing priority for low latency queries.
     */
    private static final String UI_DRUID_PRIORITY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("ui_druid_priority");

    /**
     * The routing priority for high latency queries.
     */
    private static final String NON_UI_DRUID_PRIORITY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("non_ui_druid_priority");

    /**
     * The routing priority for all queries.
     */
    private static final String DRUID_PRIORITY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_priority");

    /**
     * The url for the broker vip which serves low latency queries.
     */
    private static final String UI_DRUID_BROKER_URL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("ui_druid_broker");

    /**
     * The url for the broker vip which serves higher latency queries.
     */
    private static final String NON_UI_DRUID_BROKER_URL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("non_ui_druid_broker");

    /**
     * The url for the broker vip which serves all queries.
     */
    private static final String DRUID_BROKER_URL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_broker");

    /**
     * The url for the coordinator vip which serves low latency queries.
     */
    private static final String DRUID_COORD_URL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_coord");

    /**
     * The timeout setting for low latency queries.
     */
    private static final String UI_DRUID_REQUEST_TIMEOUT_KEY =
            SYSTEM_CONFIG.getPackageVariableName("ui_druid_request_timeout");

    /**
     * The timeout setting for high latency queries.
     */
    private static final String NON_UI_DRUID_REQUEST_TIMEOUT_KEY =
            SYSTEM_CONFIG.getPackageVariableName("non_ui_druid_request_timeout");

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
     * Fetches the druid UI request Priority.
     *
     * @return druid UI request priority
     *
     * @deprecated The druid UI priority is deprecated, please use {@link #getDruidPriority()}.
     */
    @Deprecated
    public static Integer getDruidUiPriority() {
        String priority = SYSTEM_CONFIG.getStringProperty(UI_DRUID_PRIORITY_KEY, null);
        if (priority == null || "".equals(priority)) {
            return null;
        }
        return Integer.parseInt(priority);
    }

    /**
     * Fetches the druid non-UI Priority.
     *
     * @return druid non-UI priority
     *
     * @deprecated The druid non-UI priority is deprecated, please use {@link #getDruidPriority()}.
     */
    @Deprecated
    public static Integer getDruidNonUiPriority() {
        String priority = SYSTEM_CONFIG.getStringProperty(NON_UI_DRUID_PRIORITY_KEY, null);
        if (priority == null || "".equals(priority)) {
            return getDruidUiPriority();
        }
        return Integer.parseInt(priority);
    }

    /**
     * Fetches the druid Priority.
     *
     * @return druid priority
     */
    public static Integer getDruidPriority() {
        String priority = SYSTEM_CONFIG.getStringProperty(DRUID_PRIORITY_KEY, null);
        if (priority == null || "".equals(priority)) {
            return getDruidNonUiPriority();
        }
        return Integer.parseInt(priority);
    }

    /**
     * Fetches the druid UI URL.
     *
     * @return druid UI URL
     *
     * @deprecated The druid UI URL is deprecated, please use {@link #getDruidUrl()}
     */
    @Deprecated
    public static String getDruidUiUrl() {
        String url =  SYSTEM_CONFIG.getStringProperty(UI_DRUID_BROKER_URL_KEY, null);
        if (url == null) {
            LOG.warn("ui_druid_broker not set, using non_ui_druid_broker instead");
            return null;
        }
        return url;
    }

    /**
     * Fetches the druid non-UI URL.
     *
     * @return druid non-UI URL
     *
     * @deprecated The druid non-UI URL is deprecated, please use {@link #getDruidUrl()}.
     */
    @Deprecated
    public static String getDruidNonUiUrl() {
        String url = SYSTEM_CONFIG.getStringProperty(NON_UI_DRUID_BROKER_URL_KEY, null);
        if (url == null) {
            LOG.warn("non_ui_druid_broker not set, using druid_broker instead");
            return getDruidUiUrl();
        }
        return url;
    }

    /**
     * Fetches the druid URL.
     *
     * @return druid URL
     */
    public static String getDruidUrl() {
        String url = SYSTEM_CONFIG.getStringProperty(DRUID_BROKER_URL_KEY, null);
        if (url == null) {
            LOG.warn("druid_broker not set, using ui_druid_broker instead");
            return getDruidNonUiUrl();
        }
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
     * Fetches the druid UI request timeout.
     *
     * @return druid UI request timeout
     *
     * @deprecated The druid UI timeout is deprecated, please use {@link #getDruidTimeout()}.
     */
    @Deprecated
    public static Integer getDruidUiTimeout() {
        return fetchDruidResponseTimeOut(UI_DRUID_REQUEST_TIMEOUT_KEY);
    }

    /**
     * Fetches the druid non-UI request timeout.
     *
     * @return druid non-UI request timeout
     *
     * @deprecated The druid non-UI timeout is deprecated, please use {@link #getDruidTimeout()}.
     */
    @Deprecated
    public static Integer getDruidNonUiTimeout() {
        Integer time = fetchDruidResponseTimeOut(NON_UI_DRUID_REQUEST_TIMEOUT_KEY);
        if (time == null) {
            return fetchDruidResponseTimeOut(UI_DRUID_REQUEST_TIMEOUT_KEY);
        }
        return time;
    }

    /**
     * Fetches the druid request timeout.
     *
     * @return druid request timeout
     */
    public static Integer getDruidTimeout() {
        Integer time = fetchDruidResponseTimeOut(DRUID_REQUEST_TIMEOUT_KEY);
        if (time == null) {
            return fetchDruidResponseTimeOut(NON_UI_DRUID_REQUEST_TIMEOUT_KEY);
        }
        return time;
    }

    /**
     * Create a druid service configuration object for the UI service.
     *
     * @return a druid service configuration object with all configuration parameters set
     *
     * @deprecated The druid UI service config is deprecated, please use {@link #getServiceConfig()}.
     */
    @Deprecated
    public static DruidServiceConfig getUiServiceConfig() {
        return new DruidServiceConfig("Broker", getDruidUiUrl(), getDruidUiTimeout(), getDruidUiPriority());
    }

    /**
     * Create a druid service configuration object for the non UI service.
     *
     * @return a druid service configuration object with all configuration parameters set
     *
     * @deprecated The druid Non-UI service config is deprecated, please use {@link #getServiceConfig()}.
     */
    @Deprecated
    public static DruidServiceConfig getNonUiServiceConfig() {
        return new DruidServiceConfig("Broker", getDruidNonUiUrl(), getDruidNonUiTimeout(), getDruidNonUiPriority());
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
                getDruidNonUiTimeout(),
                getDruidNonUiPriority()
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
}
