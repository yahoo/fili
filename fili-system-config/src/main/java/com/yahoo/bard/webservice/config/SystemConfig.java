// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import javax.validation.constraints.NotNull;

/**
 * System Config provides an interface for retrieving configuration values, allowing for implicit type conversion,
 * defaulting, and use of a runtime properties interface to override configured settings.
 */
public interface SystemConfig {
    Logger LOG = LoggerFactory.getLogger(SystemConfig.class);
    String MISSING_CONFIG_ERROR_FORMAT = "Error retrieving system property '%s'.";
    /**
     * Get a package scoped variable name.
     *
     * @param suffix  The variable name of the configuration variable without the package prefix
     * @return variable name
     */
    default String getPackageVariableName(String suffix) {
        return getStringProperty("package_name") + "__" + suffix;
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     *
     * @return requestedPropertyValue The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default String getStringProperty(@NotNull String key) throws SystemConfigException {
        try {
            return getMasterConfiguration().getString(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     *
     * @return requestedPropertyValue The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default int getIntProperty(@NotNull String key) throws SystemConfigException {
        try {
            return getMasterConfiguration().getInt(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     *
     * @return requestedPropertyValue The value for the requested key, false if null
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default boolean getBooleanProperty(@NotNull String key) throws SystemConfigException {
        try {
            return getMasterConfiguration().getBoolean(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default float getFloatProperty(@NotNull String key) throws SystemConfigException {
        try {
            return getMasterConfiguration().getFloat(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default double getDoubleProperty(@NotNull String key) throws SystemConfigException {
        try {
            return getMasterConfiguration().getDouble(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default long getLongProperty(@NotNull String key) throws SystemConfigException {
        try {
            return getMasterConfiguration().getLong(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param <T>  The expected type of item in the list
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    @SuppressWarnings("unchecked")
    default <T> List<T> getListProperty(@NotNull String key) throws SystemConfigException {
        try {
            //CompositeConfiguration#getList method returns a bare List that needs to be cast to the appropriate type
            return (List<T>) getMasterConfiguration().getList(key);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     *
     * @return requestedPropertyValue The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default String getStringProperty(@NotNull String key, String defaultValue) throws SystemConfigException {
        try {
            return getMasterConfiguration().getString(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     *
     * @return requestedPropertyValue The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default int getIntProperty(@NotNull String key, int defaultValue) throws SystemConfigException {
        try {
            return getMasterConfiguration().getInt(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     *
     * @return requestedPropertyValue The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default boolean getBooleanProperty(@NotNull String key, boolean defaultValue) throws SystemConfigException {
        try {
            return getMasterConfiguration().getBoolean(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default float getFloatProperty(@NotNull String key, float defaultValue) throws SystemConfigException {
        try {
            return getMasterConfiguration().getFloat(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default double getDoubleProperty(@NotNull String key, double defaultValue) throws SystemConfigException {
        try {
            return getMasterConfiguration().getDouble(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default long getLongProperty(@NotNull String key, long defaultValue) throws SystemConfigException {
        try {
            return getMasterConfiguration().getLong(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get property value for a key.
     *
     * @param key  The key for which value needs to be fetched, not null
     * @param defaultValue  The value to return if there is no configured value
     * @param <T>  The expected type of item in the list
     *
     * @return The value for the requested key
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    @SuppressWarnings("unchecked")
    default <T> List<T> getListProperty(@NotNull String key, List<T> defaultValue) throws SystemConfigException {
        try {
            //CompositeConfiguration#getList method returns a bare List that needs to be cast to the appropriate type
            return (List<T>) getMasterConfiguration().getList(key, defaultValue);
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Set property value for a key.
     *
     * @param key  The key of the property to change, not null
     * @param value  The new value
     *
     * @throws SystemConfigException A system exception if any errors occurred
     */
    default void setProperty(@NotNull String key, String value) throws SystemConfigException {
        resetProperty(key, value);
    }

    /**
     * Remove property from the user-defined runtime configuration.
     *
     * @param key  The key of the property to remove.
     */
    default void clearProperty(@NotNull String key) {
        resetProperty(key, null);
    }

    /**
     * Remove property from the user-defined runtime configuration.
     * If value is null, clear the property, otherwise set property to value.
     *
     * @param key  The key of the property to remove.
     * @param value  The value to reset to (if not null)
     */
    default void resetProperty(@NotNull String key, String value) {
        try {
            if (value == null) {
                getRuntimeProperties().remove(key);
            } else {
                getRuntimeProperties().setProperty(key, value);
            }
        } catch (Exception e) {
            String message = String.format(MISSING_CONFIG_ERROR_FORMAT, key);
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * Get the internal System Configuration instance.
     * This method is intended primary for interface support and not for client interactions.
     *
     * @return The composite configuration which backs this System Configuration
     */
    Configuration getMasterConfiguration();

    /**
     * The properties file used to hold modified SystemConfiguration values.
     * This method is intended primary for interface support and not for client interactions.
     *
     * @return  A properties object which acts as a runtime mask against other configuration properties
     */
    Properties getRuntimeProperties();

}
