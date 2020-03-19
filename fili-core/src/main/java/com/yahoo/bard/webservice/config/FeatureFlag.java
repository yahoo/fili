// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import org.slf4j.LoggerFactory;

/**
 * Feature flags bind an object to a system configuration name.
 */
public interface FeatureFlag {

    /**
     * Returns the name of a specific flag included in a class that implements this interface.
     * When it is implemented by an Enum class then the method {@code name} that is available in the Enum implicitly
     * implements this method of the interface
     *
     * @return The name of the enumeration instance.
     */
    String name();

    /**
     * Returns the simple property name of this feature flag.
     *
     * @return The name of the feature flag.
     */
    String getName();

    /**
     * Returns whether the feature flag is activated.
     *
     * @return The status of the feature flag.
     */
    boolean isOn();


    /**
     * Returns whether the feature flag has been configured.
     *
     * @return true if the feature flag has been configured.
     */
    default boolean isSet() {
        SystemConfig systemConfig = SystemConfigProvider.getInstance();
        try {
            return systemConfig.getStringProperty(systemConfig.getPackageVariableName(getName()), null) != null;
        } catch (SystemConfigException exception) {
            return false;
        }
    }

    /**
     * Sets the status of the feature flag.
     *
     * @param newValue  The new status of the feature flag.
     */
    void setOn(Boolean newValue);

    /**
     * Restores the feature flag to the startup state (if supported by the underlying conf mechanism).
     */
    default void reset() {
        String message = String.format(
                "Current implementation of FeatureFlag: %s does not support reset operation.",
                this.getClass().getSimpleName()
        );
        LoggerFactory.getLogger(FeatureFlag.class).error(message);
        throw new UnsupportedOperationException(message);
    }
}
