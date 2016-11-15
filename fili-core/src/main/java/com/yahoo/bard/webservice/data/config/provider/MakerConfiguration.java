// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;

/**
 * A MakerConfiguration provides a means to register custom MetricMakers.
 */
public interface MakerConfiguration {

    /**
     * The class name of the metric maker.
     *
     * @return the class name
     */
    String getClassName();

    /**
     * The arguments to construct the custom metric maker.
     *
     * @return the constructor arguments
     */
    Object[] getArguments();

    /**
     * The class of the metric maker.
     *
     * FIXME: Clean up exception type
     *
     * @return the metric maker class
     */
    default Class<? extends MetricMaker> getMakerClass() {
        try {
            // Unchecked cast
            return (Class<? extends MetricMaker>) Class.forName(getClassName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
