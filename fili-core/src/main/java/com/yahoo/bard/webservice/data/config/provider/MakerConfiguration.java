// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;

/**
 * Everything you need to know to build a custom MetricMaker.
 */
public class MakerConfiguration {

    protected final String name;
    protected final String className;
    protected final Object[] arguments;

    /**
     * The configuration for a metric maker.
     *
     * @param name  The maker name to regiser
     * @param className  The class name
     * @param arguments  The maker constructor arguments
     */
    public MakerConfiguration(String name, String className, Object[] arguments) {
        this.name = name;
        this.className = className;
        this.arguments = arguments;
    }

    /**
     * Get the the pretty name for the metric maker, e.g. longSum
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * The class name of the metric maker.
     *
     * @return the class name
     */
    public String getClassName() {
        return className;
    }

    /**
     * The arguments to construct the custom metric maker.
     *
     * @return the constructor arguments
     */
    public Object[] getArguments() {
        return arguments;
    }

    /**
     * The class of the metric maker.
     *
     * @return the metric maker class
     */
    public Class<? extends MetricMaker> getMakerClass() {
        try {
            return (Class<? extends MetricMaker>) Class.forName(getClassName());
        } catch (ClassNotFoundException e) {
            throw new ConfigurationError("Unable to instantiate class " + getClassName(), e);
        }
    }
}
