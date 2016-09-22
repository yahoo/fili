// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.DeserializationException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class which contains an information of metric value type. This helps to identify the metric
 * value type like int, string and it provides the Class name for the same. If the given class name is
 * null or empty, it defaults to String class.
 */
public class MetricColumnWithValueType extends MetricColumn {

    private static final Logger LOG = LoggerFactory.getLogger(MetricColumnWithValueType.class);
    private final Class classType;

    /**
     * Constructor.
     *
     * @param name   The column name
     * @param className  Class name of metric value
     */
    public MetricColumnWithValueType(String name, String className) {
        super(name);
        if (className == null || className.equals("")) {
            this.classType = null;
        } else {
            try {
                this.classType = Class.forName(className);
            } catch (ClassNotFoundException e) {
                String msg = ErrorMessageFormat.METRIC_VALUE_CLASS_NOT_FOUND.format(className);
                LOG.error(msg, e);
                throw new DeserializationException(msg, e);
            }
        }
    }

    public Class getClassType() {
        return classType;
    }
}
