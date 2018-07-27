// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.metric;

import com.yahoo.slurper.webservice.data.config.JsonObject;

/**
 * Metric object for parsing to json.
 */
public class MakerObject extends JsonObject {

    private final String name;
    private final String classPath;

    /**
     * Construct a maker config instance.
     *
     * @param makerName The name of the maker.
     * @param makerClass The class path of the maker.
     */
    public MakerObject(
            String makerName,
            String makerClass
    ) {
        this.name = makerName;
        this.classPath = makerClass;
    }

    /**
     * Gets the name of the maker.
     *
     * @return the name of the maker
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the class path of the maker.
     *
     * @return the class path of the maker
     */
    public String getClassPath() {
        return classPath;
    }
}
