// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * Serializer a Java object into json file.
 */
public class ExternalConfigSerializer {

    ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param mapper object mapper
     */
    public ExternalConfigSerializer(ObjectMapper mapper) {
        this.objectMapper = mapper;
    }

    /**
     * Parse object to a json file.
     *
     * @param object object to be parsed
     * @param path json file's path
     */
    public void parse(Object object, String path) {
        try {
            objectMapper.writeValue(new File(path), object);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
