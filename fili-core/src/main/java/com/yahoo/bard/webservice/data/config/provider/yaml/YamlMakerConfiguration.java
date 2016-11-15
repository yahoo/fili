// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.config.provider.MakerConfiguration;
import com.yahoo.bard.webservice.data.config.provider.yaml.serde.YamlArgDeserializer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Objects;

/**
 * Configuration for metric makers.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class YamlMakerConfiguration implements MakerConfiguration {
    @JsonProperty("class")
    public String cls;

    @JsonProperty("args")
    @JsonDeserialize(using = YamlArgDeserializer.class)
    public Object[] args;

    @Override
    public String getClassName() {
        return cls;
    }

    @Override
    public Object[] getArguments() {
        return args;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof YamlMakerConfiguration)) {
            return false;
        }

        YamlMakerConfiguration conf = (YamlMakerConfiguration) other;
        return Objects.equals(cls, conf.cls) &&
                Objects.equals(args, conf.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cls, args);
    }
}
