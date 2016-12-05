// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.config.names.TableName;

import java.util.Objects;

/**
 * YAML configuration for TableName.
 */
public class YamlTableName implements TableName {

    protected String name;

    /**
     * Construct a new table name.
     *
     * @param name the table name
     */
    public YamlTableName(String name) {
        this.name = name;
    }

    @Override
    public String asName() {
        return name;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof YamlTableName)) {
            return false;
        }

        YamlTableName other = (YamlTableName) object;
        return Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return 1 + Objects.hash(name);
    }
}
