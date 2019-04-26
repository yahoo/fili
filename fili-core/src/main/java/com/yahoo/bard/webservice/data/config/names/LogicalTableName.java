// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.table.LogicalTable;

import org.joda.time.ReadablePeriod;

import java.util.Objects;
import java.util.Optional;

/**
 * An interface for describing a Logical Tables core metadata.
 */
public interface LogicalTableName extends TableName {

    /**
     * A human friendly name used for display and labeling.
     *
     * @return a name
     */
    default String getLongName() {
        return asName();
    }

    /**
     * A category for grouping logical tables in the system.
     *
     * @return a category
     */
    default String getCategory() {
        return LogicalTable.DEFAULT_CATEGORY;
    }

    /**
     * The period that this logical table retains facts for.
     *
     * @return a Joda ReadablePeriod
     */
    default Optional<ReadablePeriod> getRetention() {
        return Optional.empty();
    }

    /**
     * A description of the meaning and function of this table.
     *
     * @return a textual description for the meaning of the table.
     */
    default String getDescription() {
        return asName();
    }

    /**
     * Build a default LogicalTableName from a string name.
     *
     * @param name  The name to be used for description and long name
     *
     * @return  A singleton implementation of LogicalTableName.
     */
    static LogicalTableName forName(TableName name) {
        return forName(name.asName());
    }

    /**
     * Build a default LogicalTableName from a string name.
     *
     * @param name  The name to be used for description and long name
     *
     * @return  A singleton implementation of LogicalTableName.
     */
    static LogicalTableName forName(String name) {

        return new LogicalTableName() {
            @Override
            public String asName() {
                return name;
            }

            @Override
            public int hashCode() {
                return Objects.hash(asName(), getLongName(), getDescription(), getCategory(), getRetention());
            }

            @Override
            public boolean equals(Object that) {
                if (!(that instanceof LogicalTableName)) {
                    return false;
                }

                LogicalTableName tableName = (LogicalTableName) that;

                return Objects.equals(name, tableName.asName())
                        && Objects.equals(name, tableName.getLongName())
                        && Objects.equals(getCategory(), tableName.getCategory())
                        && Objects.equals(name, tableName.getDescription())
                        && Objects.equals(getRetention(), tableName.getRetention())
                        ;
            }
        };
    }
}
