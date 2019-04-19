// Copyright 2019 Verizon Media Group
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates a given schema against a given constraint.
 */
public class SchemaConstraintValidator {


    /**
     * Private constructor.
     */
    private SchemaConstraintValidator() {

    }
    /**
     * Validates a given schema given a constraint.
     *
     * @param constraint  Constraint to validate schema against
     * @param schema  Schema to validate against
     * @return  whether the constraint is valid for the schema
     */
    public static boolean validateConstraintSchema(DataSourceConstraint constraint, PhysicalTableSchema schema) {
        Set<String> tableColumnNames = schema.getColumnNames();
        // Validate that the requested columns are answerable by the current table
        return constraint.getAllColumnNames().stream().allMatch(tableColumnNames::contains);
    }

    /**
     * Log the error from an invalid schema.
     *
     * @param logger  The Logger
     * @param table  The table being constrained
     * @param constraint  The constraints with invalid columns
     */
    public static void logAndThrowConstraintError(Logger logger, PhysicalTable table, DataSourceConstraint constraint) {
        String expected = constraint.getAllColumnNames().stream()
                .filter(name -> !table.getSchema().getColumnNames().contains(name))
                .collect(Collectors.joining(","));
        String message = ErrorMessageFormat.TABLE_SCHEMA_CONSTRAINT_MISMATCH.format(expected, table.getName());
        logger.error(message);
        throw new SchemaInvalidException(message);
    }

    /**
     * A runtime exception to wrap invalid schemas.
     */
    public static class SchemaInvalidException extends IllegalArgumentException {
        /**
         * Constructor.
         *
         * @param message  The message for the exception.
         */
        SchemaInvalidException(String message) {
            super(message);
        }
    }
}
