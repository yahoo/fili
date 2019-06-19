// Copyright 2019, Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Contains a collection of static methods to make it easier to perform configuration validation.
 */
public class LuthierValidationUtils {

    public static final String MISSING_FIELD_ERROR = "%s '%s': Missing required field '%s'";

    /**
     * Validates that the given field actually exists. If it doesn't, throws a useful error message.
     *
     * @param fieldValue The value to check for existence
     * @param configEntityType  The type of the config entity whose field we're validating
     * @param configEntityName The name of the config entity we're currently building
     * @param fieldName  The name of the field whose value we're validating
     */
    public static void validateField(
            JsonNode fieldValue,
            String configEntityType,
            String configEntityName,
            String fieldName
    ) {
        if (fieldValue == null) {
            throw new LuthierFactoryException(
                    String.format(MISSING_FIELD_ERROR, configEntityType, configEntityName, fieldName)
            );
        }
    }


    /**
     * Validates that the given field actually exists. If it doesn't, throws a useful error message.
     *
     * @param fieldValue The value to check for existence
     * @param configEntityType  The type of the config entity whose field we're validating
     * @param configEntityName The name of the config entity we're currently building
     * @param fieldName  The name of the field whose value we're validating
     */
    public static void validateField(
            JsonNode fieldValue,
            ConceptType configEntityType,
            String configEntityName,
            String fieldName
    ) {
        if (fieldValue == null) {
            throw new LuthierFactoryException(
                    String.format(MISSING_FIELD_ERROR, configEntityType.getConceptKey(), configEntityName, fieldName)
            );
        }
    }


    /** Just a bunch of static functions. **/
    private LuthierValidationUtils() {
    }
}
