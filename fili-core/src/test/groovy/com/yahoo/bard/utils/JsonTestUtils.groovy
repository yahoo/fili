// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode

import java.util.stream.StreamSupport

class JsonTestUtils {

    /**
     * Checks if the given ArrayNode contains the given json node.
     *
     * @param arrayNode The array node to iterate over
     * @param toCheck The json node being tested against
     * @return whether or not any element of the array node is equivalent to the json node
     */
    static boolean contains(ArrayNode arrayNode, JsonNode toCheck) {
        StreamSupport.stream(arrayNode.spliterator(), false).anyMatch({ (it == toCheck) })
    }
}
