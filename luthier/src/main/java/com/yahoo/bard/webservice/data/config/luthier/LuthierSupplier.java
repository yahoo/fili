// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;

import java.util.function.Supplier;

/**
 * A Supplier that loads configuration from a file into Fili. In addition to the standard
 * Supplier interface, this interface also provides access to the name of the resource it is loading.
 */
public interface LuthierSupplier extends Supplier<LuthierConfigNode> {

    /**
     * The name of the resource this supplier is loading.
     *
     * @return The name of the resource this supplier is loading, a list of possible values can be
     * found in {@link com.yahoo.bard.webservice.data.config.luthier.ConceptType}, though customers
     * may have their own.
     */
    String getResourceName();
}
