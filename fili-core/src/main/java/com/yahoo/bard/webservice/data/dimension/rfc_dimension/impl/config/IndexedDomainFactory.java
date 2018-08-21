// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.config;

import com.yahoo.bard.webservice.data.dimension.rfc_dimension.IndexedDomain;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.keystores.ByteArrayKeyValueStore;

import java.util.Optional;

// Can close over SearchProviderFactory, and all the shared state of constructing IndexedDomains
public interface IndexedDomainFactory  {

    Optional<IndexedDomain> getInstance(String name);

    IndexedDomain getInstance(String name, ByteArrayKeyValueStore keyValueStore);
}
