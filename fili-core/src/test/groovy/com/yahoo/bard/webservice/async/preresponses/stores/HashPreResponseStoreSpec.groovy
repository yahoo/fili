// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.preresponses.stores

/**
 * Verifies that the HashPreResponseStore satisfies the PreResponseStore interface. The tests may be found in
 * {@link PreResponseStoreSpec}.
 */
class HashPreResponseStoreSpec extends PreResponseStoreSpec {

    @Override
    PreResponseStore getStore() {
        return new HashPreResponseStore()
    }
}
