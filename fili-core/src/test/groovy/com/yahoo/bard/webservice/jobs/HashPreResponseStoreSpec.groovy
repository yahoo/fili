// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.jobs

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
