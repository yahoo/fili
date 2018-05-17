// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import com.yahoo.bard.webservice.logging.RequestLog

class BardQueryInfoUtils {
    /**
     * Constructs and returns a testing BardQueryInfo instance without a query type.
     *
     * @return a testing BardQueryInfo instance without a query type
     */
    static BardQueryInfo initializeBardQueryInfo() {
        resetBardQueryInfo()
        BardQueryInfo bardQueryInfo = new BardQueryInfo("test")
        RequestLog.getId() // initialize RequestLog
        RequestLog.record(bardQueryInfo)
        return bardQueryInfo
    }

    /**
     * Resets counts of all query types in BardQueryInfo.
     */
    static void resetBardQueryInfo() {
        RequestLog.dump()
    }
}
