// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import static com.yahoo.bard.webservice.application.ResourceConfigSpec.getBinder
import static com.yahoo.bard.webservice.application.ResourceConfigSpec.getClicker

import org.glassfish.hk2.utilities.Binder

/**
 * A class to Mock ResourceBinding
 */
public class MockingBinderFactory implements BinderFactory {

    public static final String INIT = "init";
    public static final String BUILD_BIND = "build_binder";

    public MockingBinderFactory() {
        getClicker().accept(INIT);
    }

    @Override
    public Binder buildBinder() {
        getClicker().accept(BUILD_BIND);
        return getBinder();
    }

    @Override
    public void afterRegistration(ResourceConfig resourceConfig) {
        getClicker().accept(resourceConfig);
    }
}
