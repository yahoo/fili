// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.application.luthier.LuthierConfigNodeLuaJ;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A supplier for config that loads the configuration directly from a Lua script on the classpath.
 */
public class LuaJNodeSupplier implements LuthierSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(LuaJNodeSupplier.class);

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final String LUTHIER_SCRIPT_KEY = SYSTEM_CONFIG.getPackageVariableName("luthier_script");

    private static final String APP_KEY = SYSTEM_CONFIG.getPackageVariableName("luthier_app");

    private static final String LOAD_FAILURE = "Can't load resource: '%s' from configuration script '%s'";

    private final String resourceName;

    private LuaValue configurationTable;

    private static final Globals GLOBALS = JsePlatform.standardGlobals();

    /**
     * Constructor.
     *
     * @param resourceName  resource name for config file
     */
    public LuaJNodeSupplier(String resourceName) {
        this.resourceName = resourceName;
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public LuthierConfigNode get() {
        if (resourceName == null) {
            return null;
        }
        if (configurationTable == null) {
            String luaScript = SYSTEM_CONFIG.getStringProperty(LUTHIER_SCRIPT_KEY, "config.lua");
            try {
                configurationTable = GLOBALS
                        .loadfile(luaScript)
                        .call()
                        .call(SYSTEM_CONFIG.getStringProperty(APP_KEY, "app"));
            } catch (LuaError e) {
                String message = String.format(LOAD_FAILURE, resourceName, luaScript);
                throw new LuthierFactoryException(message, e);
            }
        }
        LuthierConfigNode node = new LuthierConfigNodeLuaJ(configurationTable.get(resourceName));
        return node;
    }
}
