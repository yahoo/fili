// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import static com.yahoo.bard.webservice.config.ConfigurationGraph.DEPENDENT_MODULE_KEY
import static com.yahoo.bard.webservice.config.ConfigurationGraph.MODULE_NAME_KEY

import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.MapConfiguration

/**
 * Constants usable for testing in system configuration specifications
 */
class ConfigurationTestUtils {
    // Test module names
    public static final String MODULE_A_NAME = "module-a"
    public static final String MODULE_B_NAME = "module-b"
    public static final String MODULE_C_NAME = "module-c"
    public static final String MODULE_D_NAME = "module-d"
    public static final String MODULE_E_NAME = "module-e"
    public static final String MODULE_F_NAME = "module-f"
    public static final String MODULE_G_NAME = "module-g"
    public static final String MODULE_H_NAME = "module-h"
    public static final String MODULE_J_NAME = "module-j"
    public static final String MODULE_MISSING_CHILD = "module-missing-child"
    public static final String MODULE_NOT_APPEARING = "module-sir-not-appearing"

    // Test module configurations
    // a depends on b and c,  b depends on d, e depends on c, h depends on e then a
    public static final Configuration A_CONFIG = buildModule(MODULE_A_NAME, [MODULE_B_NAME, MODULE_C_NAME])
    public static final Configuration B_CONFIG = buildModule(MODULE_B_NAME, [MODULE_D_NAME])
    public static final Configuration C_CONFIG = buildModule(MODULE_C_NAME, [])
    public static final Configuration D_CONFIG = buildModule(MODULE_D_NAME, [])
    public static final Configuration E_CONFIG = buildModule(MODULE_E_NAME, [MODULE_C_NAME])
    public static final Configuration H_CONFIG = buildModule(MODULE_H_NAME, [MODULE_E_NAME, MODULE_A_NAME])

    // f depends on g, g depends on f, an invalid dependency.  Also, J depends into f
    public static final Configuration F_CONFIG = buildModule(MODULE_F_NAME, [MODULE_G_NAME])
    public static final Configuration G_CONFIG = buildModule(MODULE_G_NAME, [MODULE_F_NAME])
    public static final Configuration J_CONFIG = buildModule(MODULE_J_NAME, [MODULE_F_NAME])

    // missing child depends on never loaded not-appearing
    public static final Configuration MISSING_CHILD_CONFIG = buildModule(MODULE_MISSING_CHILD, [MODULE_NOT_APPEARING])

    public static final Map<Configuration, String> namedConfigurations =
            [(A_CONFIG): "a",
             (B_CONFIG): "b",
             (C_CONFIG): "c",
             (D_CONFIG): "d",
             (E_CONFIG): "e",
             (F_CONFIG): "f",
             (G_CONFIG): "g",
             (H_CONFIG): "h",
             (J_CONFIG): "j",
             (MISSING_CHILD_CONFIG): "missing-child"
            ]

    static Configuration buildModule(String name, List<String> dependencyNames) {
        return new MapConfiguration([(MODULE_NAME_KEY): name, (DEPENDENT_MODULE_KEY): dependencyNames]);
    }
}
