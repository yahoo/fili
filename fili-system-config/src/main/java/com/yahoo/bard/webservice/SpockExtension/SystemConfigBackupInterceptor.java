package com.yahoo.bard.webservice.SpockExtension;

import com.yahoo.bard.webservice.config.SystemConfig;

import org.spockframework.runtime.extension.IMethodInterceptor;
import org.spockframework.runtime.extension.IMethodInvocation;

import java.util.HashMap;
import java.util.Map;

public class SystemConfigBackupInterceptor implements IMethodInterceptor {

    private final SystemConfig config;
    private final Map<String, Object> configToRestore;
    private final Map<String, String> propertiesToRestore;

    public String methodName = "";

    public SystemConfigBackupInterceptor(
            SystemConfig config,
            Map<String, Object> configToRestore,
            Map<String, String> propertiesToRestore
    ) {
        this.config = config;
        this.configToRestore = new HashMap<>(configToRestore);
        this.propertiesToRestore = new HashMap<>(propertiesToRestore);
    }

    @Override
    public void intercept(IMethodInvocation invocation) throws Throwable {
        System.out.println("executing in method " + methodName);

        configToRestore.forEach((String key, Object value) -> config.getMasterConfiguration().setProperty(key, value));
        propertiesToRestore.forEach(
                (String key, String value) -> config.getRuntimeProperties().setProperty(key, value)
        );

        invocation.proceed();
    }
}
