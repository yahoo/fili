package com.yahoo.bard.webservice.SpockExtension;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.spockframework.runtime.extension.IAnnotationDrivenExtension;
import org.spockframework.runtime.extension.IMethodInterceptor;
import org.spockframework.runtime.model.FeatureInfo;
import org.spockframework.runtime.model.FieldInfo;
import org.spockframework.runtime.model.MethodInfo;
import org.spockframework.runtime.model.SpecInfo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RestoreFiliConfigRuntimeExtension implements IAnnotationDrivenExtension<RestoreFiliSystemConfig> {
    @Override
    public void visitSpecAnnotation(RestoreFiliSystemConfig annotation, SpecInfo spec) {
    }

    @Override
    public void visitFeatureAnnotation(RestoreFiliSystemConfig annotation, FeatureInfo feature) {
        SystemConfig config = SystemConfigProvider.getInstance();
        Map<String, Object> backedUpConfig = new HashMap<>();
        Iterator<String> keyIter = config.getMasterConfiguration().getKeys();
        while (keyIter.hasNext()) {
            String key = keyIter.next();
            backedUpConfig.put(key, config.getMasterConfiguration().getProperty(key));
        }

        Map<String, String> backedUpProperties = config.getRuntimeProperties().stringPropertyNames().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        propName -> config.getRuntimeProperties().getProperty(propName)
                ));
        IMethodInterceptor interceptor = new SystemConfigBackupInterceptor(config, backedUpConfig, backedUpProperties);
        ((SystemConfigBackupInterceptor) interceptor).methodName = feature.getName();
//        feature.addInterceptor(interceptor);
        feature.getSpec().addCleanupInterceptor(interceptor);
    }

    @Override
    public void visitFixtureAnnotation(RestoreFiliSystemConfig annotation, MethodInfo fixtureMethod) {

    }

    @Override
    public void visitFieldAnnotation(RestoreFiliSystemConfig annotation, FieldInfo field) {

    }

    @Override
    public void visitSpec(SpecInfo spec) {

    }
}
