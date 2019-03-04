package com.yahoo.bard.webservice.SpockExtension;

import org.spockframework.runtime.extension.ExtensionAnnotation;
import org.spockframework.runtime.extension.IAnnotationDrivenExtension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ExtensionAnnotation(RestoreFiliConfigRuntimeExtension.class)
public @interface RestoreFiliSystemConfig {
}
