// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

/**
 * Scans a package for classes by looking at files in the classpath.
 */
@SuppressWarnings("rawtypes")
public class ClassScanner {

    private static final Logger LOG = LoggerFactory.getLogger(ClassScanner.class);
    public static final int MAX_STACK_DEPTH = 10;

    private final String packageName;

    // Computed values used to build instances for testing
    private final Map<Class, Object> argumentValueCache = new HashMap<>();

    /**
     * Create a class scanner for provided base package.
     *
     * @param packageName  The base package such as "com.yahoo.bard"
     */
    public ClassScanner(String packageName) {
        this(packageName, new ArrayList<>());
    }

    /**
     * Create a class scanner for provided base package.
     *
     * @param packageName  The base package such as "com.yahoo.bard"
     * @param cacheValues  Values to cache for use in object construction
     */
    public ClassScanner(String packageName, Collection<Class> cacheValues) {
        this.packageName = packageName;
        putPrimitivesInValueCache();
        putInArgumentValueCache(cacheValues);
    }

    /**
     * Precompute all primitives.
     */
    private void putPrimitivesInValueCache() {
        putInArgumentValueCache(int.class, 1);
        putInArgumentValueCache(long.class, 1L);
        putInArgumentValueCache(double.class, 1.0);
        putInArgumentValueCache(float.class, 1.0);
        putInArgumentValueCache(char.class, (char) 1);
        putInArgumentValueCache(byte.class, (byte) 1);
        putInArgumentValueCache(boolean.class, true);
        putInArgumentValueCache(Object[].class, new Object[0]);
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @return The classes found under the class loader
     *
     * @throws IOException if a problem is encountered loading the resources from the package
     * @throws ClassNotFoundException if we're not able to find classes for the package in the directory
     */
    public List<Class<?>> getClasses() throws IOException, ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        TreeSet<File> dirs = new TreeSet<>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }

        List<Class<?>> classes = new ArrayList<>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes;
    }

    /**
     * Recursive method used to find all classes in a given directory and subdirectories.
     *
     * @param directory  The base directory
     * @param packageName  The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException if a class in the directory cannot be loaded
     */
    private List<Class<?>> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        if (!directory.exists()) {
            return classes;
        }

        Iterable<File> files = new TreeSet<>(Arrays.asList(directory.listFiles()));
        for (File file : files) {
            String name = file.getName();
            if (file.isDirectory()) {
                assert !name.contains(".");
                // Extend the package and recurse
                classes.addAll(findClasses(file, packageName + "." + name));
            } else if (name.endsWith(".class")) {
                // Grab just the class name, stripping the .class extension
                name = packageName + '.' + name.substring(0, name.length() - 6);
                classes.add(Class.forName(name));
            }
        }
        return classes;
    }

    /**
     * arg construction mode.
     */
    public enum Args {
        VALUES, // create object with default values
        NULLS,  // create object with NULL values (except primitive types)
    }

    /**
     * Builds an object for provided class.
     *
     * @param newClass  class to construct
     * @param mode  argument type
     * @param <T>  The type of the class we're trying to build, so that we can return it typesafely
     *
     * @return constructed object
     * @throws InstantiationException if the class cannot be instantiated
     */
    public <T> T constructObject(Class<T> newClass, Args mode) throws InstantiationException {
        return constructObject(newClass, mode, new Stack<>());
    }

    /**
     * Construct the given object.
     *
     * @param newClass  Class to try to instantiate
     * @param mode  The way we want to try to construct the object (the args to use)
     * @param stack  The classes we've been trying to build up the reference chain
     * @param <T>  The type of the class we're trying to build, so that we can return it typesafely
     *
     * @return  The instantiated class
     * @throws InstantiationException if we have trouble instantiating it
     */
    private <T> T constructObject(Class<T> newClass, Args mode, Stack<Class> stack)
            throws InstantiationException {

        // We cannot instantiate an abstract class
        if (Modifier.isAbstract(newClass.getModifiers())) {
            throw new InstantiationException();
        }

        // for Enums just return first one
        if (Enum.class.isAssignableFrom(newClass)) {
            return constructArg(newClass, mode, stack);
        }

        // saves context for the InstantiationException
        IllegalArgumentException cause;

        // Try a no arg constructor first
        try {
            return newClass.newInstance();
        } catch (Throwable e) {
            /* Ignore, try using constructors */
            cause = new IllegalArgumentException(newClass + ":" + mode, e);
        }

        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) newClass.getDeclaredConstructors();

        // Loop over constructor list until we succeed in creating object
        constructor: for (Constructor<T> constructor : constructors) {

            // Loop over argument types to construct args
            Class<?>[] argClass = constructor.getParameterTypes();
            Object[] args = new Object[argClass.length];
            for (int i = 0; i < argClass.length; i++) {

                // convert known abstract types to real classes types
                Class<?> cls = argClass[i];

                if (cls.isAssignableFrom(HashSet.class)) {
                    cls = HashSet.class;
                } else if (cls.isAssignableFrom(ArrayList.class)) {
                    cls = ArrayList.class;
                } else if (cls.isAssignableFrom(HashMap.class)) {
                    cls = HashMap.class;
                } else if (cls.isAssignableFrom(Object[].class)) {
                    cls = Object[].class;
                }

                // do not pass NULL into @NotNull annotated parameter
                boolean notnull = false;
                Args argMode = mode;
                for (Annotation annotation : constructor.getParameterAnnotations()[i]) {
                    if (NotNull.class.isInstance(annotation)) {
                        argMode = Args.VALUES;
                        notnull = true;
                        break;
                    }
                }

                Object arg = (cls == newClass ? null : constructArg(cls, argMode, stack));

                // cannot pass null to @NotNull arg.  Do not use this constructor, try next
                if (notnull && arg == null) {
                    continue constructor;
                }

                args[i] = arg;
            }

            // Create object from args
            try {
                constructor.setAccessible(true);
                return constructor.newInstance(args);
            } catch (Throwable e) {
                /* Ignore, try next constructor. Save exception context */
                cause = new IllegalArgumentException(constructor + ":" + mode + ":" + Arrays.asList(args), e);
            }
        }

        // Exhausted all constructors, give up
        throw (InstantiationException) new InstantiationException(cause.getMessage()).initCause(cause);
    }

    /**
     * Create default value for provided type.
     *
     * @param cls  class to construct
     * @param mode  argument type
     * @param stack  Stack of objects we've been trying to build values for
     * @param <T>  Type of class we're creating a default value for
     *
     * @return constructed object
     * @throws InstantiationException if the class cannot be instantiated
     */
    @SuppressWarnings({"boxing", "checkstyle:cyclomaticcomplexity", "unchecked"})
    private <T> T constructArg(Class<T> cls, Args mode, Stack<Class> stack) throws InstantiationException {
        T arg = null;
        T cachedValue = getCachedValue(cls);

        if (cls.isPrimitive()) {
            arg = cachedValue;
        }  else if (Enum.class.isAssignableFrom(cls)) {
            // create Enum by choosing first one
            try {
                Enum<?>[] values = (Enum<?>[]) cls.getMethod("values").invoke(null);
                arg = (T) values[0];
            } catch (ReflectiveOperationException | RuntimeException e) {
                throw (InstantiationException) new InstantiationException().initCause(e);
            }
            // if Args should be NULL set to null
        } else if (mode == Args.NULLS) {
            arg = null;
            // Otherwise use precomputed values if available
        } else if (cachedValue != null) {
            arg = cachedValue;
        } else if (cls == Object[].class) {
            arg = (T) new Object[0];
        } else if (cls.isArray()) {
            Object arrayElement = constructArg(cls.getComponentType(), mode, stack);
            arg = (T) Array.newInstance(cls.getComponentType(), 1);
            Array.set(arg, 0, arrayElement);
        } else if (Modifier.isAbstract(cls.getModifiers())) {
            arg = constructSubclass(cls, mode, stack);
        } else {
            try {
                arg = cls.newInstance();
            } catch (Throwable ignored) {
                try {
                    arg = constructObject(cls, mode, stack);
                } catch (Throwable e2) {
                    if (mode == Args.VALUES) {
                        throw e2;
                    }
                }
            }
        }
        if (arg != null) {
            putInArgumentValueCache(arg.getClass(), arg);
        }
        return arg;
    }

    /**
     * Find a subclass to construct for abstract class.
     *
     * @param cls  abstract superclass
     * @param mode  argument type
     * @param stack  Stack of classes we've constructed with so far
     * @param <T>  Type of class to construct
     *
     * @return subclass instance or null if none available
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private <T> T constructSubclass(Class<T> cls, Args mode, Stack<Class> stack) {
        // Find the precomputed value, if any
        T cachedValue = getCachedValue(cls);

        // See if we're too deep in the stack and probably recursing
        if (stack.size() > MAX_STACK_DEPTH) {

            LOG.error(
                    "Max stack depth of {} reached when constructing subclass for {}. Returning {}.\n{}",
                    MAX_STACK_DEPTH,
                    cls.getSimpleName(),
                    cachedValue,
                    stack
            );
            return cachedValue;
        }

        if (stack.contains(cls)) {
            LOG.error(
                    "Recursive constructors for subclass for {}. Returning {}.\n{}",
                    cls.getSimpleName(),
                    cachedValue,
                    stack
            );
            return cachedValue;
        }

        stack.push(cls);
        try {
            for (Class<?> subclass : getClasses()) {
                // find a subclass to construct
                if (cls.isAssignableFrom(subclass) && !Modifier.isAbstract(subclass.getModifiers())) {
                    try {
                        T arg = constructObject(subclass.asSubclass(cls), mode, stack);
                        if (arg != null) {
                            return arg;
                        }
                    } catch (InstantiationException e) {
                        LOG.trace("Instantiation exception : {}", e);
                    }
                }
            }
        } catch (ClassNotFoundException | IOException ignore) {
            // Ignore
        } finally {
            stack.pop();
        }

        return cachedValue;
    }

    /**
     * Get a cached value if available.
     *
     * @param cls  Class to search for a mock of
     * @param <T>  Type of class, so that we can return it in a typesafe way
     *
     * @return a mock if we have one, otherise just null
     */
    private <T> T getCachedValue(Class<T> cls) {
        return argumentValueCache.keySet().stream()
                .filter(cls::isAssignableFrom)
                .findFirst()
                .map(argumentValueCache::get)
                .map(StreamUtils.uncheckedCast(cls))
                .orElse(null);
    }

    /**
     * Store a value in the argument value cache, keyed by its class.
     *
     * @param values  The value being cached
     */
    public void putInArgumentValueCache(Collection<?> values) {
        values.forEach(value -> putInArgumentValueCache(value.getClass(), value));
    }

    /**
     * Store a value in the argument value cache, keyed by its class.
     *
     * @param cls  The class to associate with the object
     * @param value  The value being cached
     */
    public void putInArgumentValueCache(Class cls, Object value) {
        argumentValueCache.put(cls, value);
    }
}
