// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

/**
 * Scans a package for classes by looking at files in the classpath
 */
@SuppressWarnings("rawtypes")
public class ClassScanner {

    private static final Logger LOG = LoggerFactory.getLogger(ClassScanner.class);
    public static final int MAX_STACK_DEPTH = 10;

    private final String packageName;

    // mocked objects to use as non-null arguments
    private List<Class> classesToSkip;

    /**
     * Create a class scanner for provided base package
     *
     * @param packageName  The base package such as "com.yahoo.bard"
     */
    public ClassScanner(String packageName) {
        this.packageName = packageName;
        this.classesToSkip = new ArrayList<>();
    }

    /**
     * Create a class scanner for provided base package
     *
     * @param packageName  The base package such as "com.yahoo.bard"
     * @param classesToSkip  The classes which should not be tested
     */
    public ClassScanner(String packageName, List<Class> classesToSkip) {
        this.packageName = packageName;
        this.classesToSkip = classesToSkip;
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @return The classes
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public List<Class<?>> getClasses() throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        TreeSet<File> dirs = new TreeSet<>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class<?>> classes = new ArrayList<>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes;
    }

    /**
     * Recursive method used to find all classes in a given directory and subdirs.
     *
     * @param directory  The base directory
     * @param packageName  The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException
     */
    private List<Class<?>> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        if (!directory.exists()) {
            return classes;
        }
        TreeSet<File> files = new TreeSet<>(Arrays.asList(directory.listFiles()));

        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                Class<?> cls;
                cls = Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6));
                // do not return any mocked classes
                if (!classesToSkip.contains(cls)) {
                    classes.add(cls);
                }
            }
        }
        return classes;
    }

    /**
     * arg construction mode
     */
    public enum Args {
        VALUES, // create object with default values
        NULLS,  // create object with NULL values (except primitive types)
    }

    /**
     * Builds an object for provided class
     * @param newClass  class to construct
     * @param mode  argument type
     * @return constructed object
     * @throws InstantiationException
     */
    public <T> T constructObject(Class<T> newClass, Args mode) throws InstantiationException {
        return constructObject(newClass, mode, new Stack<Class>());
    }

    private <T> T constructObject(Class<T> newClass, Args mode, Stack<Class> stack) throws InstantiationException {

        if (Modifier.isAbstract(newClass.getModifiers())) {
            throw new InstantiationException();
        }

        // for Enums just return first one
        if (Enum.class.isAssignableFrom(newClass)) {
            return constructArg(newClass, mode, stack);
        }

        // saves context for the InstantiationException
        IllegalArgumentException cause = null;

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
                    if (annotation.annotationType().isAssignableFrom(NotNull.class)) {
                        argMode = Args.VALUES;
                        notnull = true;
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
        throw (InstantiationException) new InstantiationException().initCause(cause);
    }

    /**
     * Create default value for provided type
     * @param cls  class to construct
     * @param mode  argument type
     * @return constructed object
     * @throws InstantiationException
     */
    @SuppressWarnings({"boxing", "checkstyle:cyclomaticcomplexity"})
    private <T> T constructArg(Class<T> cls, Args mode, Stack<Class> stack) throws InstantiationException {
        Object arg;

        if (Enum.class.isAssignableFrom(cls)) {
            // create Enum by choosing first one
            try {
                Enum<?>[] values = (Enum[]) cls.getMethod("values").invoke(null);
                arg = values[0];
            } catch (ReflectiveOperationException | RuntimeException e) {
                throw (InstantiationException) new InstantiationException().initCause(e);
            }

            // Process primitive types
        } else if (cls == int.class) {
            arg = 1;
        } else if (cls == long.class) {
            arg = 1L;
        } else if (cls == double.class) {
            arg = 1.0;
        } else if (cls == char.class) {
            arg = (char) 1;
        } else if (cls == byte.class) {
            arg = (byte) 1;
        } else if (cls == boolean.class) {
            arg = true;

            // if Args should be NULL set to null
        } else if (mode == Args.NULLS) {
            arg = null;
        } else if (cls.isAssignableFrom(Aggregation.class)) {
            arg = new LongSumAggregation("name", "fieldName");
        } else if (cls == Object[].class) {
            arg = new Object[0];
        } else if (cls.isAssignableFrom(DateTime.class)) {
            arg = new DateTime(20000);
        } else if (cls.isAssignableFrom(Interval.class)) {
            arg = new Interval(1, 2);
        } else if (cls.isAssignableFrom(ReadablePeriod.class)) {
            arg = Days.days(1);
        } else if (cls.isAssignableFrom(TimeGrain.class)) {
            arg = DefaultTimeGrain.DAY;
        } else if (cls.isAssignableFrom(DateTimeZone.class)) {
            arg = DateTimeZone.UTC;
        } else if (Modifier.isAbstract(cls.getModifiers())) {
            arg = constructSubclass(cls, mode, stack);
        } else {
            try {
                arg = cls.newInstance();
            } catch (Throwable e) {
                try {
                    arg = constructObject(cls, mode, stack);
                } catch (Throwable e2) {
                    arg = mockedObjectOrNull(cls);
                    if (arg == null && mode == Args.VALUES) {
                        throw e2;
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        T targ = (T) arg;
        return targ;
    }

    /**
     * Find a subclass to construct for abstract class
     *
     * @param cls  abstract superclass
     * @param mode  argument type
     * @param stack  Stack of classes we've constructed with so far
     *
     * @return subclass instance or null if none available
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private <T> T constructSubclass(Class<T> cls, Args mode, Stack<Class> stack) {
        // See if we're too deep in the stack and probably recursing
        if (stack.size() > MAX_STACK_DEPTH) {
            T obj = mockedObjectOrNull(cls);

            LOG.error(
                "Max stack depth of {} reached when constructing subclass for {}. Returning {}.\n{}",
                MAX_STACK_DEPTH,
                cls.getSimpleName(),
                obj,
                stack
                );
            return obj;
        }

        if (stack.contains(cls)) {
            T obj = mockedObjectOrNull(cls);

            LOG.error(
                "Recursive constructors for subclass for {}. Returning {}.\n{}",
                cls.getSimpleName(),
                obj,
                stack
                );
            return obj;
        }

        stack.push(cls);
        try {
            for (Class<?> subcls : getClasses()) {
                // find a subclass to construct
                if (cls.isAssignableFrom(subcls) && !Modifier.isAbstract(subcls.getModifiers())) {
                    try {
                        @SuppressWarnings("unchecked")
                        T arg = constructObject((Class<T>) subcls, mode, stack);
                        if (arg != null) {
                            return arg;
                        }
                    } catch (InstantiationException e) {
                        LOG.debug("Instantiation exception : {}", e);
                    }
                }
            }
        } catch (ClassNotFoundException | IOException e) {
            // Ignore
        } finally {
            stack.pop();
        }

        return mockedObjectOrNull(cls);
    }

    private <T> T mockedObjectOrNull(Class<T> cls) {
        // scan for any matching mocked argument
        for (Object mockArg : classesToSkip) {
            if (cls.isInstance(mockArg)) {
                @SuppressWarnings("unchecked")
                T arg = (T) mockArg;
                return arg;
            }
        }

        return null;
    }
}
