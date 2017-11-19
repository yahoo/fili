Troubleshooting
===============

如果您无法编译 Fili，或者测试无法运行，您可以尝试一下方法。

Maven 在 Mac 无法正常运行
-------------------------

Maven 有时会选择错误的 Java 版本。您运行 `mvn --version` 可能会看到 Java 是 1.7 或者其它更早的版本，不是您想用的 1.8。
Fili 是要求使用 Java 1.8 的。

解决办法是，您的 `JAVA_HOME` 环境变量需要被 export。方法是将次放到您的 `~/.bash_profile` 文件里：
 
```bash
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```
<sub>注意：在此之前请确保 Java 1.8 已经安装</sub>

除此之外，为了保证连上 Fili 的下载源 `yahoo.bintray.com/maven`，您需要将 [settings.xml](settings.xml) 文件复制到
`~/.m2/`。

代码下载下来后，测试无法运行
----------------------------

Mac 上最直接的运行测试的方法就是使用内存式维度存储和内存式键值存储：

```bash
mvn -Dtest__dimension_backend=memory -Dtest__key_value_store_tests=memory clean test
```

一般情况下，可以忽略此处理，因为内存式维度存储是测试默认的，但也有例外，某些非内存式存储的维度需要 Redis。
