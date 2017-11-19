模块配置（Module Configuration）
===============================

Fili 有两类配置方式，一个是领域对象（domain object）式配置，包括了度量（Metrics），维度（Dimensions），表格（Tables），
这一类是通过 Java 代码实现；另一个是通过配置属性（properties）。领域对象配置在[其他文档](./configuring-metrics-zh.md)里
有[详细阐述](./setup-zh.md)，这里我们只讨论模块配置的框架。

Fili 的属性配置系统放在了一个[子模块](../../../fili-system-config)里。该系统可扩展，还可以用于 Fili 的其他模块，甚至
是其他软件里，方便其他地方用这个模块来配置自己的属性。属性配置系统模块在代码层面有很多文档可读，所以在这里我们只讨论如何
同它来配置 Fili。


配置源和配置覆盖
----------------

Fili 的配置涉及多处，您也可以覆盖其它的配置，覆盖的好处之一是您可以覆盖一些属性来停用某个功能，或者覆盖一些默认配置来让
您的软件在某个运行环境中顺利运行。

配置源如下，配置读取顺序由"配置优先级"决定，优先级高的配置源会覆盖优先级低的。要加载使用配置文件的话，Fili 必须能够在
Classpath 访问该文件。

| 优先级   | 配置源                              | 备注                                                   |
| -------: | ----------------------------------- | ------------------------------------------------------ |
| （高） 1 | 环境变量                            |                                                        |
|        2 | Java 配置                           |                                                        |
|        3 | `userConfig.properties`<sup>*</sup> | 设置某一非生产环境，例如开发服务器                     |
|        4 | `testApplicationConfig.properties`  | 测试工具专用                                           |
|        5 | `applicationConfig.properties`      | 任何一个应用都必须提供一个这样的配置文件               |
| （低） 6 | `moduleConfig.properties`           | `moduleConfig.properties` 配置文件根据依赖顺序依次生效 |

<sub>* 由于 `userConfig.properties` 通常用在开发过程中关启某个功能，`.gitignore` 默认情况下不会将其纳入版本控制文件
范围</sub>
