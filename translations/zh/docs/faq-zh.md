常见问题解答（FAQ）
===

Fili 常见问题。

如何使用存储在 Redis 里的维度（dimensions）？
------------------------------------------

在本地启用 Redis，您需要配置几个属性。 在 `src/test/resources/userConfig.properties` 或者环境变量（environment variables）
里，做如下配置：

- 将 `test__dimension_backend` 的值设为 `redis`
- 将 `test__key_value_store_tests` 的值设为 `redis`


用 IntelliJ 的话，怎样加入 Fili 的代码风格（code styles）？
--------------------------------------------------------

现在我们用 IntelliJ 配置文件的方法设置 Fili 的代码风格。如果您使用 IntelliJ，可以用根代码仓库中的
[Fili-Project-intellij-code-style.xml](../../../Fili-Project-intellij-code-style.xml) 导入这些风格规则。导入之后设置中
`Editor -> Code Style` 会出现新的 Scheme，名称为 “*Fili-Project*”。

还有一种方法，您可以用 jar 里头有的 xml 文件，然后用这个文件里头的配置来设置您的开发环境。
