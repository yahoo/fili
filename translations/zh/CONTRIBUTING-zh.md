Contributing [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)
============

如果您想给 Fili 贡献代码，谢谢您！我们期待您的参与，无论贡献大小。

我们唯一要求是就是您能够遵循 Fili 的代码风格和一致性。很多代码风格方面的要求检查已经自动化，还有一些其它的管理，我们也
用文档的形式做了说明。[贡献开发指南](/translations/zh/docs/contributing)还在撰写完善中，您也可以去那贡献您的时间！

### 报错 & 添加和完善功能

如果您发现了一个 bug 或者哪个地方不对，请开一个 [issue](https://github.com/yahoo/fili/issues)，提供尽可能详尽的错误来源
和信息，可以的话，请记得加入服务器日志。

如果您想往 Fili 添加一个新的功能或完善某个功能，请开一个 [issue](https://github.com/yahoo/fili/issues)，阐述您想要的
修改。

### 编译

我们的贡献是以 PR 的形式。Fili 是一个多模块软件，您需要[最新版本的 Maven](https://maven.apache.org/download.cgi)。等您
安装好 Maven 之后，就可以编译和运行测试：

```bash
$ git clone git@github.com:yahoo/fili.git
$ cd fili/
$ mvn clean test
```

### 测试 & 代码风格

我们十分注重代码测试，一是为了保证正确性，二是确保所做的修改不会影响现有的代码和功能。我们的测试完全依赖于
[Spock](http://spockframework.org/) 测试框架。该框架简洁，内置强大的[模拟框架](http://spockframework.org/spock/docs/1.1-rc-2/interaction_based_testing.html)，
而且使用的语言是 [Groovy](http://www.groovy-lang.org/)。:smile:

我们力保代码质量，高质量的代码易于维护，理解，bug 更少。为此，我们使用自动化的[风格检查](http://checkstyle.sourceforge.net/)，
里面的规则机制应该能涵盖大多数常见的风格问题。详细的风格检查内容放在[风格设置](checkstyle-style.xml)文件里，
[Google Java 风格指南](https://google.github.io/styleguide/javaguide.html)基本上与其一直。

### 鸣谢

特别感谢 Groovy JsonSlurper 的开发者，让 Fili 的
[排序功能](https://github.com/yahoo/fili/blob/master/fili-core/src/test/java/com/yahoo/bard/webservice/util/JsonSlurper.java)
得以实现。


设计和架构
----------

Fili 是从零开发，具备高扩展性。Fili 的架构设计非常独到，我们做了很多图解，但是还没有放上来。目前，您可以认为我们的架构
还在提升中。
