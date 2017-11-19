代码审核
========

用 Pull Requests（PRs）进行代码审核是开发中的重要一项，所有 Fili 的开发者要知道代码库的修改，这是最主要的一种交流方式。
然而管理 PR，理解一个 PR 的进度则比较复杂，特别是审核当中出现了很多评论和讨论的 PR。该文档阐述我们如何使用 GitHub 标签
（labels）来标注某个 PR 的进度，让审核人一眼就能对 PR 的进度有一个准确的了解。文档还涉及如何让 PR 变得更加容易审核。（
PR 越容易审核，越能尽快 merge！）

内容
----

- [概述](#代码审核)
- [内容](#内容)
- [什么样的PR是符合要求的？](#什么样的PR是符合要求的？)
- [阶段](#阶段)
- [流程](#流程)
- [状态](#状态)

什么样的PR是符合要求的？
-----------------------

开一个 PR 让人去审核的目的有两个：

1. 修改 Fili，这是修改代码的作者必须做的事
2. 确保 PR 所做的修改能够让 Fili 变得更完善，这是审核人的任务

### 修改类型

修改 Fili 代码主要分为以下几类：

1. **清理改善**  

   这类修改分为三类：
   
   1. 文体修改（Stylistic changes），让代码变得更加可读，更加契合整个代码库。
   
   2. 重构代码（Refactoring changes），减少代码重复，或是简化功能繁杂的类（classes）和抽象类（abstractions）。
   
   3. 增改测试，以补漏测试和改善测试。
   
2. **功能**  
   添加 Fili 的新功能，或者修改现有的功能。

### 修改划分

虽然大部分的修改是功能上的修改，清理改善类的修改一样重要。不过对于后一类修改，有些方法可以简化加速审核：

1. 修改遵循 [Fili 代码风格][FiliStyle].
2. 某次修改不能影响其他的修改，特别是功能性修改。
   - 此类修改最好单独作为一个 PR，至少要和功能性 PR 分开。一个 PR 只做一件事
   - 尽量将代码细分成一个个小的独立 commits，大的 commits 会有重叠，比较难区分，小的 commets 则比较容易 squashed。
      
### 记录修改

虽然某些情况下，修改和修改原因很直观，但是并非所有情况都如此，为了简化加速审核，帮助审核人了解为什么和做了什么样的修改，
请在 PR 描述栏和 CHANGELOG 里介绍这些信息。请务必遵循现有的 CHANGELOG 条目的格式。`Current` 加进去的修改可以是非发布性
的修改，可能不是最终 API 层面需要了解的修改。还有一个好的习惯，那就是自审 PRs，在需要解释的地方自己写一些 comments。总的
来说，***提交 PR 的人须要帮助审核人简化审核流程***

阶段
----

PR 从开设到合并（Merged）或关闭，会经过几个状态阶段，最终才会合并或关闭。

- [未完成（Work in Progress）](#未完成)
- [待审核（Reviewable）](#待审核)
- [可以合并（Mergeable）](#可以合并)
- [已合并（Merged）*](#已合并)
- [已关闭（Closed）*](#已关闭)

<sub>* 代表 GitHub 设置的 PR 状态标签。</sub>

### 未完成（Work in Progress）

PR 还有未上传的修改，可能还在处理审核人提出的修改。这个时候 PR 还是可以审核，但是因为代码不完全，所以需要之后再次审核。

### 待审核（Reviewable）

PR 已经上传了所有的修改，可以审核了。

### 可以合并（Mergeable）

PR 通过审核要求之后，就可以随时[合并*](#已合并)了：

- 至少有两个审核人批准修改
- 当前修改的母分支是最新的 `master` 分支
- commits 压缩（squashed）成一个或多个逻辑单元

### 已合并*

PR 已经合并。此标签是 GitHub 设置的，需要和["可以合并"标签](#可以合并)同时存在。

### 已关闭*

PR 不会再被合并，所有人停止审核。关闭后的 PR 日后有需要，可以重新被开设。

流程
----

RP 状态是人标注的，没有什么硬性机制，这里是状态切换的一个参照表。

| 起                                    | 终                                                                                                      |
|---------------------------------------|---------------------------------------------------------------------------------------------------------|
| -                                     | [未完成（Work in Progress）](#未完成), [待审核（Reviewable）](#待审核), [已关闭（Closed）*](#已关闭)    |
| [未完成（Work in Progress）](#未完成) | [待审核（Reviewable）](#待审核), [已关闭（Closed）*](#已关闭)                                           |
| [待审核（Reviewable）](#待审核)       | [未完成（Work in Progress）](#未完成), [可以合并（Mergeable）](#可以合并), [已关闭（Closed）*](#已关闭) |
| [可以合并（Mergeable）](#可以合并)    | [已合并（Merged）*](#已合并), [已关闭（Closed）*](#已关闭)                                              |
| [已合并（Merged）*](#已合并)          | -                                                                                                       |
| [已关闭（Closed）*](#已关闭)          | [未完成（Work in Progress）](#未完成), [待审核（Reviewable）](#待审核)                                  |

状态
----

根据修改本身的特点，Fili repo 的情况，和 PR 的进展情况，我们可以给 PR 打上不同的状态标签。有的标签是关于修改性质的，有些
是需要在[合并（Mergeable）](#可以合并)之前要做的改动。

- [Breakfix](#breakfix)
- [Breaking Change](#breaking-change)
- [Need 2 Reviews](#need-2-reviews)
- [Need 1 Review](#need-1-review)
- [Need Changes](#need-changes)
- [Need Rebase](#need-rebase)
- [Need Squash](#need-squash)

### Breakfix

该 PR 做的修改急需用来修复已发布版本中的一个 bug。有 bug 的发布版本要尽快修复，所以带有 `Breakfix` 标签的需要首先被审核。

### Breaking Change

该 PR 做的修改会修改用户使用的 API，这些修改还没来得及放入 API 文档，一旦放入，这个 PR 要附上文档相关位置的链接。为了
便于查找，以下是用户使用的 API：

- 响应格式，包括 headers
- 请求格式，包括 URL，查询语句参数，和 headers
- 配置，包括变量名称，辅助类（helper classes）,interfaces
- Druid 查询语句
- 请求日志（RequestLog）
- 请求和相应处理流程的 interfaces，包括 ResultSetMapper，RequestHandler，ResponseProcessor interfaces
- 功能开关（Feature Flags）

### Need 2 Reviews

PR [可以合并](#可以合并)之前需要至少两个审核批准。

### Need 1 Review

PR [可以合并](#可以合并)之前还需要一个审核批准。

### Need Changes

审核人认为批准之前还需要做一些修改。该审核人在此情况下需要给这个 PR 打上这个标签，提交 PR 的人在完成修改之后应该将此标签
移除。

### Need Rebase

所做的修改被[合并](#可以合并)之前需要被 rebased 到最新的 `master` 上。

### Need Squash

所做的修改被[合并](#可以合并)之前需要被 squash 成独立地逻辑 commit 单元。

**注意！没有拿到两个审核批准的 PR 不能被 squash。** 处理审核人修改要求的 commits 在审核通过之前需要留着，便于审核人跟进
要求之后的修改。


[FiliStyle]: https://google.github.io/styleguide/javaguide.html
