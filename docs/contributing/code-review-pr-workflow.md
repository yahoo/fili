Code Review PR Workflow
=======================

Code review through Pull Requests (PRs) is an important aspect of development because it is one of the primary 
communication channels of changes in the codebase to everyone else working on it. Determining the state of a PR, 
however, is not always easy, especially for very active PRs with lots of comments and discussion. This document 
describes how we use GitHub's labels to mark the state of a PR, making it easy to determine it's state at a glance. It 
also describes what goes into making a PR easy to review. (and the easier a PR is to review, the faster it will get 
merged!)

Contents
--------

- [Overview](#code-review-pr-workflow)
- [Contents](#contents)
- [What Makes a Good PR?](#what-makes-a-good-pr)
- [Phases](#phases)
- [Flow](#flow)
- [Flags](#flags)

What Makes a Good PR?
---------------------

The purpose of code review and opening a PR is two-fold: 

1. To change Fili, which is the job of the author of the changes
2. To ensure that the changes in the PR make Fili better, which is the job of the PR reviewers

### Types of Changes

The types of changes made to Fili are usually of a few main types: 

1. **Cleanup**  

   These fall into 3 categories:
   
   1. Stylistic changes that make the code easier to read and better aligned with the rest of the codebase.
   
   2. Refactoring changes that make the code better by reducing duplication or cleaning up classes and abstractions that
      may have had too many responsibilities.
   
   3. Changes to add or improve tests to cover untested behavior or make tests clearer. 
   
2. **Logical**  
   These are changes that add features, functionality, and capabilities that Fili didn't have before, or perhaps change 
   the way existing features, functionality, and capabilities work.

### Organizing Changes

While the bulk of submitted changes are Logical changes, Cleanup changes are very welcome. When making Cleanup changes,
however, following a few small guidelines will ensure that the changes are easy for PR reviewers to review:

1. The changes _should_ make the code better according to the [Fili style guidelines][FiliStyle].
2. The changes _should not_ obscure other changes, especially Logical changes.
   - It's better to have Cleanup changes as a separate PR, or at least as separate commits from commits with Logical
     changes. (Separate PR strongly preferred)
   - It's better to have too many small commits that need to be squashed together instead of too few large commits that
     have overlapping changes. Small commits can be combined more easily than large commits can be split.
      
### Documenting Changes

In some cases, what the changes are and why they are being made are self-evident, but in many cases, it's not clear what
the spirit of the change is. So, to make it easier for reviewers to understand what the changes are, and why they are
being made, please describe the changes in the PR description and CHANGELOG, as well. Please follow the format of the
existing CHANGELOG entry. `Current` changes can be modified in unstable ways and shouldn't be considered stable from a
public API point of view. It's also good to review your own PRs and leave comments where the changes need additional
explanation. Remember, ***it's the author's job to make reviewing changes easy for the reviewers!***

Phases
------

As a PR moves from being opened to being Merged or Closed, there are a number of different states or phases that it will
move through, ending up being either Closed or Merged.

- [Work in Progress](#work-in-progress)
- [Reviewable](#reviewable)
- [Mergeable](#mergeable)
- [Merged*](#merged)
- [Closed*](#closed)

<sub>* denotes built-in GitHub PR states. </sub>

### Work in Progress  

Changes are actively being made to the PR, often in response to feedback from reviewers, or to allow feedback on 
partial work. Feel free to review it, but the code is not considered complete by the author and will likely require 
re-review once the work in progress is completed.

### Reviewable  

The author considers the changes complete and ready for full review.

### Mergeable

Once a PR has met this set of checks, it is Mergeable and is ready to be [Merged*](#merged):

- The changes have been approved by at least 2 reviewers
- The changes are based on the head of the `master` branch
- The commits have been squashed into logical commits

### Merged*

The PR has been merged. This is a built-in GitHub PR state and _should_ coexist with the [Mergeable](#mergeable) state.

### Closed*

The changes will not be merged and the PR should not receive additional attention. Closed PRs can be reopened.

Flow
----

While there are no restrictions on the states, since they are only indicated by labels, this table details the expected 
state transitions from one phase to another.

| Start                                 | End                                                                                  |
|---------------------------------------|--------------------------------------------------------------------------------------|
| -                                     | [Work in Progress](#work-in-progress), [Reviewable](#reviewable), [Closed*](#closed) |
| [Work in Progress](#work-in-progress) | [Reviewable](#reviewable), [Closed*](#closed)                                        |
| [Reviewable](#reviewable)             | [Work in Progress](#work-in-progress), [Mergeable](#mergeable), [Closed*](#closed)   |
| [Mergeable](#mergeable)               | [Merged*](#merged), [Closed*](#closed)                                               |
| [Merged*](#merged)                    | -                                                                                    |
| [Closed*](#closed)                    | [Work in Progress](#work-in-progress), [Reviewable](#reviewable)                     |

Flags
-----

Depending on the changes, what's been happening in the repo outside the PR, and where in the process a PR is, different
flags may be applied to the PR. Some flags indicate aspects of the changes, while other flags indicate things that need
to happen to the commits or changes in the PR before it can be considered [Mergeable](#mergeable).

- [Breakfix](#breakfix)
- [Breaking Change](#breaking-change)
- [Need Announce](#need-announce)
- [Need 2 Reviews](#need-2-reviews)
- [Need 1 Review](#need-1-review)
- [Need Changes](#need-changes)
- [Need Rebase](#need-rebase)
- [Need Squash](#need-squash)

### Breakfix

The changes in the PR are urgently needed to fix a broken release. Broken releases should be fixed as quickly as 
possible, so `Breakfix` PRs should be given higher priority than other PRs.

### Breaking Change

The changes in the PR are backwards-incompatible with customer-facing APIs. These APIs are not currently documented, 
but once they are, they will be linked here. In the mean time, the working list of customer-facing APIs include: 

- Response Formats (including headers)
- Request Formats (including URLs, query string parameters, and headers)
- Configuration (including property names, helper classes, and interfaces)
- Druid Queries
- RequestLog
- Request / Response workflow interfaces (including ResultSetMapper, RequestHandler, and ResponseProcessor interfaces) 
- Feature Flags 

### Need Announce

The changes in the PR are interesting enough to customers that they should be notified of the changes. This includes
changes like:

- Breaking changes
- Significant new features or capabilities
- Known (and hopefully fixed) bugs
- Deprecations

### Need 2 Reviews

2 more approvals are needed before the PR can be considered [Mergeable](#mergeable).

### Need 1 Review

1 more approval is needed before the PR can be considered [Mergeable](#mergeable).

### Need Changes

The reviewers of the PR feel that there are still changes that need to be made before they can approve the PR. Any
reviewer who requests changes in a comment should make sure that this tag is applied to the PR, and the author of the
PR should remove this tag when they believe they have made all of the needed changes.

### Need Rebase

The changes need to be rebased onto the head of the `master` branch before the PR can be considered
[Mergeable](#mergeable).

### Need Squash

The changes need to be squashed into logical commits before the PR can be considered [Mergeable](#mergeable). 

**Note: A PR that has not gotten the 2 approvals it needs should not be squashed.** Any commits made in response to
review  comments should be left on the branch until the review is complete so that it is easy for reviewers to track
changes made in response to their comments. 


[FiliStyle]: https://google.github.io/styleguide/javaguide.html
