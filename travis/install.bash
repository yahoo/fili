#!/bin/bash
if [[ "${TRAVIS_PULL_REQUEST}" == "false" ]]; then
  git checkout ${TRAVIS_BRANCH}
fi
