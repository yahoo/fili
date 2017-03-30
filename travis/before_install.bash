#!/bin/bash

# Load credentials for git when not a PR
if [[ "${TRAVIS_PULL_REQUEST}" == "false" ]]; then
    echo "INFO Setting up git credentials, this is not a PR: ${TRAVIS_PULL_REQUEST}"
    git remote set-url origin git@github.com:yahoo/fili.git
    openssl aes-256-cbc -K ${encrypted_3b73c80ce5a3_key} -iv ${encrypted_3b73c80ce5a3_iv} -in github_key.enc -out github_key -d
    chmod 600 github_key
    eval $(ssh-agent -s)
    ssh-add github_key
fi
