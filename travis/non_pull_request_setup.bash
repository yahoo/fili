#!/bin/bash
git remote set-url origin git@github.com:yahoo/fili.git
openssl aes-256-cbc -K $encrypted_3b73c80ce5a3_key -iv $encrypted_3b73c80ce5a3_iv -in github_key.enc -out github_key -d
chmod 600 github_key
eval $(ssh-agent -s)
ssh-add github_key
