#!/bin/bash
PUBLISH_WHITELIST="master sd-master"

# Only publish whitelisted branches
echo "Branch: ${GIT_BRANCH} Whitelist: ${PUBLISH_WHITELIST}"
WHITELISTED="false"
for a in ${PUBLISH_WHITELIST}; do
    if [[ ${GIT_BRANCH} = "origin/${a}" ]];
    then
      WHITELISTED="true";
    fi;
done;
echo Whitelisted: $WHITELISTED
[[ ${WHITELISTED} == "true" ]];
