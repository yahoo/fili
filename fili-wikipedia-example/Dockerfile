FROM druidio/example-cluster:latest
LABEL maintainer="https://groups.google.com/forum/#!forum/fili-developers"

# fili github information
ENV GITHUB_OWNER=yahoo BRANCH=master

RUN apt-get update && apt-get install -y --no-install-recommends git

RUN wget -q -O - http://archive.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz | tar -xzf - -C /usr/local \
      && ln -s /usr/local/apache-maven-3.2.5 /usr/local/apache-maven \
      && ln -s -f /usr/local/apache-maven/bin/mvn /usr/local/bin/mvn

# fili code goes here
RUN mkdir -p /usr/local/fili

# update in case of new commits
ADD https://api.github.com/repos/$GITHUB_OWNER/fili/git/refs/heads/$BRANCH \
fili-branch.json
RUN git clone -q --branch ${BRANCH} --depth 1 \
https://github.com/$GITHUB_OWNER/fili.git /usr/local/fili

WORKDIR /usr/local/fili

# install Fili in the container
RUN mvn -U -B install -DskipTests=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip

EXPOSE 9998

RUN chmod +x /usr/local/fili/fili-wikipedia-example/entrypoint.sh
ENTRYPOINT ["/usr/local/fili/fili-wikipedia-example/entrypoint.sh"] 
