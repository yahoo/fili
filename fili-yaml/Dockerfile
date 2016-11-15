# See: https://hub.docker.com/_/jetty/ for usage
FROM jetty

MAINTAINER jtolar@yahoo-inc.com

ADD run-jetty.sh /run-jetty.sh
RUN chmod 755 /run-jetty.sh
ENTRYPOINT ["/run-jetty.sh"]

COPY target/fili-yaml.war /var/lib/jetty/webapps/ROOT.war
