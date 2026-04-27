FROM tomcat:9-jre8-temurin-noble

## Build arguments
ARG XNAT_VERSION
ARG XNAT_CS_PLUGIN_VERSION
ARG XNAT_BATCH_LAUNCH_PLUGIN_VERSION
ARG XNAT_OHIF_VIEWER_VERSION=3.7.2
ARG XNAT_ROOT=/data/xnat
ARG XNAT_HOME=${XNAT_ROOT}/home
ARG TOMCAT_XNAT_FOLDER_PATH=${CATALINA_HOME}/webapps/ROOT

## Environment variables
# Kept explicit to ensure 'make-xnat-config.sh' has everything it needs
ENV XNAT_HOME=${XNAT_HOME} \
    XNAT_DATASOURCE_DRIVER=org.postgresql.Driver \
    XNAT_DATASOURCE_PASSWORD=xnat \
    XNAT_DATASOURCE_URL=jdbc:postgresql://localhost:5432/xnat \
    XNAT_DATASOURCE_USERNAME=xnat \
    XNAT_HIBERNATE_DIALECT=org.hibernate.dialect.PostgreSQL9Dialect \
    PGPASSWORD=xnat \
    CATALINA_OPTS="-Xms512m -Xmx2g -Dxnat.home=${XNAT_HOME}"

## System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    docker.io \
    postgresql-16 \
    postgresql-client-16 \
    supervisor \
    unzip \
    && rm -rf /var/lib/apt/lists/*

## Prepare filesystem
RUN mkdir -p \
    "${TOMCAT_XNAT_FOLDER_PATH}" "${XNAT_HOME}/config" "${XNAT_HOME}/logs" \
    "${XNAT_HOME}/plugins" "${XNAT_HOME}/work" "${XNAT_ROOT}/archive" \
    "${XNAT_ROOT}/build" "${XNAT_ROOT}/cache" "${XNAT_ROOT}/ftp" \
    "${XNAT_ROOT}/pipeline" "${XNAT_ROOT}/prearchive" \
    /run/postgresql /var/lib/postgresql/data /var/log/tomcat \
    && chown -R postgres:postgres /var/lib/postgresql/data /run/postgresql \
    && rm -rf "${CATALINA_HOME}/webapps/"*

## XNAT and plugins
WORKDIR /tmp
RUN curl -fSL -o xnat.war \
    "https://api.bitbucket.org/2.0/repositories/xnatdev/xnat-web/downloads/xnat-web-${XNAT_VERSION}.war" \
    && unzip -o xnat.war -d ${TOMCAT_XNAT_FOLDER_PATH} \
    && curl -fSL -o ${XNAT_HOME}/plugins/container-service.jar \
    "https://api.bitbucket.org/2.0/repositories/xnatdev/container-service/downloads/container-service-${XNAT_CS_PLUGIN_VERSION}-fat.jar" \
    && curl -fSL -o ${XNAT_HOME}/plugins/batch-launch.jar \
    "https://api.bitbucket.org/2.0/repositories/xnatx/xnatx-batch-launch-plugin/downloads/batch-launch-${XNAT_BATCH_LAUNCH_PLUGIN_VERSION}.jar" \
    && curl -fSL -o ${XNAT_HOME}/plugins/ohif-viewer.jar \
    "https://www.xnat.org/files/ohif-viewer-xnat-plugin/ohif-viewer-${XNAT_OHIF_VIEWER_VERSION}.jar" \
    && rm -rf /tmp/*

## Database initialisation
USER postgres
RUN /usr/lib/postgresql/16/bin/initdb \
    --pgdata=/var/lib/postgresql/data \
    && printf "\ndatasource.driver = '/tmp'\n" >> \
    /var/lib/postgresql/data/postgresql.conf
# hadolint ignore=DL3002
USER root

## Configuration and launch scripts
COPY make-xnat-config.sh /usr/local/bin/
RUN /usr/local/bin/make-xnat-config.sh && rm /usr/local/bin/make-xnat-config.sh

COPY launch-xnat.sh XNAT.sql /
COPY xnat-prefs-init.ini ${XNAT_HOME}/config/prefs-init.ini
COPY supervisord.conf /etc/supervisord.conf

EXPOSE 8080

# Ensures test suite waits until XNAT is responding
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s \
    CMD curl -f http://localhost:8080/data/system/version || exit 1

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
