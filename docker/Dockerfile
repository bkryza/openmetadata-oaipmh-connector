# Extends the official OpenMetadata ingestion image with the OAI-PMH connector.
# The base tag should match the OpenMetadata server version you are running.
ARG OM_VERSION=2.0.9
FROM openmetadata/ingestion:${OM_VERSION}

WORKDIR /opt/airflow/openmetadata-oaipmh-connector
USER airflow

# Copy package sources and example configs
COPY --chown=airflow:0 pyproject.toml ./
COPY --chown=airflow:0 src ./src
COPY --chown=airflow:0 configs ./configs

# Install without upgrading any shared OpenMetadata dependencies
RUN pip install --no-deps .
