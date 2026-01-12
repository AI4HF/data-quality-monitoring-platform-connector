# Feast Data Quality - Monitoring Platform Connector

This connector includes a one-time Python script that gathers Data Quality results from an onfhir-feast server and forwards them to the monitoring platform.

## Usage

Deploy the Feast server before running the connector.

    git clone https://gitlab.srdc.com.tr/onfhir/onfhir-feast.git

Deploy the Monitoring Platform before running the connector.

    git clone https://github.com/AI4HF/monitoring-platform.git

Once both the Feast server and the Monitoring Platform are running, start the connector:

    docker compose up -d

To run locally, before running the compose, build the required image of the connector (comment out the docker login-tag-push lines if only local deployment is intended):
    
    sh build.sh