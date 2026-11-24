# OpenMetadata OAI-PMH Connector

An [OpenMetadata](https://docs.open-metadata.org) ingestion connector that harvests
metadata from any [OAI-PMH](https://www.openarchives.org/pmh/) endpoint and loads it
into an OpenMetadata server as a browseable, searchable catalog.

## How records are mapped

| OAI-PMH concept | OpenMetadata entity |
|---|---|
| OAI-PMH endpoint | `DatabaseService` (type: CustomDatabase) |
| Repository (from `Identify`) | `Database` |
| OAI-PMH set | `DatabaseSchema` (when `useSets=true`) |
| OAI-PMH record | `Table` (title as display name, abstract as description) |
| Metadata fields | Fixed columns on every table |

Each harvested record becomes a `Table` whose columns represent the metadata
schema (identifier, title, creator, description, publisher, etc.).  Deleted records
are silently skipped.

## Supported metadata formats

| `metadataPrefix` | Format |
|---|---|
| `oai_dc` | Dublin Core (baseline, supported by all OAI-PMH servers) |
| `oai_datacite` | DataCite kernel-4 wrapped in the `oai-1.1` envelope |

## Prerequisites

- Python 3.10, 3.11, or 3.12
- A running OpenMetadata **2.0** server
- The ingestion-bot JWT token from the OpenMetadata UI
  (`Settings → Bots → ingestion-bot → Token`)

---

## Option A — Run ingestion locally (outside Docker)

### 1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install the connector

```bash
pip install -e .
```

### 3. Set environment variables

```bash
export OPENMETADATA_HOST="http://localhost:8585/api"
export OPENMETADATA_JWT_TOKEN="<paste-your-ingestion-bot-token>"
```

### 4. Run ingestion

```bash
# Onedata demo endpoint (oai_datacite format)
metadata ingest -c configs/oaipmh-onedata-demo.yaml

# Generic template — edit configs/oaipmh-generic-template.yaml first
metadata ingest -c configs/oaipmh-generic-template.yaml
```

---

## Option B — Install into the OpenMetadata Docker deployment

This is the recommended approach when you have deployed OpenMetadata locally using
the official Docker Compose file.

### 1. Download the OpenMetadata Docker Compose file

```bash
curl -fsSLO \
  https://github.com/open-metadata/OpenMetadata/releases/download/2.0.9-release/docker-compose-postgres.yml
```

> Adjust the release tag to match the OpenMetadata version you are deploying.

### 2. Build and start with the OAI-PMH connector baked in

```bash
docker compose \
  -f docker-compose-postgres.yml \
  -f docker/compose.override.yml \
  up --detach --build
```

The override file replaces the `ingestion` service image with a custom build that
adds this package on top of the official image.  All other services (server,
database, etc.) are unchanged.

### 3. Run the ingestion workflow from inside the container

```bash
docker compose -f docker-compose-postgres.yml exec ingestion \
  bash -c '
    export OPENMETADATA_HOST="http://openmetadata-server:8585/api"
    export OPENMETADATA_JWT_TOKEN="<your-token>"
    metadata ingest -c /opt/airflow/openmetadata-oaipmh-connector/configs/oaipmh-onedata-demo.yaml
  '
```

Alternatively, copy your YAML config into the container first:

```bash
docker cp configs/oaipmh-onedata-demo.yaml \
  $(docker compose -f docker-compose-postgres.yml ps -q ingestion):/tmp/

docker compose -f docker-compose-postgres.yml exec ingestion \
  bash -c '
    export OPENMETADATA_HOST="http://openmetadata-server:8585/api"
    export OPENMETADATA_JWT_TOKEN="<your-token>"
    metadata ingest -c /tmp/oaipmh-onedata-demo.yaml
  '
```

### 4. Register the connector in the OpenMetadata UI (optional)

You can also trigger the ingestion from the UI instead of the CLI:

1. Open `http://localhost:8585` in a browser and log in.
2. Go to **Settings → Services → Databases → Add New Service**.
3. Choose **Custom** as the service type.
4. Enter a service name (e.g. `onedata_oaipmh`).
5. Set **Source Python Class** to:
   ```
   openmetadata_oaipmh_connector.connector.OAIPMHConnector
   ```
6. Add the connection options (`oaipmhEndpoint`, `metadataPrefix`, etc.) as
   key-value pairs.
7. Add a **Metadata Ingestion** pipeline and run it.

---

## Configuration reference

All options go under `serviceConnection.config.connectionOptions` in the YAML.

| Key | Required | Default | Description |
|---|---|---|---|
| `oaipmhEndpoint` | yes | — | Base URL of the OAI-PMH endpoint (without verb parameters) |
| `metadataPrefix` | no | `oai_dc` | Metadata format: `oai_dc` or `oai_datacite` |
| `useSets` | no | `false` | `true` → one schema per OAI-PMH set |
| `setSpec` | no | — | Harvest only records belonging to this set |
| `requestTimeout` | no | `60` | HTTP timeout in seconds |

---

## Example: Onedata demo endpoint

```yaml
connectionOptions:
  oaipmhEndpoint: "https://demo.onedata.org/oai_pmh"
  metadataPrefix: "oai_datacite"
  useSets: "false"
```

This harvests all non-deleted records from the public Onedata demo, exposing each
dataset as a searchable entry in OpenMetadata with its DataCite title, abstract,
creators, and publisher.
