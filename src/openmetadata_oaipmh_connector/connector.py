"""
OAI-PMH connector for OpenMetadata.

Harvests metadata records from an OAI-PMH endpoint and maps them to
OpenMetadata entities:

    OAI-PMH repository  →  DatabaseService  (CustomDatabase)
    OAI-PMH set         →  DatabaseSchema   (or one default "records" schema)
    OAI-PMH record      →  Table            (metadata fields as columns)

Supported metadata prefixes
----------------------------
oai_datacite   DataCite Metadata Schema (kernel-4 wrapped in oai-1.1)
oai_dc         Dublin Core (default OAI-PMH format)

Configuration options (connectionOptions)
-----------------------------------------
oaipmhEndpoint     (required) Base URL of the OAI-PMH endpoint,
                   e.g. https://demo.onedata.org/oai_pmh
metadataPrefix     Metadata format to request (default: oai_dc)
setSpec            Harvest only records belonging to this set.
                   When omitted all records are harvested.
useSets            "true" to map each set to its own schema;
                   "false" (default) to use a single "records" schema.
requestTimeout     HTTP request timeout in seconds (default: 60)
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Iterable, Optional

import requests

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.data.table import (
    Column,
    DataType,
    Table,
    TableData,
    TableType,
)
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (  # noqa: E501
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException, Source
from metadata.ingestion.models.barrier import Barrier
from metadata.ingestion.ometa.ometa_api import OpenMetadata

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# XML namespace constants
# ---------------------------------------------------------------------------
NS_OAI = "http://www.openarchives.org/OAI/2.0/"
NS_DATACITE_KRN = "http://datacite.org/schema/kernel-4"   # the <resource> element
NS_DATACITE_OAI = "http://schema.datacite.org/oai/oai-1.1/"  # <oai_datacite> wrapper
NS_OAI_DC = "http://www.openarchives.org/OAI/2.0/oai_dc/"
NS_DC = "http://purl.org/dc/elements/1.1/"


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _sanitize_name(value: str, max_len: int = 128) -> str:
    """Convert an arbitrary string into a valid OpenMetadata entity name."""
    name = re.sub(r"[^a-zA-Z0-9_\-.]", "_", value)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        return "unnamed"
    if name[0].isdigit():
        name = "r_" + name
    return name[:max_len]


def _text(element: ET.Element | None) -> str:
    """Return stripped text of an element, or empty string."""
    return (element.text or "").strip() if element is not None else ""


def _find_any(root: ET.Element, local_name: str) -> ET.Element | None:
    """
    Search the entire subtree for the first element whose *local* name matches,
    regardless of namespace.  Useful when namespace declarations vary between
    OAI-PMH server implementations.
    """
    for el in root.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag == local_name:
            return el
    return None


def _findall_any(root: ET.Element, local_name: str) -> list[ET.Element]:
    """Return all elements whose local name matches, regardless of namespace."""
    return [
        el for el in root.iter()
        if (el.tag.split("}")[-1] if "}" in el.tag else el.tag) == local_name
    ]


# ---------------------------------------------------------------------------
# Metadata columns that every harvested record exposes
# ---------------------------------------------------------------------------

_RECORD_COLUMNS: list[tuple[str, DataType, str]] = [
    ("oai_identifier",   DataType.VARCHAR,   "Full OAI-PMH record identifier"),
    ("title",            DataType.VARCHAR,   "Resource title"),
    ("creator",          DataType.VARCHAR,   "Creator(s), semicolon-separated"),
    ("description",      DataType.VARCHAR,   "Abstract or primary description"),
    ("publisher",        DataType.VARCHAR,   "Publishing organisation"),
    ("publication_year", DataType.VARCHAR,   "Year of publication"),
    ("resource_type",    DataType.VARCHAR,   "Resource type (e.g. Dataset, Software)"),
    ("subject",          DataType.VARCHAR,   "Subject keywords, semicolon-separated"),
    ("identifier_url",   DataType.VARCHAR,   "Resolvable URL or DOI for the resource"),
    ("datestamp",        DataType.TIMESTAMP, "Last-modified timestamp reported by OAI-PMH"),
    ("set_spec",         DataType.VARCHAR,   "OAI-PMH set(s) this record belongs to"),
]


def _make_columns() -> list[Column]:
    return [
        Column(name=name, dataType=dtype, dataLength=1024, description=desc)
        for name, dtype, desc in _RECORD_COLUMNS
    ]


# ---------------------------------------------------------------------------
# Main connector class
# ---------------------------------------------------------------------------

class OAIPMHConnector(Source):
    """
    OpenMetadata Source connector for OAI-PMH harvesting endpoints.

    See module docstring for the full option reference.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata) -> None:
        super().__init__()
        self.config = config
        self.metadata = metadata

        conn = config.serviceConnection.root.config
        raw_opts: dict = {}
        if conn.connectionOptions and conn.connectionOptions.root:
            raw_opts = conn.connectionOptions.root

        self.endpoint: str = raw_opts.get("oaipmhEndpoint", "").rstrip("/")
        self.metadata_prefix: str = raw_opts.get("metadataPrefix", "oai_dc")
        self.set_spec: str | None = raw_opts.get("setSpec") or None
        self.use_sets: bool = raw_opts.get("useSets", "false").lower() == "true"
        self.timeout: int = int(raw_opts.get("requestTimeout", "60"))

        if not self.endpoint:
            raise ValueError(
                "connectionOptions.oaipmhEndpoint is required. "
                "Example: https://demo.onedata.org/oai_pmh"
            )

    # ------------------------------------------------------------------
    # Framework lifecycle
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,
    ) -> "OAIPMHConnector":
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, got {type(connection).__name__}"
            )
        return cls(config, metadata)

    def prepare(self) -> None:
        pass

    def test_connection(self) -> None:
        """Verify that the OAI-PMH endpoint responds to Identify."""
        resp = requests.get(
            self.endpoint,
            params={"verb": "Identify"},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        error_el = root.find(f"{{{NS_OAI}}}error")
        if error_el is not None:
            raise RuntimeError(
                f"OAI-PMH error [{error_el.get('code')}]: {_text(error_el)}"
            )

    def close(self) -> None:
        pass

    # ------------------------------------------------------------------
    # OAI-PMH HTTP helpers
    # ------------------------------------------------------------------

    def _oai_get(self, params: dict) -> ET.Element:
        """Execute a single OAI-PMH request and return the parsed XML root."""
        resp = requests.get(self.endpoint, params=params, timeout=self.timeout)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        error_el = root.find(f"{{{NS_OAI}}}error")
        if error_el is not None:
            raise RuntimeError(
                f"OAI-PMH error [{error_el.get('code', '')}]: {_text(error_el)}"
            )
        return root

    def _identify(self) -> dict[str, str]:
        """Call the Identify verb and return a dict of repository properties."""
        root = self._oai_get({"verb": "Identify"})
        identify_el = root.find(f"{{{NS_OAI}}}Identify")
        result: dict[str, str] = {}
        if identify_el is not None:
            for child in identify_el:
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                result[local] = _text(child)
        return result

    def _list_sets(self) -> list[tuple[str, str]]:
        """
        Return a list of (setSpec, setName) for every set in the repository.
        Returns an empty list if the repository declares no set hierarchy.
        """
        sets: list[tuple[str, str]] = []
        params: dict = {"verb": "ListSets"}
        while True:
            try:
                root = self._oai_get(params)
            except RuntimeError as exc:
                if "noSetHierarchy" in str(exc):
                    return []
                raise
            list_sets_el = root.find(f"{{{NS_OAI}}}ListSets")
            if list_sets_el is None:
                break
            for set_el in list_sets_el.findall(f"{{{NS_OAI}}}set"):
                spec = _text(set_el.find(f"{{{NS_OAI}}}setSpec"))
                name_el = set_el.find(f"{{{NS_OAI}}}setName")
                name = _text(name_el) if name_el is not None else spec
                if spec:
                    sets.append((spec, name or spec))
            token_el = list_sets_el.find(f"{{{NS_OAI}}}resumptionToken")
            if token_el is None or not _text(token_el):
                break
            params = {"verb": "ListSets", "resumptionToken": _text(token_el)}
        return sets

    def _iter_records(
        self, set_spec: str | None = None
    ) -> Iterable[tuple[dict, ET.Element | None]]:
        """
        Yield (header_dict, metadata_element_or_None) for every *non-deleted*
        record in the repository (or in a specific set if set_spec is given).

        header_dict keys: identifier, datestamp, setSpecs (list[str])
        """
        params: dict = {
            "verb": "ListRecords",
            "metadataPrefix": self.metadata_prefix,
        }
        if set_spec:
            params["set"] = set_spec

        while True:
            try:
                root = self._oai_get(params)
            except RuntimeError as exc:
                if "noRecordsMatch" in str(exc):
                    return
                raise

            list_records_el = root.find(f"{{{NS_OAI}}}ListRecords")
            if list_records_el is None:
                break

            for record_el in list_records_el.findall(f"{{{NS_OAI}}}record"):
                header_el = record_el.find(f"{{{NS_OAI}}}header")
                if header_el is None:
                    continue
                if header_el.get("status") == "deleted":
                    continue  # skip deleted records as requested

                header: dict = {"setSpecs": []}
                for child in header_el:
                    local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if local == "setSpec":
                        header["setSpecs"].append(_text(child))
                    else:
                        header[local] = _text(child)

                metadata_el = record_el.find(f"{{{NS_OAI}}}metadata")
                yield header, metadata_el

            token_el = list_records_el.find(f"{{{NS_OAI}}}resumptionToken")
            if token_el is None or not _text(token_el):
                break
            # For subsequent pages only the resumptionToken is sent
            params = {"verb": "ListRecords", "resumptionToken": _text(token_el)}

    # ------------------------------------------------------------------
    # Metadata extraction — oai_datacite
    # ------------------------------------------------------------------

    def _parse_datacite(self, metadata_el: ET.Element) -> dict:
        """
        Extract fields from an oai_datacite-wrapped DataCite kernel-4 record.

        Structure:
            <metadata>
              <oai_datacite xmlns="http://schema.datacite.org/oai/oai-1.1/">
                <payload>
                  <resource xmlns="http://datacite.org/schema/kernel-4">
                    ...
                  </resource>
                </payload>
              </oai_datacite>
            </metadata>
        """
        # Locate the <resource> element — try namespaced first, then bare
        resource = metadata_el.find(f".//{{{NS_DATACITE_KRN}}}resource")
        if resource is None:
            resource = _find_any(metadata_el, "resource")
        if resource is None:
            return {}

        result: dict = {}

        # --- Identifier ---
        id_el = (
            resource.find(f"{{{NS_DATACITE_KRN}}}identifier")
            or _find_any(resource, "identifier")
        )
        if id_el is not None:
            result["identifier"] = _text(id_el)
            result["identifier_type"] = id_el.get("identifierType", "")

        # --- Titles ---
        titles = [
            _text(el)
            for el in (
                resource.findall(f".//{{{NS_DATACITE_KRN}}}title")
                or _findall_any(resource, "title")
            )
            if _text(el)
        ]
        result["titles"] = titles

        # --- Creators ---
        creators = [
            _text(el)
            for el in (
                resource.findall(f".//{{{NS_DATACITE_KRN}}}creatorName")
                or _findall_any(resource, "creatorName")
            )
            if _text(el)
        ]
        result["creators"] = creators

        # --- Publisher ---
        pub_el = resource.find(f"{{{NS_DATACITE_KRN}}}publisher") or _find_any(
            resource, "publisher"
        )
        result["publisher"] = _text(pub_el)

        # --- Publication year ---
        year_el = resource.find(
            f"{{{NS_DATACITE_KRN}}}publicationYear"
        ) or _find_any(resource, "publicationYear")
        result["publication_year"] = _text(year_el)

        # --- Resource type ---
        rt_el = resource.find(f"{{{NS_DATACITE_KRN}}}resourceType") or _find_any(
            resource, "resourceType"
        )
        if rt_el is not None:
            result["resource_type"] = rt_el.get("resourceTypeGeneral", _text(rt_el))

        # --- Descriptions ---
        descs: list[str] = []
        abstract: str | None = None
        for d_el in resource.findall(
            f".//{{{NS_DATACITE_KRN}}}description"
        ) or _findall_any(resource, "description"):
            text = _text(d_el)
            if not text:
                continue
            if d_el.get("descriptionType") == "Abstract":
                abstract = text
            else:
                descs.append(text)
        result["descriptions"] = ([abstract] + descs) if abstract else descs

        # --- Subjects / keywords ---
        result["subjects"] = [
            _text(el)
            for el in (
                resource.findall(f".//{{{NS_DATACITE_KRN}}}subject")
                or _findall_any(resource, "subject")
            )
            if _text(el)
        ]

        # --- Alternate identifiers (e.g. share URL) ---
        result["alternate_identifiers"] = [
            (el.get("alternateIdentifierType", ""), _text(el))
            for el in (
                resource.findall(f".//{{{NS_DATACITE_KRN}}}alternateIdentifier")
                or _findall_any(resource, "alternateIdentifier")
            )
            if _text(el)
        ]

        return result

    # ------------------------------------------------------------------
    # Metadata extraction — oai_dc (Dublin Core)
    # ------------------------------------------------------------------

    def _parse_dc(self, metadata_el: ET.Element) -> dict:
        """Extract fields from an oai_dc Dublin Core metadata element."""

        def _all(local: str) -> list[str]:
            return [
                _text(el)
                for el in _findall_any(metadata_el, local)
                if _text(el)
            ]

        ids = _all("identifier")
        return {
            "identifier": ids[0] if ids else "",
            "titles": _all("title"),
            "creators": _all("creator"),
            "publisher": "; ".join(_all("publisher")),
            "publication_year": (_all("date") or [""])[0][:4],
            "resource_type": (_all("type") or [""])[0],
            "descriptions": _all("description"),
            "subjects": _all("subject"),
            "alternate_identifiers": [],
        }

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def _parse_metadata(self, metadata_el: ET.Element | None) -> dict:
        if metadata_el is None:
            return {}
        if self.metadata_prefix in ("oai_datacite",):
            return self._parse_datacite(metadata_el)
        # oai_dc and any other Dublin-Core-based format
        return self._parse_dc(metadata_el)

    # ------------------------------------------------------------------
    # Naming helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _record_name(oai_id: str) -> str:
        """Derive a short, stable table name from an OAI-PMH identifier."""
        # e.g. "oai:demo.onedata.org:2fd07bf5..." → "2fd07bf5..."
        parts = oai_id.rsplit(":", 1)
        short = parts[-1] if parts else oai_id
        return _sanitize_name(short)

    # ------------------------------------------------------------------
    # Main ingestion iterator
    # ------------------------------------------------------------------

    def _iter(self, *_, **__) -> Iterable[Either]:  # noqa: ANN002
        service_name = self.config.serviceName

        # ---- Identify repository ----------------------------------------
        try:
            info = self._identify()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not Identify OAI-PMH repository: %s", exc)
            info = {}

        repo_name: str = info.get("repositoryName") or self.endpoint
        db_name: str = _sanitize_name(repo_name) or "oaipmh_repository"

        # ---- DatabaseService --------------------------------------------
        yield Either(
            right=CreateDatabaseServiceRequest(
                name=service_name,
                serviceType=DatabaseServiceType.CustomDatabase,
                connection=self.config.serviceConnection.root,
                displayName=repo_name,
                description=(
                    f"OAI-PMH repository harvested from {self.endpoint}. "
                    f"Metadata prefix: {self.metadata_prefix}."
                ),
            )
        )

        # ---- Database ---------------------------------------------------
        yield Either(
            right=CreateDatabaseRequest(
                name=db_name,
                service=service_name,
                displayName=repo_name,
                description=info.get("description") or None,
            )
        )
        db_fqn = f"{service_name}.{db_name}"

        columns = _make_columns()

        # ---- Schemas and Tables -----------------------------------------
        if self.set_spec:
            # Harvest a single set
            yield from self._emit_schema_and_records(
                db_fqn=db_fqn,
                schema_name=_sanitize_name(self.set_spec),
                schema_display=self.set_spec,
                schema_desc=f"OAI-PMH set: {self.set_spec}",
                filter_set=self.set_spec,
                columns=columns,
            )
        elif self.use_sets:
            # One schema per set
            sets = self._list_sets()
            if not sets:
                logger.info("Repository declares no sets; using a single schema.")
                yield from self._emit_schema_and_records(
                    db_fqn=db_fqn,
                    schema_name="records",
                    schema_display="Records",
                    schema_desc="All harvested OAI-PMH records",
                    filter_set=None,
                    columns=columns,
                )
            else:
                for spec, name in sets:
                    yield from self._emit_schema_and_records(
                        db_fqn=db_fqn,
                        schema_name=_sanitize_name(spec),
                        schema_display=name,
                        schema_desc=f"OAI-PMH set: {spec}",
                        filter_set=spec,
                        columns=columns,
                    )
        else:
            # Default: single "records" schema, all records
            yield from self._emit_schema_and_records(
                db_fqn=db_fqn,
                schema_name="records",
                schema_display="Records",
                schema_desc=(
                    f"All records harvested from {self.endpoint} "
                    f"(metadataPrefix={self.metadata_prefix})"
                ),
                filter_set=None,
                columns=columns,
            )

    # ------------------------------------------------------------------
    # Description and sample-data helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_description(oai_id: str, header: dict, meta: dict) -> str | None:
        """
        Build a human-readable description block that shows every available
        metadata value.  This text is displayed in the OpenMetadata UI
        whenever a user opens a record.
        """
        lines: list[str] = []

        if meta.get("titles") and meta["titles"][0]:
            lines.append("**Title:** " + meta["titles"][0])
        if meta.get("creators"):
            lines.append("**Creator(s):** " + "; ".join(meta["creators"]))
        if meta.get("publisher"):
            lines.append(f"**Publisher:** {meta['publisher']}")
        if meta.get("publication_year"):
            lines.append(f"**Publication Year:** {meta['publication_year']}")
        if meta.get("resource_type"):
            lines.append(f"**Resource Type:** {meta['resource_type']}")
        if meta.get("subjects"):
            lines.append("**Subject(s):** " + "; ".join(meta["subjects"]))
        if meta.get("identifier"):
            lines.append(f"**Identifier:** {meta['identifier']}")
        if header.get("datestamp"):
            lines.append(f"**Datestamp:** {header['datestamp']}")
        if header.get("setSpecs"):
            lines.append("**Set(s):** " + "; ".join(header["setSpecs"]))
        if meta.get("alternate_identifiers"):
            for id_type, id_val in meta["alternate_identifiers"]:
                lines.append(f"**Alternate Identifier ({id_type}):** {id_val}")

        if meta.get("descriptions"):
            lines.append("")  # blank line before abstract
            lines.append("**Abstract:**")
            lines.append(meta["descriptions"][0])

        return "\n\n".join(lines) or None

    @staticmethod
    def _build_sample_row(oai_id: str, header: dict, meta: dict) -> list:
        """Return one sample-data row aligned to _RECORD_COLUMNS order."""
        def _first(lst: list, default: str = "") -> str:
            return lst[0] if lst else default

        return [
            oai_id,
            _first(meta.get("titles", [])),
            "; ".join(meta.get("creators", [])),
            _first(meta.get("descriptions", [])),
            meta.get("publisher", ""),
            meta.get("publication_year", ""),
            meta.get("resource_type", ""),
            "; ".join(meta.get("subjects", [])),
            meta.get("identifier", ""),
            header.get("datestamp", ""),
            "; ".join(header.get("setSpecs", [])),
        ]

    # ------------------------------------------------------------------
    # Schema + record emission
    # ------------------------------------------------------------------

    def _emit_schema_and_records(
        self,
        *,
        db_fqn: str,
        schema_name: str,
        schema_display: str,
        schema_desc: str,
        filter_set: str | None,
        columns: list[Column],
    ) -> Iterable[Either]:
        yield Either(
            right=CreateDatabaseSchemaRequest(
                name=schema_name,
                database=db_fqn,
                displayName=schema_display,
                description=schema_desc,
            )
        )
        schema_fqn = f"{db_fqn}.{schema_name}"

        seen_names: set[str] = set()
        # table_name → sample row, collected for a second-pass sample-data upload
        sample_data_pending: dict[str, list] = {}
        count = 0

        for header, metadata_el in self._iter_records(filter_set):
            oai_id: str = header.get("identifier", "")
            meta = self._parse_metadata(metadata_el)

            # Build a unique table name
            base_name = self._record_name(oai_id) if oai_id else f"record_{count}"
            table_name = base_name
            suffix = 1
            while table_name in seen_names:
                table_name = f"{base_name}_{suffix}"
                suffix += 1
            seen_names.add(table_name)

            display_name: str = meta.get("titles", [""])[0] or oai_id
            description = self._build_description(oai_id, header, meta)

            yield Either(
                right=CreateTableRequest(
                    name=table_name,
                    databaseSchema=schema_fqn,
                    displayName=display_name[:256],
                    description=description,
                    tableType=TableType.External,
                    columns=columns,
                    sourceUrl=meta.get("identifier") or None,
                )
            )
            sample_data_pending[f"{schema_fqn}.{table_name}"] = self._build_sample_row(
                oai_id, header, meta
            )
            count += 1

        # ---- Sample data (second pass) ------------------------------------
        # Tables must be committed to the server before we can attach data.
        if sample_data_pending:
            yield Either(right=Barrier(reason="tables must exist before sample data"))
            col_names = [col_name for col_name, _, _ in _RECORD_COLUMNS]
            for table_fqn, row in sample_data_pending.items():
                try:
                    table: Table | None = self.metadata.get_by_name(
                        entity=Table, fqn=table_fqn
                    )
                    if table is None:
                        logger.warning("Table not found for sample data: %s", table_fqn)
                        continue
                    self.metadata.ingest_table_sample_data(
                        table=table,
                        sample_data=TableData(columns=col_names, rows=[row]),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Could not ingest sample data for %s: %s", table_fqn, exc
                    )

        logger.info(
            "Schema '%s': emitted %d record(s) from set=%s",
            schema_name,
            count,
            filter_set or "(all)",
        )
