import os
import requests
from models import MonitoringPlatformQualityCheck
from dateutil.parser import isoparse
from datetime import datetime, timezone

def _log(msg: str):
    """Emit a UTC timestamped log line to stdout for simple, container-friendly tracing."""
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"[{ts}] {msg}", flush=True)

def _opt(s: str):
    """Return the string if non-empty after trimming; otherwise return None."""
    return s if s and str(s).strip() else None

def to_iso_z(ts):
    """Normalize a timestamp to ISO-8601 with milliseconds and trailing 'Z'.
    Accepts None (now), epoch ms/seconds (int/float), or date strings parseable by dateutil."""
    if ts is None:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(float(ts)/1000.0, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    try:
        dt = isoparse(ts)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

class FeastDQOnlyConnector:
    """One-shot connector that triggers Feast Data Quality for dataset+criteria pairs and forwards
    each quality result to the Monitoring Platform via Logstash using Basic authentication."""
    def __init__(self,
                 feast_server_url: str,
                 criteria_ids: list,
                 dataset_ids: list,
                 logstash_url: str,
                 logstash_basic_auth: str):
        """Initialize connector with Feast base URL, list of criteria and datasets."""
        self.base = feast_server_url.rstrip("/")
        self.criteria_ids = criteria_ids
        self.dataset_ids = dataset_ids
        self.logstash_url = logstash_url
        self.logstash_basic_auth = logstash_basic_auth

    def _feast_headers(self):
        """Build HTTP headers for Feast request."""
        return {"Content-Type": "application/json"}

    def _resolve_logstash_basic(self) -> str:
        """Return base64-encoded Basic token from LOGSTASH_BASIC_AUTH; error if missing."""
        token = _opt(self.logstash_basic_auth)
        if not token:
            raise RuntimeError("Missing Logstash credentials: LOGSTASH_BASIC_AUTH must be set (base64 user:pass)")
        _log("[AUTH] Using LOGSTASH_BASIC_AUTH (base64)")
        return token

    def _logstash_headers(self):
        """Build HTTP headers for Monitoring Platform requests with resolved Basic auth."""
        return {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self._resolve_logstash_basic()}"
        }

    def fetch_dataset_name(self, dataset_id: str) -> str | None:
        """Query Feast for dataset metadata and return a reasonable display name (title/name)."""
        url = f"{self.base}/Dataset/{dataset_id}"
        _log(f"[FEAST] GET {url}")
        r = requests.get(url, headers=self._feast_headers(), timeout=30)
        _log(f"[FEAST] GET status={r.status_code}")
        if r.ok and r.text:
            j = r.json()
            name = j.get("title") or j.get("name")
            _log(f"[FEAST] Dataset name: {name!r}")
            return name
        return None

    def evaluate_quality(self, dataset_id: str, criteria_id: str) -> dict:
        """Trigger Feast dataset quality evaluation using filename-only outputPath; return report JSON."""
        ts_str = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        filename_only = f"{dataset_id}-{criteria_id}-{ts_str}.json"
        url = f"{self.base}/Dataset/{dataset_id}/DatasetQualityCriteria/{criteria_id}/$quality"
        _log(f"[FEAST] POST {url}?outputPath={filename_only}")
        r = requests.post(url, params={"outputPath": filename_only}, headers=self._feast_headers(), timeout=180)
        _log(f"[FEAST] POST status={r.status_code}")
        r.raise_for_status()
        return r.json()

    def send_event(self, evt: MonitoringPlatformQualityCheck):
        """Send a single quality result to the Monitoring Platform via Logstash."""
        payload = evt.to_dict()
        _log(f"[MP] POST {self.logstash_url} dataset_id={payload.get('dataset_id')} name={payload.get('name')!r} value={payload.get('value')} passed={payload.get('passed')}")
        r = requests.post(self.logstash_url, json=payload, headers=self._logstash_headers(), timeout=30)
        _log(f"[MP] POST status={r.status_code}")
        r.raise_for_status()

    def run_once(self):
        """Run connector once for all dataset–criteria pairs; log counts and exit."""
        _log("[RUN] Start")
        _log(f"[RUN] Datasets={self.dataset_ids} Criteria={self.criteria_ids}")
        total_pairs = 0
        total_events = 0
        for ds_id in self.dataset_ids:
            _log(f"[RUN] Dataset={ds_id}")
            ds_name = self.fetch_dataset_name(ds_id)
            for crit in self.criteria_ids:
                total_pairs += 1
                _log(f"[RUN] Criteria={crit}")
                report = self.evaluate_quality(ds_id, crit)
                iso_ts = to_iso_z(report.get("issued"))
                results = (report.get("results") or [])
                _log(f"[RUN] Results={len(results)} issued={iso_ts}")
                sent_for_pair = 0
                for res in results:
                    cat = res.get("category") or {}
                    evt = MonitoringPlatformQualityCheck(
                        dataset_id=report.get("datasetId") or ds_id,
                        dataset_name=ds_name,
                        name=res.get("name"),
                        category={"context": cat.get("context"), "category": cat.get("category")},
                        low=res.get("low"),
                        value=float(res.get("value")) if res.get("value") is not None else None,
                        passed=bool(res.get("passed")),
                        at_timestamp=iso_ts
                    )
                    self.send_event(evt)
                    sent_for_pair += 1
                    total_events += 1
                _log(f"[RUN] Sent={sent_for_pair} for dataset={ds_id} criteria={crit}")
        _log(f"[DONE] Pairs={total_pairs} Events={total_events}")

if __name__ == "__main__":
    _log("feast-dq-connector starting")
    feast_server_url = os.getenv("FEAST_SERVER_URL")
    criteria_ids = [c.strip() for c in os.getenv("FEAST_CRITERIA_IDS","").split(",") if c.strip()]
    dataset_ids = [d.strip() for d in os.getenv("FEAST_DATASET_IDS","").split(",") if d.strip()]
    logstash_url = os.getenv("LOGSTASH_URL")
    logstash_basic_auth = _opt(os.getenv("LOGSTASH_BASIC_AUTH",""))
    if not all([feast_server_url, criteria_ids, dataset_ids, logstash_url, logstash_basic_auth]):
        raise RuntimeError("Missing required env vars: FEAST_SERVER_URL, FEAST_CRITERIA_IDS, FEAST_DATASET_IDS, LOGSTASH_URL, LOGSTASH_BASIC_AUTH")
    connector = FeastDQOnlyConnector(
        feast_server_url=feast_server_url,
        criteria_ids=criteria_ids,
        dataset_ids=dataset_ids,
        logstash_url=logstash_url,
        logstash_basic_auth=logstash_basic_auth
    )
    connector.run_once()
    _log("feast-dq-connector exiting")
