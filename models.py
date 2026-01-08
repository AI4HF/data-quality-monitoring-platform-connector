import json

class MonitoringPlatformQualityCheck:
    """Event model for the Monitoring Platform's expected 'quality_check' payload."""
    def __init__(self, dataset_id: str, dataset_name: str | None,
                 name: str, category: dict | None,
                 low, value, passed: bool, at_timestamp: str):
        """Initialize a quality check event"""
        self.event_type = "quality_check"
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.name = name
        self.category = category or {}
        self.low = low
        self.value = value
        self.passed = passed
        self.at_timestamp = at_timestamp

    def to_dict(self):
        """Serialize to the Monitoring Platform JSON schema, omitting null fields."""
        payload = {
            "event_type": self.event_type,
            "dataset_id": self.dataset_id,
            "dataset_name": self.dataset_name,
            "name": self.name,
            "category": {
                k: v for k, v in {
                    "context": (self.category or {}).get("context"),
                    "category": (self.category or {}).get("category")
                }.items() if v is not None
            } if self.category else None,
            "low": self.low,
            "value": self.value,
            "passed": self.passed,
            "@timestamp": self.at_timestamp
        }
        return {k: v for k, v in payload.items() if v is not None}

    def __str__(self):
        """Return the compact JSON string form of the event."""
        return json.dumps(self.to_dict())
