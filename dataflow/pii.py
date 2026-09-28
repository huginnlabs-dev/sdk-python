"""Client-side PII classification.

Field *names* are matched against privacy keywords and reduced to category
labels ("email,phone"). Only labels travel in span metadata — values never
leave the process unencrypted (payloads are E2E-encrypted client-side).

Multiword keywords ("first_name") match by substring, single tokens
("card", "ip") by exact word match so "description" never lights up the
"ip" category.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Sequence

_CATEGORIES: List[tuple] = [
    ("password", ("password", "passwd", "pwd")),
    ("secret", ("token", "secret", "apikey", "api_key", "credential", "session", "jwt", "auth")),
    ("payment", ("card", "pan", "cvv", "cvc", "iban", "expiry")),
    ("email", ("email", "e_mail", "mail")),
    ("phone", ("phone", "mobile", "tel", "msisdn")),
    ("government_id", ("ssn", "passport", "tax_id", "national_id")),
    ("birth", ("birth", "dob", "age")),
    ("name", ("first_name", "last_name", "full_name", "surname", "customer_name", "display_name")),
    ("address", ("street", "zip", "postal", "street_address", "postal_address",
                 "home_address", "billing_address", "shipping_address", "mailing_address")),
    ("geo", ("city", "country", "region", "location", "lat", "lon", "lng")),
    ("ip", ("ip", "ip_address", "client_ip", "remote_addr")),
    ("device", ("device", "user_agent", "imei", "fingerprint")),
]

_NONWORD = re.compile(r"[^a-z0-9_]+")


def classify_pii(fields: Iterable[str]) -> str:
    """Map field names to deduplicated, comma-joined privacy categories."""
    seen: Dict[str, bool] = {}
    for field in fields:
        norm = _NONWORD.sub("_", str(field).lower())
        tokens = set(norm.split("_"))
        for category, keywords in _CATEGORIES:
            if seen.get(category):
                continue
            for kw in keywords:
                if ("_" in kw and kw in norm) or (kw in tokens):
                    seen[category] = True
                    break
    return ",".join(sorted(seen))
