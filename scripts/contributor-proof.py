import json
import random
import string

from headroom.transforms.smart_crusher import SmartCrusher
from headroom.utils import extract_user_query

rng = random.Random(17)
rows = []
for i in range(200):
    msg = "".join(rng.choices(string.ascii_lowercase, k=100))
    if i in range(80, 100):
        msg += " Kafka replication lag"
    rows.append({"message": msg, "status": "ok", "service": "worker"})
messages = [
    {
        "role": "user",
        "content": [
            {"type": "text", "text": "Review production incidents"},
            {"type": "text", "text": "Focus on Kafka replication"},
        ],
    }
]
q = extract_user_query(messages)
r = SmartCrusher(with_compaction=False).crush(json.dumps(rows), q, record_toin=False)
print("query:", repr(q))
print("strategy:", r.strategy, "bytes:", len(r.original), "->", len(r.compressed))
print("target records visible:", r.compressed.count("Kafka replication lag"), "of 20")
