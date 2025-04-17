import json
from .models import ExchangeCurrency

def compare_snapshots(old: dict, new: ExchangeCurrency) -> list[str]:
    changed_fields = []
    fields = ["active", "deposit", "withdraw", "precision", "limits", "networks"]
    for field in fields:
        new_val = getattr(new, field)
        old_val = old.get(field)
        if json.dumps(new_val, sort_keys=True) != json.dumps(old_val, sort_keys=True):
            changed_fields.append(field)
    return changed_fields