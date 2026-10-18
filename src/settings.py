"""Load local configuration, falling back to the committed example file."""

try:
    from .config import CHAT_CONFIG, GATEWAY_CONFIG, INTENT_CONFIG, WORKFLOW_CONFIGS
except ModuleNotFoundError as exc:
    if exc.name != f"{__package__}.config":
        raise
    from .config_example import (
        CHAT_CONFIG,
        GATEWAY_CONFIG,
        INTENT_CONFIG,
        WORKFLOW_CONFIGS,
    )
