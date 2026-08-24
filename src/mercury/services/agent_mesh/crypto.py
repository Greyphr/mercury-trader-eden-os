"""Ed25519 identity keypair for the agent-mesh client.

Native reimplementation of Eden ``agent_runtime/crypto.py`` (``AgentKeyPair``
+ ``load_or_generate_keypair``), which itself is declared "code parity" with
Eden ``device_terminals/device_auth.py``. Same wire format on both sides:
raw 32-byte Ed25519 keys, base64-encoded public keys and signatures.

The private key never leaves this machine — only the base64 public key
travels inside ``agent.register``. Key files persist at
``{paths.data_dir}/eden/keys/{agent_id}_key`` (Mercury's equivalent of Eden's
``agent_data/keys/`` convention) and are generated on first connect; the new
identity then needs one-time owner approval on the Eden side.
"""

from __future__ import annotations

from base64 import b64decode, b64encode
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


class AgentKeyPair:
    """Ed25519 signing identity (parity with Eden agent_runtime/crypto.py)."""

    def __init__(self, private_key: Ed25519PrivateKey | None = None) -> None:
        self._private_key = private_key or Ed25519PrivateKey.generate()
        self._public_key = self._private_key.public_key()

    @classmethod
    def generate(cls) -> AgentKeyPair:
        return cls()

    @property
    def public_key_b64(self) -> str:
        """Base64-encoded raw Ed25519 public key (32 bytes)."""
        raw = self._public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return b64encode(raw).decode("ascii")

    def sign(self, message: bytes) -> str:
        """Sign ``message``, returning the base64-encoded signature (64 bytes)."""
        return b64encode(self._private_key.sign(message)).decode("ascii")

    @staticmethod
    def verify(public_key_b64: str, message: bytes, signature_b64: str) -> bool:
        try:
            pub_key = Ed25519PublicKey.from_public_bytes(b64decode(public_key_b64))
            pub_key.verify(b64decode(signature_b64), message)
            return True
        except (InvalidSignature, ValueError):
            return False

    # ── persistence ──────────────────────────────────────────────

    def save_private_key(self, path: Path) -> None:
        """Persist raw private-key bytes. Called once at first connect."""
        raw = self._private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    @classmethod
    def load_private_key(cls, path: Path) -> AgentKeyPair:
        return cls(Ed25519PrivateKey.from_private_bytes(path.read_bytes()))


def load_or_generate_keypair(key_dir: Path, agent_id: str) -> AgentKeyPair:
    """Load the persisted keypair for ``agent_id``, or generate + persist one.

    Single source of truth for key lifecycle (parity with Eden's helper of
    the same name): stored at ``key_dir / f"{agent_id}_key"``; a corrupted
    file is regenerated rather than crashing startup.
    """
    key_path = key_dir / f"{agent_id}_key"
    if key_path.exists():
        try:
            return AgentKeyPair.load_private_key(key_path)
        except Exception:  # noqa: BLE001 — corrupted key: regenerate below
            pass
    keypair = AgentKeyPair.generate()
    keypair.save_private_key(key_path)
    return keypair
