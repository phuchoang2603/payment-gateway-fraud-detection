from datetime import datetime, timezone
from uuid import uuid4

from simulator.config import RUN_STATE_VERSION


class RunState:
    def __init__(self, store, identity, profile_digest, *, resume=False, takeover=False):
        self.store = store
        self.owner = uuid4().hex
        self.key = "_run/state.json"
        if store.exists("manifest.json"):
            raise FileExistsError("dataset version already published")
        exists = store.exists(self.key)
        if exists and not resume:
            raise FileExistsError("incomplete dataset requires resume")
        if not exists and resume:
            raise FileNotFoundError("no run state to resume")
        if exists:
            etag = store.etag(self.key)
            state = store.json(self.key)
            if (
                state.get("version") != RUN_STATE_VERSION
                or state.get("generation_identity") != identity
                or state.get("profile_sha256") != profile_digest
            ):
                raise ValueError(
                    "incompatible run state: format, generation/contract/revision identity or profile digest mismatch"
                )
            if state.get("owner") and not takeover:
                raise FileExistsError(
                    "another writer owns this dataset; stop it before explicit --takeover-stopped-writer"
                )
            self.state = state
            self.state["owner"] = self.owner
            self.etag = store.put_json(self.key, state, expected=etag)
        else:
            self.state = {
                "version": RUN_STATE_VERSION,
                "generation_identity": identity,
                "profile_sha256": profile_digest,
                "owner": self.owner,
                "phase": "generation",
                "attempts": [],
            }
            self.etag = store.put_json(self.key, self.state, absent=True)

    def check_owner(self):
        if self.store.exists("manifest.json") or self.store.json(self.key).get("owner") != self.owner:
            raise FileExistsError("writer ownership lost or dataset published")

    def checkpoint(self):
        self.check_owner()
        self.etag = self.store.put_json(self.key, self.state, expected=self.etag)

    def attempt(self, configuration):
        self.state["attempts"].append(
            {"started_at": datetime.now(timezone.utc).isoformat(), "execution": configuration}
        )
        self.checkpoint()

    def receipt(self, key, value):
        self.check_owner()
        try:
            etag = self.store.etag(key)
        except FileNotFoundError:
            etag = None
        self.store.put_json(key, value, expected=etag, absent=etag is None)
        self.checkpoint()

    def release(self):
        try:
            etag = self.store.etag(self.key)
        except FileNotFoundError:
            return
        if self.store.json(self.key).get("owner") == self.owner:
            self.state["owner"] = None
            self.etag = self.store.put_json(self.key, self.state, expected=etag)
