import time

import httpx
import pytest
from fastapi.testclient import TestClient

from peerlink.hub import create_app


@pytest.mark.parametrize("cancel,expected", [(False, "FAILED"), (True, "CANCELLED")])
def test_notification_failure_preserves_a_concurrent_decision(tmp_path, monkeypatch, cancel, expected):
    app = create_app(tmp_path / "hub.db")
    client = TestClient(app)
    owner = {"Authorization": "Bearer " + app.state.store.create_admin("alice")}
    device = client.post("/api/devices", headers=owner, json={"name": "laptop"}).json()
    with app.state.store.connect() as conn:
        conn.execute("INSERT INTO project_catalog VALUES (?,?,?,?,?)",
                     ("demo", "", "ACTIVE", "alice", time.time()))
    assert client.put("/api/bridge/agents", headers={"Authorization": "Bearer " + device["token"]},
                      json={"id": "demo"}).status_code == 200
    monkeypatch.setattr(app.state.relay, "online", lambda _: True)

    async def dropped(device_id, envelope):
        if cancel and envelope["type"] == "agent.approval_requested":
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://test") as sender:
                decision = await sender.post(f"/api/requests/{envelope['message_id']}/decision",
                                             headers=owner, json={"action": "cancel"})
                assert decision.json()["status"] == "CANCELLED"
        return False

    monkeypatch.setattr(app.state.relay, "send", dropped)
    sent = client.post("/api/bridge/messages", headers=owner, json={
        "receiver": "alice", "agent_id": "demo", "content": "Hello",
    }).json()
    assert sent["status"] == expected
    request = client.get(f"/api/requests/{sent['id']}", headers=owner).json()
    assert request["status"] == expected
    assert request["error"] == (None if cancel else "DEVICE_OFFLINE")
