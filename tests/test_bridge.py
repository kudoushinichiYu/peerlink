import asyncio
import json
import socket
import sys
import threading
import time
from contextlib import asynccontextmanager, suppress

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from websockets.exceptions import ConnectionClosedError, InvalidHandshake

from peerlink.bridge.client import BridgeRunner, websocket_url
from peerlink.bridge.codex import CodexAppServerBackend
from peerlink.bridge.registry import add_agent
from peerlink.connector import read_json, review, save_json
from peerlink.hub import create_app


def setup_team(tmp_path):
    app = create_app(tmp_path / "hub.db")
    client = TestClient(app)
    sender = app.state.store.register_user("alex")
    owner = app.state.store.register_user("bob")
    app.state.store.approve_user("alex")
    app.state.store.approve_user("bob")
    with app.state.store.connect() as conn:
        conn.execute("INSERT INTO project_catalog VALUES (?,?,?,?,?)",
                     ("recommendation", "Recommendation", "ACTIVE", "bob", time.time()))
    sender_auth = {"Authorization": "Bearer " + sender}
    owner_auth = {"Authorization": "Bearer " + owner}
    device = client.post("/api/devices", headers=owner_auth, json={"name": "bob-mac"}).json()
    device_auth = {"Authorization": "Bearer " + device["token"]}
    return app, client, sender_auth, owner_auth, device, device_auth


class LocalClient:
    def __init__(self, client, headers):
        self.client = client
        self.headers = headers

    def call(self, method, path, **kwargs):
        response = self.client.request(method, path, headers=self.headers, **kwargs)
        response.raise_for_status()
        return response.json()


class LocalSocket:
    def __init__(self):
        self.frames = []

    async def send(self, frame):
        self.frames.append(json.loads(frame))


@pytest.mark.parametrize("hub,expected", [
    ("http://[::1]:8000/", "ws://[::1]:8000/api/bridge/ws"),
    ("https://example.org/team/peerlink", "wss://example.org/team/peerlink/api/bridge/ws"),
    ("https://example.org/team/peerlink/", "wss://example.org/team/peerlink/api/bridge/ws"),
])
def test_websocket_url_preserves_hub_base_path(hub, expected):
    assert websocket_url(hub) == expected


def test_two_users_relay_bridge_echo_and_review(tmp_path, monkeypatch):
    app, client, sender_auth, owner_auth, device, device_auth = setup_team(tmp_path)
    workspace = tmp_path / "recommendation"
    workspace.mkdir()
    state = tmp_path / "bob-state"
    save_json(state / "connector.json", {
        "hub": "http://127.0.0.1:8000", "id": device["id"], "owner": "bob",
        "token": device["token"], "projects": {},
    })
    add_agent(state, "recommendation", workspace, visibility="team")

    with client.websocket_connect("/api/bridge/ws", headers=device_auth) as socket:
        assert socket.receive_json()["type"] == "device.hello"
        assert client.put("/api/bridge/agents", headers=device_auth, json={
            "id": "recommendation", "visibility": "team", "description": "Recommendation",
        }).status_code == 200
        assert client.get("/api/bridge/agents?owner=bob", headers=sender_auth).json()[0]["online"] is True

        payload = {"receiver": "bob", "agent_id": "recommendation",
                   "content": "How does the pipeline work?", "message_id": "a" * 24}
        sent = client.post("/api/bridge/messages", headers=sender_auth, json=payload).json()
        assert sent["status"] == "WAITING_APPROVAL"
        assert socket.receive_json()["type"] == "agent.approval_requested"
        assert client.post(f"/api/requests/{sent['id']}/decision", headers=owner_auth,
                           json={"action": "approve"}).json()["status"] == "WAITING_DEVICE"
        incoming = socket.receive_json()
        assert incoming["type"] == "agent.message"
        assert incoming["sender"] == "alex"
        assert "workspace" not in incoming

        runner = BridgeRunner(state)
        runner.client = LocalClient(client, device_auth)
        outgoing = LocalSocket()
        asyncio.run(runner.handle_message(incoming, outgoing))
        assert outgoing.frames == [{"version": "1", "type": "agent.response",
                                   "message_id": sent["id"], "status": "draft_ready", "error_code": None}]
        pending = read_json(state / "drafts" / (sent["id"] + ".pending.json"))
        assert pending["answer"] == "echo: How does the pipeline work?"
        request = client.get(f"/api/requests/{sent['id']}", headers=sender_auth).json()
        assert request["status"] == "WAITING_OUTPUT_APPROVAL"
        assert request["response"] is None
        assert "workspace" not in json.dumps(request)

        monkeypatch.setattr("peerlink.connector.configured", lambda _: ({}, LocalClient(client, device_auth)))
        review(state, sent["id"], "send")
        completed = client.get(f"/api/requests/{sent['id']}", headers=sender_auth).json()
        assert completed["status"] == "COMPLETED"
        assert completed["response"] == pending["answer"]

    assert client.post("/api/bridge/messages", headers=sender_auth, json={
        "receiver": "bob", "agent_id": "recommendation", "content": "offline?",
    }).status_code == 409


def test_bridge_private_default_and_idempotent_message_id(tmp_path):
    app, client, sender_auth, owner_auth, device, device_auth = setup_team(tmp_path)
    with client.websocket_connect("/api/bridge/ws", headers=device_auth) as socket:
        socket.receive_json()
        assert client.put("/api/bridge/agents", headers=device_auth, json={
            "id": "recommendation", "workspace": "/private/work",
        }).status_code == 422
        assert client.put("/api/bridge/agents", headers=device_auth, json={
            "id": "recommendation", "description": "Private by default",
        }).status_code == 200
        assert client.get("/api/bridge/agents", headers=sender_auth).json() == []
        question = {"receiver": "bob", "agent_id": "recommendation", "content": "secret?",
                    "message_id": "b" * 24}
        assert client.post("/api/bridge/messages", headers=sender_auth, json=question).status_code == 404
        question["content"] = "self test"
        first = client.post("/api/bridge/messages", headers=owner_auth, json=question).json()
        second = client.post("/api/bridge/messages", headers=owner_auth, json=question).json()
        assert first == second
        assert client.post("/api/bridge/messages", headers=owner_auth,
                           json={**question, "content": "different"}).status_code == 409


def test_legacy_project_paths_are_scrubbed_on_migration(tmp_path):
    app, client, sender_auth, owner_auth, device, device_auth = setup_team(tmp_path)
    client.put("/api/projects", headers=device_auth, json={
        "id": "recommendation", "path": "/Users/bob/private/recommendation", "runtime": "mock",
    })
    with app.state.store.connect() as conn:
        assert conn.execute("SELECT path FROM projects WHERE owner='bob'").fetchone()[0] == ""
        conn.execute("UPDATE projects SET path='/Users/bob/old-secret' WHERE owner='bob'")
    create_app(tmp_path / "hub.db")
    with app.state.store.connect() as conn:
        assert conn.execute("SELECT path FROM projects WHERE owner='bob'").fetchone()[0] == ""


def test_bridge_reconnects_after_handshake_failure_and_dropped_socket(tmp_path, monkeypatch):
    save_json(tmp_path / "connector.json", {"hub": "http://127.0.0.1:8000", "id": "device",
                                            "owner": "bob", "token": "test", "projects": {}})
    attempts = []
    recoveries = []
    workers = []

    def work(state, once, auth_dir, *, stop_event):
        workers.append((state, stop_event))
        stop_event.wait(10)

    class DroppedSocket:
        def __aiter__(self):
            raise ConnectionClosedError(None, None)

    @asynccontextmanager
    async def connect(url, **kwargs):
        attempts.append(url)
        if len(attempts) == 1:
            raise InvalidHandshake("Hub restarting")
        if len(attempts) == 2:
            yield DroppedSocket()
        else:
            raise asyncio.CancelledError

    monkeypatch.setattr("websockets.asyncio.client.connect", connect)
    monkeypatch.setattr(BridgeRunner, "recover_drafts", lambda self: recoveries.append(self.state))
    monkeypatch.setattr("peerlink.bridge.client.work", work)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(BridgeRunner(tmp_path).run())
    assert attempts == ["ws://127.0.0.1:8000/api/bridge/ws"] * 3
    assert recoveries == [tmp_path]
    assert len(workers) == 1 and workers[0][0] == tmp_path and workers[0][1].is_set()


def test_bridge_recovers_interrupted_work_and_keeps_answers_local(tmp_path):
    app, client, sender, owner, device, device_auth = setup_team(tmp_path)
    state = tmp_path / "device"
    save_json(state / "connector.json", {"hub": "http://localhost", "id": device["id"],
                                         "owner": "bob", "token": device["token"]})
    runner = BridgeRunner(state)
    runner.client.http.close()
    runner.client = LocalClient(client, device_auth)
    cases = [
        ("a" * 24, "bridge", "RUNNING", None, "FAILED"),
        ("b" * 24, "bridge", "RUNNING", "draft on disk", "WAITING_OUTPUT_APPROVAL"),
        ("c" * 24, "bridge", "WAITING_OUTPUT_APPROVAL", "ready before restart", "WAITING_OUTPUT_APPROVAL"),
        ("d" * 24, "legacy", "RUNNING", None, "RUNNING"),
    ]
    for mid, transport, status, answer, _ in cases:
        with app.state.store.connect() as conn:
            conn.execute("INSERT INTO requests(id,sender,receiver,project,device,question,status,created,"
                         "lease,expires,transport) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                         (mid, "alex", "bob", "recommendation", device["id"], "Hello", status,
                          time.time(), "lease", time.time() + 120, transport))
        task = {"id": mid, "lease": "lease", "transport": transport}
        if answer:
            task["answer"] = answer
        save_json(state / "drafts" / f"{mid}.pending.json", task)
        runner.storage.reserve(mid, "recommendation", "alex")
    runner.recover_drafts()
    runner.recover_drafts()
    for mid, transport, _, answer, expected in cases:
        remote = client.get(f"/api/requests/{mid}", headers=sender).json()
        assert remote["status"] == expected
        assert remote["response"] is None
        path = state / "drafts" / f"{mid}.pending.json"
        if expected == "FAILED":
            assert not path.exists()
            assert path.with_suffix(".stopped").is_file()
        else:
            assert read_json(path).get("answer") == answer


def test_second_bridge_cannot_recover_work_owned_by_running_bridge(tmp_path, monkeypatch):
    save_json(tmp_path / "connector.json", {"hub": "http://localhost", "id": "device",
                                            "owner": "bob", "token": "test"})

    async def exercise():
        running = asyncio.Event()
        release = asyncio.Event()

        async def serve(self, auth_dir):
            running.set()
            await release.wait()

        monkeypatch.setattr(BridgeRunner, "_serve", serve)
        first = asyncio.create_task(BridgeRunner(tmp_path).run())
        await running.wait()
        try:
            with pytest.raises(ValueError, match="Bridge 已在运行"):
                await BridgeRunner(tmp_path).run()
            assert not first.done()
        finally:
            release.set()
            await first
        await BridgeRunner(tmp_path).run()

    asyncio.run(exercise())


def test_real_websocket_bridge_and_http_approval(tmp_path, monkeypatch):
    app, client, sender_auth, owner_auth, device, device_auth = setup_team(tmp_path)
    state = tmp_path / "bridge-state"
    workspace = tmp_path / "project"
    workspace.mkdir()
    add_agent(state, "recommendation", workspace, visibility="team")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    hub = f"http://127.0.0.1:{port}"
    question = "\x1b" * 10000
    save_json(state / "connector.json", {"hub": hub, "id": device["id"],
                                          "owner": "bob", "token": device["token"], "projects": {}})
    monkeypatch.setattr("peerlink.bridge.client.notify", lambda *_: None)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                          log_level="error", access_log=False))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    async def exercise():
        runner = BridgeRunner(state)
        background = asyncio.create_task(runner.run())
        try:
            async with httpx.AsyncClient(base_url=hub, timeout=4) as http:
                for _ in range(100):
                    try:
                        response = await http.get("/api/bridge/agents?owner=bob", headers=sender_auth)
                        if response.status_code == 200 and response.json() and response.json()[0]["online"]:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("Bridge did not connect over a real WebSocket")
                sent = await http.post("/api/bridge/messages", headers=sender_auth,
                                       json={"receiver": "bob", "agent_id": "recommendation",
                                             "content": question, "message_id": "c" * 24})
                assert sent.status_code == 200
                request_id = sent.json()["id"]
                before = await http.get(f"/api/requests/{request_id}", headers=sender_auth)
                assert before.json()["status"] == "WAITING_APPROVAL"
                approved = await http.post(f"/api/requests/{request_id}/decision",
                                           headers=owner_auth, json={"action": "approve"})
                assert approved.status_code == 200
                for _ in range(100):
                    current = await http.get(f"/api/requests/{request_id}", headers=sender_auth)
                    if current.json()["status"] == "WAITING_OUTPUT_APPROVAL":
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("Bridge did not create a reviewable draft")
                assert current.json()["response"] is None
                assert read_json(state / "drafts" / f"{request_id}.pending.json")["answer"] == "echo: " + question
        finally:
            background.cancel()
            with suppress(asyncio.CancelledError):
                await background

    try:
        asyncio.run(exercise())
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_codex_app_server_backend_uses_read_only_protocol(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    executable = tmp_path / "fake-codex"
    executable.write_text("#!" + sys.executable + "\n" + '''
import json
import os
import sys

def receive():
    return json.loads(sys.stdin.readline())

def send(value):
    print(json.dumps(value), flush=True)

assert receive()["method"] == "initialize"
send({"id": 1, "result": {}})
assert receive()["method"] == "initialized"
thread = receive()
assert thread["method"] == "thread/start"
assert thread["params"]["sandbox"] == "read-only"
assert thread["params"]["approvalPolicy"] == "never"
send({"id": 2, "result": {"thread": {"id": "fake-thread"}}})
turn = receive()
assert turn["method"] == "turn/start"
assert turn["params"]["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}
assert turn["params"]["cwd"] == os.getcwd()
send({"id": 3, "result": {"turn": {"id": "fake-turn"}}})
send({"method": "item/completed", "params": {"item": {
    "type": "agentMessage", "phase": "final_answer", "text": "🛰" * 100000}}})
send({"method": "turn/completed", "params": {"turn": {
    "id": "fake-turn", "status": "completed"}}})
''')
    executable.chmod(0o700)
    answer = asyncio.run(CodexAppServerBackend(str(executable)).answer("Question", str(workspace)))
    assert answer.text == "🛰" * 100000
    assert answer.thread_id == "fake-thread"
