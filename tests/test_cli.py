import sys

import httpx
import pytest

from peerlink.bridge.registry import (add_agent, load_agents, rebind_agent, registry_path,
                                      validate_workspace)
from peerlink.cli import discover_bridge_agents, main
from peerlink.connector import save_json


class StubClient:
    def __init__(self, status):
        self.status = status

    def call(self, method, path, **kwargs):
        assert (method, path, kwargs) == (
            "GET", "/api/bridge/agents", {"params": {"owner": "yujunjie.50"}},
        )
        if self.status == 200:
            return [{"id": "ai-outbound", "owner": "yujunjie.50"}]
        response = httpx.Response(self.status, request=httpx.Request("GET", "https://hub/api/bridge/agents"))
        response.raise_for_status()


def test_bridge_discovery_works_on_new_hub():
    assert discover_bridge_agents(StubClient(200), "yujunjie.50")[0]["id"] == "ai-outbound"


def test_bridge_discovery_falls_back_only_for_old_hub():
    assert discover_bridge_agents(StubClient(404), "yujunjie.50") == []
    with pytest.raises(httpx.HTTPStatusError):
        discover_bridge_agents(StubClient(403), "yujunjie.50")


def test_rebind_updates_only_local_workspace_and_preserves_policy(tmp_path, monkeypatch, capsys):
    state = tmp_path / "state"
    wrong = tmp_path / "wrong"
    correct = tmp_path / "correct"
    wrong.mkdir()
    correct.mkdir()
    add_agent(state, "ai-outbound", wrong, visibility="allowlist",
              allowlist=["yujunjie.50"], description="Echo test")
    monkeypatch.setattr("peerlink.cli.configured", lambda state: pytest.fail("rebind must stay local"))
    monkeypatch.setattr(sys, "argv", ["peerlink", "--state", str(state),
                                    "agent", "rebind", "ai-outbound", str(correct)])

    main()

    agent = load_agents(state)["ai-outbound"]
    assert agent == {"id": "ai-outbound", "workspace": str(correct), "backend": "echo",
                     "description": "Echo test", "visibility": "allowlist",
                     "allowlist": ["yujunjie.50"]}
    assert str(wrong) in capsys.readouterr().out


def test_rebind_rejects_unknown_agent_and_broad_or_missing_paths(tmp_path, monkeypatch):
    state = tmp_path / "state"
    home = tmp_path / "Users" / "member"
    home.mkdir(parents=True)
    project = home / "project"
    project.mkdir()
    monkeypatch.setattr("peerlink.bridge.registry.Path.home", lambda: home)

    with pytest.raises(ValueError, match="未注册"):
        rebind_agent(state, "missing", project)
    with pytest.raises(ValueError, match="用户目录集合"):
        validate_workspace(home.parent)
    with pytest.raises(FileNotFoundError):
        validate_workspace(home / "not-found")

    save_json(registry_path(state), {"ai-outbound": {
        "id": "ai-outbound", "workspace": str(home.parent), "backend": "echo",
        "description": "", "visibility": "allowlist", "allowlist": ["yujunjie.50"],
    }})
    old, new = rebind_agent(state, "ai-outbound", project)
    assert (old, new) == (str(home.parent), str(project))
    assert load_agents(state)["ai-outbound"]["allowlist"] == ["yujunjie.50"]
