"""Home control: the one live action a voice call runs itself."""

from __future__ import annotations

import subprocess

import talk_home
import talk_tools


class Runner:
    def __init__(self, returncode=0, stdout="", stderr="", raises=None):
        self.calls: list[list[str]] = []
        self.returncode, self.stdout, self.stderr, self.raises = returncode, stdout, stderr, raises

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        assert kwargs["timeout"] == talk_home.TIMEOUT_S
        if self.raises:
            raise self.raises
        return subprocess.CompletedProcess(argv, self.returncode, self.stdout, self.stderr)


def test_each_device_maps_to_its_cli_call():
    assert talk_home.argv("lights", " Off ") == ["openhome", "lights", "off"]
    assert talk_home.argv("tv", "power") == ["openhome", "ir", "lgtv", "power"]
    assert talk_home.argv("speaker", "volume-up") == ["openhome", "ir", "edifier", "volume-up"]


def test_an_unknown_device_or_an_option_shaped_command_runs_nothing():
    for device, command in (
        ("garage", "open"),
        ("tv", ""),
        ("tv", "--url=http://evil"),
        ("tv", "-h"),
    ):
        runner = Runner()
        assert "unknown device or command" in talk_home.run(
            {"device": device, "command": command}, runner=runner
        )
        assert runner.calls == []


def test_repeat_presses_are_bounded():
    runner = Runner()
    assert talk_home.run({"device": "tv", "command": "volume-up", "repeat": 99}, runner=runner) == (
        f"done: tv volume-up x{talk_home.MAX_REPEAT}"
    )
    assert len(runner.calls) == talk_home.MAX_REPEAT


def test_a_failed_or_silent_press_is_said_and_stops_the_rest():
    runner = Runner(returncode=1, stderr="error: request failed\nAPI said 502")
    reply = talk_home.run({"device": "tv", "command": "mute", "repeat": 3}, runner=runner)
    assert reply == "tv mute failed: API said 502" and len(runner.calls) == 1

    hung = Runner(raises=subprocess.TimeoutExpired("openhome", talk_home.TIMEOUT_S))
    assert "nothing confirmed" in talk_home.run({"device": "lights", "command": "on"}, runner=hung)


def test_advertised_only_where_the_cli_exists(monkeypatch):
    names = lambda: [tool["name"] for tool in talk_tools.default_talk_tools()]  # noqa: E731
    assert "home_control" not in names()
    monkeypatch.setattr(talk_home, "available", lambda: True)
    assert names()[1] == "home_control"
