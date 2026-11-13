"""GPT-Live on the Echo: delegation routing, output gating, and one session end to end."""

from __future__ import annotations

import array
import asyncio
from types import SimpleNamespace

import pytest

import talk_echo_live
import talk_realtime as rt
import talk_runs
from talk_echo_live import OutputShaper, home_arguments


def _pcm(level: int, samples: int = 480) -> bytes:
    return array.array("h", [level] * samples).tobytes()


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        ("Turn off the lights", ("lights", "off", 1)),
        ("Switch the lamp on.", ("lights", "on", 1)),
        ("Turn the TV volume up three steps", ("tv", "volume-up", 3)),
        ("Make the speaker quieter by 2", ("speaker", "volume-down", 2)),
        ("Turn the TV off", ("tv", "power", 1)),
        ("Switch the TV to HDMI 2", ("tv", "hdmi-2", 1)),
        ("Mute the TV", ("tv", "mute", 1)),
        ("Turn up the volume on the TV twice", ("tv", "volume-up", 2)),
        ("Turn the speaker to bluetooth", ("speaker", "bluetooth", 1)),
        ("Turn the lights and the TV off", None),
        ("Turn the lights on and off", None),
        ("Ask Hermes to check how much disk space is free on Zaza", None),
        ("Open the garage", None),
        (None, None),
    ],
)
def test_home_arguments(prompt, expected):
    found = home_arguments(prompt)
    assert (found and (found["device"], found["command"], found["repeat"])) == expected


@pytest.mark.parametrize(
    ("prompt", "hangs_up"),
    [("Hang up", True), ("Can you turn off the call? Like, bye", True), ("Goodbye!", True),
     ("End the conversation", True), ("Turn off the lights", False),
     ("How much storage is left", False)],
)
def test_end_call_phrases(prompt, hangs_up):
    assert bool(talk_echo_live.END_CALL_PROMPT.search(prompt)) is hangs_up


@pytest.mark.parametrize(
    ("prompt", "clock"),
    [("Yes, it is. Uh, what time is it", True), ("Okay, how much time is it", True),
     ("What's the date today?", True), ("How much disk space is left", False)],
)
def test_clock_questions(prompt, clock):
    assert bool(talk_echo_live.CLOCK_PROMPT.search(prompt)) is clock


@pytest.mark.parametrize(
    ("said", "goodbye"),
    [("Okay, bye", True), ("Thanks, goodbye!", True), ("That's all, you can hang up", True),
     ("Can you remind me to say goodbye to grandma before she leaves tomorrow", False),
     ("Turn off the lights", False)],
)
def test_is_goodbye(said, goodbye):
    assert talk_echo_live.is_goodbye(said) is goodbye


def test_output_shaper_holds_a_cushion_keeps_word_pauses_and_drops_silence():
    frame = len(_pcm(0))
    shaper = OutputShaper(tail_bytes=frame * 2, prebuffer_bytes=frame * 2, gain=2.0)
    assert shaper.feed(_pcm(0)) == b""
    assert shaper.feed(_pcm(4000)) == b""
    assert shaper.feed(_pcm(5000)) == _pcm(8000) + _pcm(10000)
    assert shaper.feed(_pcm(0)) == _pcm(0)
    assert shaper.feed(_pcm(20000)) == _pcm(32767)
    assert shaper.feed(_pcm(0)) + shaper.feed(_pcm(0)) == _pcm(0) * 2
    assert shaper.feed(_pcm(0)) == b""


def test_output_shaper_plays_a_reply_shorter_than_the_cushion():
    frame = len(_pcm(0))
    shaper = OutputShaper(tail_bytes=frame, prebuffer_bytes=frame * 10, gain=1.0)
    assert shaper.feed(_pcm(3000)) + shaper.feed(_pcm(0)) == b""
    assert shaper.feed(_pcm(0)) == _pcm(3000) + _pcm(0)


class FakeLive:
    def __init__(self):
        self.sent: list = []
        self.events: asyncio.Queue = asyncio.Queue()
        self.closed = False

    async def connect(self, setup):
        self.setup = setup

    async def send(self, commands):
        self.sent.extend(commands)

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.events.get()

    async def close(self):
        self.closed = True


class FakeAudio:
    def __init__(self):
        self.played: list[bytes] = []
        self.hung_up = False
        self.mic = [b"\x01\x00" * 480]

    def start(self):
        pass

    def stop(self):
        pass

    def read_input_chunk(self):
        return self.mic.pop() if self.mic else None

    def queue_playback(self, pcm, item_id=None):
        self.played.append(pcm)

    def request_hang_up(self):
        self.hung_up = True


async def _until(predicate):
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never held")


def test_live_session_routes_delegations_and_gates_silence(monkeypatch):
    monkeypatch.setattr(talk_echo_live, "resolve_live_config",
                        lambda: SimpleNamespace(model="gpt-live-1-codex", voice="sol"))
    monkeypatch.setattr(talk_echo_live.talk_identity, "build_instructions", lambda *a, **k: "base")
    owners = []
    monkeypatch.setattr(talk_echo_live.talk_host, "host", lambda: SimpleNamespace(
        identity_sections=lambda: {},
        run_agent=lambda prompt, background: owners.append(talk_runs.current_owner())
        or "About 120 GB free."))
    homes = []
    monkeypatch.setattr(talk_echo_live.talk_home, "run",
                        lambda arguments: homes.append(arguments) or "done: lights off")
    live, audio, says = FakeLive(), FakeAudio(), asyncio.Queue()

    async def scenario():
        task = asyncio.create_task(talk_echo_live.run_live_session(
            audio=audio, lane="echo", announce_queue=says, session_factory=lambda: live))
        await _until(lambda: any(isinstance(c, rt.AppendInputAudio) for c in live.sent))
        await live.events.put(rt.OutputAudio(_pcm(0, 24000)))
        await live.events.put(rt.OutputAudio(_pcm(3000)))
        await live.events.put(rt.OutputAudio(_pcm(0, 24000)))
        await live.events.put(rt.DelegationRequested("d1", prompt="Turn off the lights"))
        await live.events.put(rt.DelegationRequested("d3", prompt="How much storage is left"))
        await live.events.put(rt.DelegationRequested("d2", prompt="Hang up"))
        await says.put([rt.AddContext(item_id="talksay1", text="Dinner is ready."),
                        rt.StartResponse(allow_tools=False)])
        await _until(lambda: len([c for c in live.sent
                                  if isinstance(c, rt.SubmitDelegationResult)]) == 3)
        await _until(lambda: any(isinstance(c, rt.AppendLiveContext) for c in live.sent))
        await live.events.put(rt.SessionTerminated(rt.SessionState.CLOSED))
        return await task

    assert asyncio.run(scenario()) == 0
    assert "Turn off the lights" in live.setup.instructions
    # Silence before the reply is dropped; the reply comes out louder, then its tail.
    assert audio.played == [_pcm(3750) + _pcm(0, 24000)]
    assert homes == [{"device": "lights", "command": "off", "repeat": 1}]
    assert audio.hung_up
    results = {c.delegation_id: c.content for c in live.sent
               if isinstance(c, rt.SubmitDelegationResult)}
    assert results["d1"] == "done: lights off"
    assert results["d3"] == "About 120 GB free."
    assert owners and owners[0] is not None
    assert talk_runs.current_owner() is None
    assert [c.content for c in live.sent if isinstance(c, rt.AppendLiveContext)
            and c.kind == "message"] == [
        "Dinner is ready."]
    assert live.closed
