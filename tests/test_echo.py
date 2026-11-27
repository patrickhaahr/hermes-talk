"""The Echo surface: Wyoming framing, resampling, paced playback, and one wake word end to end."""

from __future__ import annotations

import array
import asyncio
import json
import time

import pytest

import talk_echo
from talk_echo import EchoAudio, Upsampler, encode_event, read_event


@pytest.fixture(autouse=True)
def _say_socket(monkeypatch, tmp_path):
    monkeypatch.setattr(talk_echo, "say_socket_path", lambda: tmp_path / "say.sock")


def _reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def _pcm(samples) -> bytes:
    return array.array("h", samples).tobytes()


def _samples(pcm: bytes) -> list[int]:
    out = array.array("h")
    out.frombytes(pcm)
    return list(out)


def _heard(audio: EchoAudio, pcm: bytes) -> bool:
    """Whether the session hears any of this chunk, once it is through the gate's delay."""

    audio.feed_microphone(pcm)
    for _ in range(20):
        audio.feed_microphone(bytes(1280))
    heard = b""
    while (chunk := audio.read_input_chunk()) is not None:
        heard += chunk
    return any(heard)


# -- framing -------------------------------------------------------------------


def test_reads_hassmics_own_framing():
    # Byte for byte what hassmic's wy_write sends for a microphone chunk.
    data = b'{"rate":16000,"width":2,"channels":1,"timestamp":5}'
    raw = (
        b'{"type":"audio-chunk","version":"1.5.4","data_length":%d,"payload_length":4}\n'
        % len(data)
        + data
        + b"\x01\x00\x02\x00"
    )

    async def go():
        reader = _reader(raw)
        first = await read_event(reader)
        return first, await read_event(reader)

    event, after = asyncio.run(go())
    assert event.type == "audio-chunk"
    assert event.data["rate"] == 16000
    assert event.payload == b"\x01\x00\x02\x00"
    assert after is None


def test_encoded_events_read_back_and_inline_data_is_accepted():
    raw = encode_event("audio-start", {"rate": 24000}) + b'{"type":"ping","data":{"text":"x"}}\n'

    async def go():
        reader = _reader(raw)
        return await read_event(reader), await read_event(reader)

    start, ping = asyncio.run(go())
    assert (start.type, start.data, start.payload) == ("audio-start", {"rate": 24000}, b"")
    assert (ping.type, ping.data) == ("ping", {"text": "x"})


@pytest.mark.parametrize(
    "raw",
    [b"not json\n", b'{"type":"x","payload_length":-1}\n', b'{"type":"x","payload_length":9}\nab'],
)
def test_malformed_events_are_refused(raw):
    async def go():
        return await read_event(_reader(raw))

    with pytest.raises(talk_echo.WyomingError):
        asyncio.run(go())


# -- resampling -------------------------------------------------------------------


def test_upsampler_makes_three_samples_of_two_and_interpolates():
    up = Upsampler()
    out = _samples(up.feed(_pcm([0, 300, 600, 900, 1200])))
    assert out == [0, 200, 400, 600, 800, 1000]
    assert _samples(up.feed(_pcm([1500, 1800]))) == [1200, 1400, 1600]


def test_upsampler_is_continuous_across_any_chunking():
    ramp = _pcm(range(0, 3000, 3))
    whole = Upsampler().feed(ramp)
    up = Upsampler()
    pieces = b"".join(up.feed(ramp[i : i + 7]) for i in range(0, len(ramp), 7))
    assert pieces == whole


# -- paced playback -----------------------------------------------------------------


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _sent(raw: list[bytes]) -> list[talk_echo.WyomingEvent]:
    async def go():
        reader = _reader(b"".join(raw))
        events = []
        while (event := await read_event(reader)) is not None:
            events.append(event)
        return events

    return asyncio.run(go())


def _ms(n: float) -> bytes:
    return b"\x00\x01" * int(talk_echo.RATE * n / 1000)


def test_playback_goes_out_at_the_rate_it_plays_plus_the_lead():
    clock, wire = Clock(), []
    audio = EchoAudio(wire.append, clock=clock)
    audio.queue_playback(_ms(1000), "item")
    audio.pace_once()
    events = _sent(wire)
    assert [e.type for e in events] == ["audio-start", "audio-chunk"]
    assert events[0].data == {"rate": 24000, "width": 2, "channels": 1}
    assert len(events[1].payload) == len(_ms(talk_echo.PLAYBACK_LEAD_S * 1000))

    wire.clear()
    audio.pace_once()  # no time passed: nothing more is due
    assert wire == []
    clock.now += 0.1
    audio.pace_once()
    (chunk,) = _sent(wire)
    assert chunk.type == "audio-chunk" and len(chunk.payload) == len(_ms(100))
    assert audio.playback_pending


def test_barge_in_discards_the_unsent_and_reports_what_was_sent():
    clock, wire = Clock(), []
    audio = EchoAudio(wire.append, clock=clock)
    audio.queue_playback(_ms(2000), "item")
    audio.pace_once()
    for _ in range(35):  # the pacing task's ticks, 10 ms apart
        clock.now += 0.01
        audio.pace_once()
    item, heard_ms = audio.drain_playback()
    assert item == "item" and abs(heard_ms - 500) <= 1  # 150 ms lead + 350 ms played
    assert not audio.playback_pending
    wire.clear()
    clock.now += 1
    audio.pace_once()
    assert wire == []
    assert audio.end_event() == encode_event("audio-stop")


def test_a_session_that_never_spoke_ends_quietly():
    audio = EchoAudio(lambda _: None, clock=Clock())
    event = _sent([audio.end_event()])[0]
    assert (event.type, event.data["code"]) == ("error", talk_echo.QUIET_END_CODE)
    assert _sent([audio.end_event(failed=True)])[0].data["code"] == "talk-session-failed"


def test_quiet_timer_counts_actions_and_playback_not_sound():
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    clock.now += 5
    audio.feed_microphone(_pcm([0] * 1600))
    assert audio.quiet_s() == 5
    audio.feed_microphone(_pcm([8000, -8000] * 800))
    assert audio.quiet_s() == 5
    audio.mark_activity()
    assert audio.quiet_s() == 0
    clock.now += 3
    audio.queue_playback(_ms(100))
    assert audio.quiet_s() == 0


def test_microphone_is_held_until_read_and_dropped_while_paused():
    audio = EchoAudio(lambda _: None, clock=Clock())
    audio.feed_microphone(_pcm([0] * 1024))
    assert audio.read_input_chunk() is None  # one sample short of a frame: the upsampler carries it
    audio.feed_microphone(_pcm([0] * 1024))
    assert len(audio.read_input_chunk()) == talk_echo.GATE_FRAME_BYTES
    assert audio.read_input_chunk() is None
    audio.pause_input()
    audio.feed_microphone(_pcm([0] * 1000))
    audio.resume_input()
    assert audio.read_input_chunk() is None


def test_microphone_backlog_keeps_the_newest_audio():
    audio = EchoAudio(lambda _: None, clock=Clock())
    for _ in range(100):
        audio.feed_microphone(_pcm([0] * 1600))  # 100 ms each
    assert audio.dropped_input_blocks > 0
    held = 0
    while (chunk := audio.read_input_chunk()) is not None:
        held += len(chunk)
    assert held <= talk_echo.MAX_INPUT_BYTES


# -- one wake word, end to end ---------------------------------------------------------


def test_wake_word_runs_a_session_on_the_echo_and_hands_it_back_when_quiet(monkeypatch):
    """A fake Echo speaking hassmic's side of Wyoming over a real socket."""

    monkeypatch.setattr(talk_echo, "PACE_TICK_S", 0.005)
    heard: list[bytes] = []

    async def session(*, audio, lane, announce_queue=None):
        assert lane == "echo"
        audio.start()
        try:
            while sum(map(len, heard)) < 3000:
                if (chunk := audio.read_input_chunk()) is not None:
                    heard.append(chunk)
                await asyncio.sleep(0.005)
            audio.queue_playback(_ms(50), "reply")
            await asyncio.Event().wait()
        finally:
            audio.stop()

    async def go():
        received: list[talk_echo.WyomingEvent] = []
        done = asyncio.Event()

        async def echo(reader, writer):
            while (event := await read_event(reader)) is not None:
                received.append(event)
                if event.type == "run-satellite":
                    writer.write(
                        encode_event("run-pipeline", {"start_stage": "asr", "end_stage": "tts"})
                    )
                    for _ in range(2):
                        writer.write(encode_event("audio-chunk", {"rate": 16000}, _pcm([0] * 1600)))
                    await writer.drain()
                if event.type == "audio-stop":
                    done.set()

        server = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        surface = asyncio.create_task(
            talk_echo.serve("127.0.0.1", port, idle_s=0.3, runner=session)
        )
        try:
            await asyncio.wait_for(done.wait(), 5)
        finally:
            surface.cancel()
            await asyncio.gather(surface, return_exceptions=True)
            server.close()
        return received

    received = asyncio.run(go())
    types = [e.type for e in received]
    assert types[:3] == ["describe", "run-satellite", "time"]
    assert abs(received[2].data["epoch"] - time.time()) < 60  # the Echo's only clock source
    assert types.index("audio-start") < types.index("audio-chunk") < types.index("audio-stop")
    assert sum(len(e.payload) for e in received if e.type == "audio-chunk") == len(_ms(50))
    assert json.loads(json.dumps(received[types.index("audio-start")].data))["rate"] == 24000


def test_while_the_echo_speaks_only_a_voice_louder_than_its_echo_gets_through():
    """The echo canceller's leftover of the reply must not reach the session as the operator."""

    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock, barge_in_level=0.02)
    leftover = _pcm([300, -300] * 501)  # about -40 dBFS
    voice = _pcm([4000, -4000] * 2000)  # about -18 dBFS
    audio.queue_playback(_ms(1000), "reply")
    audio.pace_once()

    assert not _heard(audio, leftover)
    assert _heard(audio, voice)

    clock.now += talk_echo.PLAYBACK_LEAD_S + talk_echo.ECHO_TAIL_S + 0.01
    audio.drain_playback()
    assert _heard(audio, voice)
    assert " gated under 0.02 or" in audio.level_report()


def test_echo_as_loud_as_the_operator_is_held_back_by_the_reply_level():
    """After the operator talks over it, the canceller can leak the reply at its own level."""

    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock, barge_in_level=0.02)
    reply = _pcm([3300, -3300] * 2400)  # about -20 dBFS, a normal reply
    leak = _pcm([4000, -4000] * 501)  # the reply coming back, a little louder
    shout = _pcm([12000, -12000] * 2000)
    audio.queue_playback(reply, "reply")
    audio.pace_once()

    assert not _heard(audio, leak)
    assert _heard(audio, shout)

    audio.drain_playback()
    clock.now += talk_echo.PLAYBACK_LEAD_S + 0.45  # the room still carries the reply back
    assert not _heard(audio, leak)


def test_wake_word_during_the_reply_stops_it_and_opens_the_microphone():
    clock, wire = Clock(), []
    audio = EchoAudio(wire.append, clock=clock, barge_in_level=0.02)
    audio.queue_playback(_pcm([3300, -3300] * 24000), "reply")
    audio.pace_once()

    audio.cut_reply()
    assert not audio.playback_pending
    audio.queue_playback(_ms(500), "reply")  # the rest of it, still streaming in
    assert not audio.playback_pending
    assert _heard(audio, _pcm([4000, -4000] * 2000))  # "stop", over what the Echo still plays

    assert audio.drain_playback()[0] == "reply", "the session still learns what was heard"
    audio.queue_playback(_ms(500), "next")
    assert audio.playback_pending


# -- standby, end_call, echo-say ---------------------------------------------------------


def _fake_echo(on_event):
    """A fake hassmic: answers run-satellite, records events, and lets the test act on each."""

    received: list[talk_echo.WyomingEvent] = []

    async def echo(reader, writer):
        while (event := await read_event(reader)) is not None:
            received.append(event)
            await on_event(event, writer)

    return echo, received


async def _until(predicate):
    for _ in range(300):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never held")


def _wake(writer):
    writer.write(encode_event("run-pipeline", {"start_stage": "asr", "end_stage": "tts"}))
    writer.write(encode_event("audio-chunk", {"rate": 16000}, _pcm([0] * 1600)))


def test_a_standby_session_is_connected_before_the_wake_word_and_takes_the_call(monkeypatch):
    """The first words reach a session that is already live; the next one is ready after."""

    monkeypatch.setattr(talk_echo, "PACE_TICK_S", 0.005)
    started: list[float] = []
    loop_time = []

    async def session(*, audio, lane, announce_queue=None):
        started.append(asyncio.get_running_loop().time())
        audio.start()
        try:
            while audio.read_input_chunk() is None:
                await asyncio.sleep(0.005)
            audio.queue_playback(_ms(20), "reply")
            await asyncio.Event().wait()
        finally:
            audio.stop()

    async def go():
        stopped = asyncio.Event()

        async def on_event(event, writer):
            if event.type == "run-satellite":
                await asyncio.sleep(0.2)  # the standby connects while the room is quiet
                loop_time.append(asyncio.get_running_loop().time())
                _wake(writer)
                await writer.drain()
            if event.type == "audio-stop":
                stopped.set()

        echo, received = _fake_echo(on_event)
        server = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        surface = asyncio.create_task(
            talk_echo.serve("127.0.0.1", port, idle_s=0.2, runner=session)
        )
        try:
            await asyncio.wait_for(stopped.wait(), 5)
            await asyncio.sleep(0.8)  # the next standby
        finally:
            surface.cancel()
            await asyncio.gather(surface, return_exceptions=True)
            server.close()
        return received

    received = asyncio.run(go())
    assert len(started) == 2
    assert started[0] < loop_time[0], "the call's session started before the wake word"
    assert [e.type for e in received].count("audio-stop") == 1


@pytest.mark.parametrize(("wakes", "ends"), [([], True), (["run-pipeline"], False),
                                             (["detection"], False)])
def test_a_standby_that_speaks_on_its_own_ends_soon_unless_woken(monkeypatch, wakes, ends):
    monkeypatch.setattr(talk_echo, "UNWOKEN_IDLE_S", 0.3)
    monkeypatch.setattr(talk_echo, "PACE_TICK_S", 0.005)

    async def session(*, audio, lane, announce_queue=None):
        audio.start()
        try:
            await asyncio.sleep(0.05)
            audio.queue_playback(_ms(20), "news")  # a result that landed while nobody listened
            await asyncio.Event().wait()
        finally:
            audio.stop()

    async def go():
        stopped = asyncio.Event()

        async def on_event(event, writer):
            if event.type == "audio-start":
                for wake in wakes:
                    writer.write(encode_event(wake))
            if event.type == "audio-stop":
                stopped.set()

        echo, received = _fake_echo(on_event)
        server = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        surface = asyncio.create_task(
            talk_echo.serve("127.0.0.1", port, idle_s=30, runner=session)
        )
        try:
            await asyncio.wait({asyncio.ensure_future(stopped.wait())}, timeout=1.5)
        finally:
            surface.cancel()
            await asyncio.gather(surface, return_exceptions=True)
            server.close()
        return stopped.is_set(), [e.type for e in received]

    stopped, types = asyncio.run(go())
    assert "audio-start" in types
    assert stopped is ends


def test_a_result_landing_after_the_call_restarts_the_standby_to_pick_it_up(monkeypatch):
    """A new session adopts owed results when it starts, so the standby restarts for one."""

    monkeypatch.setattr(talk_echo, "STANDBY_REFRESH_S", 60)
    landed = []
    monkeypatch.setattr(
        talk_echo.talk_runs, "owed_since",
        lambda session_id, since: session_id == talk_echo.RETURN_SESSION_ID and bool(landed),
    )
    started: list[int] = []

    async def session(*, audio, lane, announce_queue=None):
        started.append(1)
        if len(started) == 2:
            landed.clear()  # the new standby adopted it
        audio.start()
        try:
            await asyncio.Event().wait()
        finally:
            audio.stop()

    async def go():
        echo, _ = _fake_echo(lambda event, writer: asyncio.sleep(0))
        server = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        surface = asyncio.create_task(
            talk_echo.serve("127.0.0.1", port, idle_s=0.2, runner=session)
        )
        try:
            await _until(lambda: len(started) == 1)
            landed.append(1)
            await _until(lambda: len(started) == 2)
            await asyncio.sleep(0.8)
        finally:
            surface.cancel()
            await asyncio.gather(surface, return_exceptions=True)
            server.close()

    asyncio.run(go())
    assert len(started) == 2


def test_echo_say_reaches_the_standby_session_as_an_announcement(monkeypatch):
    got: list = []

    async def session(*, audio, lane, announce_queue=None):
        got.append(await announce_queue.get())  # the call's clock
        got.append(await announce_queue.get())
        await asyncio.Event().wait()

    async def go():
        async def on_event(event, writer):
            pass

        echo, _ = _fake_echo(on_event)
        server = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        surface = asyncio.create_task(talk_echo.serve("127.0.0.1", port, runner=session))
        try:
            await asyncio.sleep(0.2)
            answer = await asyncio.to_thread(talk_echo.send_say, "the  laundry\nis done")
            for _ in range(100):
                if got:
                    break
                await asyncio.sleep(0.01)
        finally:
            surface.cancel()
            await asyncio.gather(surface, return_exceptions=True)
            server.close()
        return answer

    assert asyncio.run(go()) == "ok"
    assert len(got) == 2
    assert "from the host's clock" in got[0][0].text
    assert any("the laundry is done" in getattr(c, "text", "") for c in got[1])


def test_end_call_waits_for_the_goodbye_then_hangs_up():
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    assert not audio.hung_up()
    audio.request_hang_up()
    audio.queue_playback(_ms(4000), "bye")
    audio.pace_once()
    clock.now += talk_echo.HANG_UP_MIN_S
    assert not audio.hung_up(), "the goodbye is still playing"
    audio.drain_playback()
    clock.now += talk_echo.PLAYBACK_LEAD_S + talk_echo.ECHO_TAIL_S
    assert audio.hung_up()


def test_a_handoff_is_acknowledged_and_closed_once_the_operator_is_quiet(monkeypatch):
    import talk_tools

    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    monkeypatch.setattr(talk_echo, "_calling", audio)
    started = {"refused": "delegate_task needs a task", "ok": "WORK_STARTED run 1"}
    monkeypatch.setitem(talk_tools._HANDLERS, "delegate_task", lambda a: started[a["task"]])

    talk_tools.execute_talk_tool("delegate_task", {"task": "refused"})
    clock.now += talk_echo.HANDOFF_QUIET_S
    assert not audio.take_settled_handoff(), "nothing was handed off"

    assert talk_tools.scripted_reply(
        "delegate_task", talk_tools.execute_talk_tool("delegate_task", {"task": "ok"})
    ) == "", "'On it.' would cover the next command"
    clock.now += talk_echo.HANDOFF_QUIET_S - 0.5
    for _ in range(60):
        audio.feed_microphone(_pcm([9000, -9000] * 128))
        clock.now += 0.016
    clock.now += talk_echo.HANDOFF_QUIET_S - 0.5
    assert not audio.take_settled_handoff(), "the operator went on"
    clock.now += 0.6
    assert audio.take_settled_handoff()
    assert not audio.take_settled_handoff(), "said once"

    audio.request_hang_up(after_reply=True)
    clock.now += talk_echo.HANG_UP_MIN_S * 3
    assert not audio.hung_up(), "'On it.' has not been said yet"
    audio.queue_playback(_ms(400), "on-it")
    audio.pace_once()
    audio.drain_playback()
    clock.now += talk_echo.PLAYBACK_LEAD_S + talk_echo.ECHO_TAIL_S
    assert audio.hung_up()


def test_handoff_skips_delayed_ack_when_reply_already_spoke(monkeypatch):
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    monkeypatch.setattr(talk_echo, "_calling", audio)
    audio.handed_off()
    clock.now += 0.5
    audio.queue_playback(_ms(400), "combo-time-and-on-it")
    audio.pace_once()
    clock.now += talk_echo.HANDOFF_QUIET_S + 1.0
    assert not audio.take_settled_handoff()

    audio.handed_off()
    talk_echo.run_get_time({})
    clock.now += talk_echo.HANDOFF_QUIET_S + 1.0
    assert not audio.take_settled_handoff()


def test_end_call_accepts_garbled_barge_in_right_after_reply(monkeypatch):
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    monkeypatch.setattr(talk_echo, "_calling", audio)
    monkeypatch.setattr(talk_echo, "GOODBYE_WAIT_S", 0.1)
    audio.queue_playback(_ms(500), "news")
    audio.pace_once()
    clock.now += 0.8
    audio.operator_said("Hello.")
    assert talk_echo.run_end_call({}) == talk_echo.HANGING_UP


@pytest.mark.parametrize(
    ("heard", "hangs_up"),
    [("Farvel.", True), ("Tovel.", True), ("Forvel farve.", True), ("Thank you, bye-bye.", True),
     ("CV.", False), ("Simba.", False), ("You...", False), ("Alright.", False), ("Hvordan?", False),
     ("Tænd TV, tilbage.", False)],
)
def test_end_call_needs_a_goodbye_the_transcriber_heard_too(monkeypatch, heard, hangs_up):
    audio = EchoAudio(lambda _: None)
    monkeypatch.setattr(talk_echo, "_calling", audio)
    monkeypatch.setattr(talk_echo, "GOODBYE_WAIT_S", 0.1)
    audio.operator_said(heard)
    reply = talk_echo.run_end_call({})
    assert (reply == talk_echo.HANGING_UP) is hangs_up
    assert talk_echo.run_end_call({}) == talk_echo.HANGING_UP, "asked again, the operator insists"


@pytest.mark.parametrize(
    ("heard", "hangs_up"),
    [("Bye-bye.", True), ("Okay, that's it.", True), ("Tak, farvel.", True), ("Stop.", True),
     ("Okay, stop.", True), ("Slut.", True), ("Hold op.", True), ("Okay, tak.", True),
     ("Stop the music.", False), ("Stop work.", False),
     ("Play.", False), ("How do you say goodbye in Danish?", False)],
)
def test_a_plain_goodbye_hangs_up_without_the_model(heard, hangs_up):
    audio = EchoAudio(lambda _: None)
    audio.operator_said(heard)
    assert (audio._hang_up_at is not None) is hangs_up


def test_a_goodbye_without_a_transcript_hangs_up(monkeypatch):
    audio = EchoAudio(lambda _: None)
    monkeypatch.setattr(talk_echo, "_calling", audio)
    monkeypatch.setattr(talk_echo, "GOODBYE_WAIT_S", 0.1)
    for _ in range(40):
        audio.feed_microphone(_pcm([9000, -9000] * 128))
    assert talk_echo.run_end_call({}) == talk_echo.HANGING_UP


def test_the_room_does_not_keep_the_call_open_but_someone_speaking_puts_the_end_off():
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    audio.call_started()

    def hear(level, seconds):
        for _ in range(int(seconds / 0.016)):
            audio.feed_microphone(_pcm([level, -level] * 128))
            clock.now += 0.016

    hear(900, 10.5)
    assert audio.idle(10), "a room of 0.027 is no activity"
    audio.mark_activity()
    hear(900, 9.0)
    hear(9000, 1.5)
    assert not audio.idle(10)
    hear(900, 10)
    assert audio.idle(10)


def test_sorry_is_asked_once_a_call_and_not_in_one_nobody_woke(monkeypatch):
    import talk_tools

    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    monkeypatch.setattr(talk_echo, "_calling", audio)
    def say(danish):
        output = talk_tools.execute_talk_tool("unclear", {"danish": danish})
        return talk_tools.scripted_reply("unclear", output)

    said = [say(True), say(False)]
    assert said == ["Hvad siger du?", ""]
    clock.now += talk_echo.UNCLEAR_COOLDOWN_S
    for _ in range(100):
        audio.feed_microphone(_pcm([9000, -9000] * 128))
        clock.now += 0.016
    assert say(False) == "Sorry?"
    assert say(False) == ""
    audio.allow_question(False)
    clock.now += talk_echo.UNCLEAR_COOLDOWN_S
    for _ in range(100):
        audio.feed_microphone(_pcm([9000, -9000] * 128))
        clock.now += 0.016
    assert talk_echo.run_unclear({"danish": False}) == talk_echo.NOT_ASKED


def test_silent_reply_audio_does_not_gate_the_microphone():
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock, barge_in_level=0.02)
    for _ in range(10):
        audio.feed_microphone(_pcm([100, 100, -100, -100] * 256))
    audio.queue_playback(bytes(talk_echo.RATE * 2), "silent")
    audio.pace_once()
    soft = _pcm([550, 550, -550, -550] * 1000)
    assert _heard(audio, soft)


def test_end_call_plays_the_goodbye_then_the_bye_tone_then_hands_back(monkeypatch):
    monkeypatch.setattr(talk_echo, "PACE_TICK_S", 0.005)

    async def session(*, audio, lane, announce_queue=None):
        audio.start()
        try:
            while audio.read_input_chunk() is None:
                await asyncio.sleep(0.005)
            audio.queue_playback(_ms(50), "bye")
            audio.operator_said("Farvel.")
            talk_echo.run_end_call({})
            await asyncio.Event().wait()
        finally:
            audio.stop()

    async def go():
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        times = {}

        async def on_event(event, writer):
            if event.type == "run-satellite":
                await asyncio.sleep(0.1)
                _wake(writer)
                await writer.drain()
                times["woke"] = loop.time()
            if event.type == "audio-stop":
                times["stopped"] = loop.time()
                stopped.set()

        echo, received = _fake_echo(on_event)
        server = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        surface = asyncio.create_task(
            talk_echo.serve("127.0.0.1", port, idle_s=30, runner=session)
        )
        try:
            await asyncio.wait_for(stopped.wait(), 5)
        finally:
            surface.cancel()
            await asyncio.gather(surface, return_exceptions=True)
            server.close()
        return received, times

    received, times = asyncio.run(go())
    played = b"".join(e.payload for e in received if e.type == "audio-chunk")
    assert played == _ms(50) + talk_echo.BYE_TONE
    assert times["stopped"] - times["woke"] < 1.5, "hung up soon after the goodbye"


def test_end_call_never_waits_past_its_bound_even_if_the_model_keeps_talking():
    clock = Clock()
    audio = EchoAudio(lambda _: None, clock=clock)
    audio.request_hang_up()
    audio.queue_playback(_ms(20_000), "rambling")
    clock.now += talk_echo.HANG_UP_MAX_S
    assert audio.hung_up()


def test_end_call_without_a_call_says_so():
    assert "no call" in talk_echo.run_end_call({})


def test_end_call_is_offered_only_to_a_surface_that_can_hang_up():
    import talk_operator_auth
    import talk_tools

    def names(**kw):
        return [t["name"] for t in talk_tools.default_talk_tools(**kw)]

    assert "end_call" in names(hangable=True)
    assert "end_call" not in names()
    assert "get_time" in names(hangable=True)
    assert "get_time" not in names()
    # A misheard room fragment once made the speaker recite the whole skill catalog.
    assert "talk_capabilities" in names()
    assert "talk_capabilities" not in names(hangable=True)
    assert talk_tools._HANDLERS["end_call"] is talk_echo.run_end_call
    assert talk_tools._HANDLERS["get_time"] is talk_echo.run_get_time
    assert "end_call" in talk_operator_auth.READ_ONLY_TALK_TOOLS
    assert "get_time" in talk_operator_auth.READ_ONLY_TALK_TOOLS


def test_a_standby_taking_the_call_learns_the_time_it_started():
    """The standby's prompt clock can be 15 minutes old; the wake word brings it up to date."""

    import talk_identity
    import talk_realtime

    async def go():
        queues = []

        async def session(*, audio, lane, announce_queue=None):
            queues.append(announce_queue)
            await asyncio.Event().wait()

        conversation = talk_echo._Conversation(
            lambda data: None, session, 20.0, lambda c, failed: None, active=False
        )
        await asyncio.sleep(0)
        assert queues[0].empty()
        conversation.activate()
        batch = queues[0].get_nowait()
        await conversation.close(notify_echo=False)
        return batch

    batch = asyncio.run(go())
    assert [type(command) for command in batch] == [talk_realtime.AddContext]
    assert talk_identity.current_moment() in batch[0].text


def _frames_heard(levels, piece_bytes):
    gate = talk_echo.TalkerGate()
    frame_samples = talk_echo.GATE_FRAME_BYTES // 2
    pcm = b"".join(_pcm([round(level * 32768)] * frame_samples) for level in levels)
    pieces = [pcm[i : i + piece_bytes] for i in range(0, len(pcm), piece_bytes)]
    heard = b"".join(gate.feed(piece) for piece in pieces)
    size = talk_echo.GATE_FRAME_BYTES
    assert len(heard) == len(pcm) // size * size
    return [i // size - talk_echo.GATE_ONSET_FRAMES for i in range(0, len(heard), size)
            if any(heard[i : i + size])]


HASSMIC_PIECE = 768  # 16 ms, what the Echo 2 sends


@pytest.mark.parametrize("piece", [HASSMIC_PIECE, 1000, 3072])
def test_the_room_behind_the_talker_is_silenced_and_words_keep_their_edges(piece):
    music, word = 0.011, 0.05
    assert _frames_heard([music] * 10 + [word] * 3 + [music] * 12, piece) == list(range(6, 17))


@pytest.mark.parametrize("knock", [[0.3], [0.3, 0.3], [0.3, 0.003, 0.3]])
def test_a_knock_does_not_open_the_gate(knock):
    assert _frames_heard([0.003] * 10 + knock + [0.003] * 12, HASSMIC_PIECE) == []


def test_a_soft_command_is_heard_in_a_quiet_room_and_the_same_level_of_music_is_not():
    quiet, soft = 0.003, 0.015
    assert _frames_heard([quiet] * 10 + [soft] * 3 + [quiet] * 12, HASSMIC_PIECE) == list(range(6, 17))
    music = 0.011
    assert _frames_heard([music] * 10 + [soft] * 3 + [music] * 12, HASSMIC_PIECE) == []


@pytest.mark.parametrize("word", [
    [0.0017, 0.0059, 0.0061, 0.022, 0.0141, 0.0023, 0.0075, 0.0132, 0.0054, 0.0067, 0.0061],
    [0.0022, 0.0062, 0.0126, 0.022, 0.0100, 0.0040, 0.0040, 0.0101, 0.0114, 0.0043, 0.0062],
    [0.0043, 0.0129, 0.022, 0.0061, 0.0045, 0.0094, 0.0157, 0.0058, 0.0091, 0.0072, 0.0098],
])
def test_soft_danish_light_commands_are_heard_whole_around_their_silent_k(word):
    """Frame levels of "sluk lyset" and "tænd lyset" by two Danish voices at a casual volume."""

    heard = _frames_heard([0.004] * 20 + word + [0.004] * 12, HASSMIC_PIECE)
    assert set(range(22, 20 + len(word))) <= set(heard)


def test_a_command_softer_than_the_wake_word_reaches_the_session_louder():
    gate = talk_echo.TalkerGate()
    size = talk_echo.GATE_FRAME_BYTES // 2
    heard = gate.feed(_pcm([300] * size * 10 + [700] * size * 4 + [300] * size * 12))
    assert max(_samples(heard)) == 700 * talk_echo.TALKER_LIFT
    loud = talk_echo.TalkerGate().feed(_pcm([20000] * size * 8))
    assert max(_samples(loud)) == 32767


def test_the_end_of_the_wake_word_is_dropped_and_the_command_after_it_is_not():
    size = talk_echo.GATE_FRAME_BYTES // 2
    tail, command, room = 2000, 1500, 200

    def heard(*stretches):
        gate = talk_echo.TalkerGate()
        gate.skip_wake_word()
        levels = [level for level, frames in stretches for _ in range(frames)]
        out = _samples(gate.feed(_pcm([s for level in levels for s in [level] * size])))
        return {s // talk_echo.TALKER_LIFT for s in out}

    paused = heard((0, 12), (tail, 5), (room, 5), (command, 6), (room, 12))
    assert tail not in paused and command in paused
    one_breath = heard((0, 12), (tail, 7), (command, 8), (room, 12))
    assert tail not in one_breath and command in one_breath
    waited = heard((0, 12), (room, 4), (command, 6), (room, 12))
    assert command in waited


def test_a_loud_room_stays_shut_out_and_a_command_over_it_gets_in():
    room, command = 0.026, 0.07
    heard = _frames_heard(
        [0.0] * 12 + [room] * 30 + [command] * 6 + [room] * 30, HASSMIC_PIECE
    )
    assert set(range(42, 48)) <= set(heard)
    assert not set(range(33, 36)) & set(heard)
    assert max(heard) < 55
