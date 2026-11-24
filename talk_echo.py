"""An Amazon Echo running echo-dot-assist's hassmic (Wyoming mode) as a Talk surface.

hassmic keeps Amazon's audio front end (echo canceller, beamforming) and wake
word engine and serves the Wyoming satellite protocol on TCP 16700. This
module is the client that Home Assistant would otherwise be: it connects to
the Echo, and each wake word starts one Talk session whose microphone is the
Echo's and whose speaker is the Echo's. The session ends itself after a quiet
spell, and the Echo goes back to waiting for its wake word.

Duplex needs hassmic to keep streaming the microphone while it speaks; stock
hassmic stops it at the first reply, which leaves one exchange per wake word.

Between calls one session waits connected (standby), so a wake word starts
talking to a live model instead of spending a mint and a connect (~1.3 s, ~3 s
in a cold process) while the operator is already speaking. Whatever that
standby session says on its own (a background result that landed, or
``hermes talk echo-say``) makes it the call: the Echo speaks first.
"""

from __future__ import annotations

import array
import asyncio
import contextlib
import functools
import json
import logging
import math
import os
import socket
import statistics
import threading
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    from . import talk_audio, talk_runs
except ImportError:  # pragma: no cover - flat-module fallback (Hermes file-path load)
    import talk_audio
    import talk_runs

_log = logging.getLogger(__name__)

LANE = "echo"
#: The Echo has no parent Hermes session. Work it delegates is filed under
#: this id instead, so the next session adopts a result that lands after the
#: call that asked for it has ended.
RETURN_SESSION_ID = "talk-echo"
DEFAULT_PORT = 16700
DEFAULT_IDLE_S = 20.0

#: hassmic's microphone stream, fixed in its proto_wyoming.c.
MIC_RATE = 16_000
RATE = talk_audio.SAMPLE_RATE
FRAME_BYTES = talk_audio.FRAME_BYTES

#: Audio handed to the Echo ahead of the clock. Barge-in cannot recall what
#: was already sent, so this is also how long the Echo talks over the
#: operator after an interruption: enough to ride out network jitter, short
#: enough not to be heard as ignoring them.
PLAYBACK_LEAD_S = 0.15
PACE_TICK_S = 0.02
#: Captured audio held while the provider session is still connecting after
#: the wake word, so the request said in one breath is not lost.
MAX_INPUT_BYTES = RATE * FRAME_BYTES * 5
MAX_PLAYBACK_BYTES = talk_audio.MAX_PLAYBACK_BYTES
#: Microphone level (normalized RMS) that counts as someone talking, for the
#: quiet timer only. hassmic brings speech to about -26 dBFS (0.05).
SPEECH_LEVEL = 0.02
GATE_FRAME_S = 0.064
GATE_FRAME_BYTES = int(RATE * GATE_FRAME_S) * FRAME_BYTES
GATE_ONSET_FRAMES = 3
GATE_HANG_FRAMES = 4
GATE_LEVEL_MIN = 0.008
GATE_LEVEL_MAX = SPEECH_LEVEL
GATE_OVER_ROOM = 1.5
GATE_ROOM_FRAMES = 47
TALKER_LIFT = 3
GATE_WAKE_WORD_FRAMES = 9
#: While the Echo speaks, quieter microphone audio goes to the session as
#: silence: what Amazon's echo canceller leaves of the reply would otherwise
#: be heard as the operator, and answered. Someone talking over the reply is
#: louder and still cuts it. Normalized RMS; 0.02 is -34 dBFS.
DEFAULT_BARGE_IN_LEVEL = 0.02
#: The canceller's leftover is not always quiet: after the operator talks over
#: a reply it re-adapts for seconds, and meanwhile hands back the reply at up to
#: its own level (measured 1.2x, 2026-11-16). So the floor above also scales
#: with the reply: the operator counts only above this multiple of the loudest
#: reply audio sent within ECHO_SPAN_S, which covers its trip back to the mic.
ECHO_RATIO = 2.0
ECHO_SPAN_S = 0.8
#: How long the reply still comes back after its last audio was sent: the trip
#: to the mic takes 0.3-0.45 s, and the room rings on. A leftover let through
#: here is answered as the operator, often as their goodbye.
ECHO_TAIL_S = 0.6

CONNECT_TIMEOUT_S = 10.0
LINK_PROBE_IDLE_S = 15
LINK_PROBE_EVERY_S = 5
LINK_DEAD_AFTER_S = 30
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0
MAX_HEADER_BYTES = 64 * 1024
MAX_PAYLOAD_BYTES = 1 << 20

#: Ends a run without the error animation (hassmic: core_no_speech).
QUIET_END_CODE = "stt-no-text-recognized"

#: A standby session is replaced after this long, since provider sessions are
#: capped (OpenAI Realtime at 60 min). It is also replaced as soon as a result
#: lands for :data:`RETURN_SESSION_ID`: a session adopts those when it starts.
STANDBY_REFRESH_S = 15 * 60
STANDBY_RETRY_MIN_S = 2.0
STANDBY_RETRY_MAX_S = 60.0
#: The goodbye is said in the same turn as end_call, so the call closes once
#: the Echo is silent, never sooner than this: a goodbye a moment late still plays.
HANG_UP_MIN_S = 0.5
HANG_UP_MAX_S = 10.0
#: ``hermes talk echo-say`` reaches the running surface through this socket,
#: under the Hermes home (only its owner may connect).
SAY_SOCKET = "talk-echo.sock"
MAX_SAY_CHARS = 1_000


# -- Wyoming framing ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WyomingEvent:
    type: str
    data: dict[str, Any] = field(default_factory=dict)
    payload: bytes = b""


class WyomingError(Exception):
    """The peer sent something that is not a Wyoming event."""


def encode_event(
    event_type: str, data: dict[str, Any] | None = None, payload: bytes = b""
) -> bytes:
    """One event as header line, data block and payload, as hassmic writes them."""

    body = json.dumps(data, separators=(",", ":")).encode() if data else b""
    header: dict[str, Any] = {"type": event_type, "version": "1.5.4"}
    if body:
        header["data_length"] = len(body)
    if payload:
        header["payload_length"] = len(payload)
    return json.dumps(header, separators=(",", ":")).encode() + b"\n" + body + payload


def _length(header: dict[str, Any], key: str, limit: int) -> int:
    value = header.get(key) or 0
    if not isinstance(value, int) or not 0 <= value <= limit:
        raise WyomingError(f"bad {key}")
    return value


async def read_event(reader: asyncio.StreamReader) -> WyomingEvent | None:
    """The next event, or ``None`` when the peer closed the connection."""

    try:
        line = await reader.readuntil(b"\n")
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise WyomingError("connection closed inside a header") from exc
        return None
    except asyncio.LimitOverrunError as exc:
        raise WyomingError("header too long") from exc
    try:
        header = json.loads(line)
    except ValueError as exc:
        raise WyomingError("header is not JSON") from exc
    if not isinstance(header, dict) or not isinstance(header.get("type"), str):
        raise WyomingError("header has no type")
    data = header.get("data") if isinstance(header.get("data"), dict) else {}
    data_length = _length(header, "data_length", MAX_HEADER_BYTES)
    payload_length = _length(header, "payload_length", MAX_PAYLOAD_BYTES)
    try:
        if data_length:
            extra = json.loads(await reader.readexactly(data_length))
            if isinstance(extra, dict):
                data = {**data, **extra}
        payload = await reader.readexactly(payload_length) if payload_length else b""
    except asyncio.IncompleteReadError as exc:
        raise WyomingError("connection closed inside an event") from exc
    except ValueError as exc:
        raise WyomingError("data block is not JSON") from exc
    return WyomingEvent(header["type"], data, payload)


# -- 16 kHz to 24 kHz -------------------------------------------------------------


class Upsampler:
    """16 kHz to 24 kHz mono PCM16, linearly interpolated, continuous across chunks.

    Three output samples per two input samples; the third needs the next
    group's first sample, so up to two samples (and an odd byte) wait for the
    next chunk instead of restarting the interpolation at every boundary.
    """

    def __init__(self) -> None:
        self._carry = array.array("h")
        self._odd = b""

    def feed(self, pcm: bytes) -> bytes:
        data = self._odd + pcm
        usable = len(data) - len(data) % 2
        self._odd = data[usable:]
        samples = self._carry
        samples.frombytes(data[:usable])
        out = array.array("h")
        groups = (len(samples) - 1) // 2
        for g in range(groups):
            x0, x1, x2 = samples[2 * g], samples[2 * g + 1], samples[2 * g + 2]
            out.append(x0)
            out.append((x0 + 2 * x1) // 3)
            out.append((2 * x1 + x2) // 3)
        self._carry = samples[2 * groups :]
        return out.tobytes()


# -- the audio device -------------------------------------------------------------


def _bye_tone() -> bytes:
    """A 120 ms falling two-tone: hassmic's rising wake blip, mirrored, for the hang-up."""

    n = RATE * 120 // 1000
    fade = RATE * 0.01
    return array.array("h", (
        round(6000 * min(1.0, i / fade, (n - i) / fade)
              * math.sin(2 * math.pi * (1320.0 if i < n // 2 else 880.0) * i / RATE))
        for i in range(n)
    )).tobytes()


BYE_TONE = _bye_tone()


def _lifted(frame: bytes) -> bytes:
    samples = array.array("h", frame)
    return array.array("h", (max(-32768, min(32767, x * TALKER_LIFT)) for x in samples)).tobytes()


class TalkerGate:
    """Passes the talker and turns the room behind them (music, a TV) into silence.

    Left in, the room keeps the provider's turn from ending or is answered as a
    request. The stream runs GATE_ONSET_FRAMES late so a word keeps its onset.
    The level that opens it follows the room: low in a quiet one, so a command
    said more softly than the wake word is still heard, and lifted: the Echo sets
    its gain by the wake word. A call's audio opens with the end of the wake word,
    which skip_wake_word keeps from being answered. A single loud frame is a
    knock, not a word.
    """

    def __init__(self) -> None:
        self._partial = bytearray()
        self._held: deque[bytes] = deque()
        self._open_frames = 0
        self._was_loud = False
        self._passed: list[float] = []
        self._wake_word_frames = 0
        self._recent: deque[float] = deque(maxlen=GATE_ROOM_FRAMES)

    def skip_wake_word(self) -> None:
        self._wake_word_frames = GATE_WAKE_WORD_FRAMES

    def _without_wake_word(self, frame: bytes) -> bytes:
        if not self._wake_word_frames or not any(frame):
            return frame
        self._wake_word_frames -= 1
        loud = talk_audio._pcm16_rms(frame) >= GATE_LEVEL_MAX
        begun = self._wake_word_frames < GATE_WAKE_WORD_FRAMES - 1
        if not loud and begun:
            self._wake_word_frames = 0
        return bytes(len(frame))

    def _opening_level(self) -> float:
        room = statistics.median(self._recent) if self._recent else 0.0
        return min(GATE_LEVEL_MAX, max(GATE_LEVEL_MIN, GATE_OVER_ROOM * room))

    def feed(self, pcm: bytes) -> bytes:
        """Take microphone audio in pieces of any size and return what the session hears now."""

        self._partial += pcm
        heard = b""
        while len(self._partial) >= GATE_FRAME_BYTES:
            frame = bytes(self._partial[:GATE_FRAME_BYTES])
            del self._partial[:GATE_FRAME_BYTES]
            heard += self._frame(self._without_wake_word(frame))
        return heard

    def _frame(self, frame: bytes) -> bytes:
        level = talk_audio._pcm16_rms(frame)
        self._held.append(frame)
        loud = level >= self._opening_level()
        if loud and (self._was_loud or self._open_frames):
            self._open_frames = GATE_ONSET_FRAMES + GATE_HANG_FRAMES + 1
            self._passed.append(level)
        self._was_loud = loud
        self._recent.append(level)
        due = bytes(GATE_FRAME_BYTES)
        if len(self._held) > GATE_ONSET_FRAMES:
            held = self._held.popleft()
            if self._open_frames:
                due = _lifted(held)
        self._open_frames = max(0, self._open_frames - 1)
        if self._passed and not self._open_frames:
            _log.info("echo: heard a sound, %.1f s of it loud, peak %.3f over a room of %.3f",
                      len(self._passed) * GATE_FRAME_S, max(self._passed),
                      statistics.median(self._recent))
            self._passed.clear()
        return due


class EchoAudio:
    """DuplexAudio's session interface, over one Echo's Wyoming connection.

    Playback is paced here, not on the Echo: audio goes out at the rate it
    plays, :data:`PLAYBACK_LEAD_S` ahead, so a barge-in can still discard what
    has not been heard. ``write`` sends bytes to the Echo; it is called on the
    event loop that runs the pacing task.
    """

    def __init__(
        self,
        write: Callable[[bytes], None],
        *,
        clock: Callable[[], float] = time.monotonic,
        barge_in_level: float = DEFAULT_BARGE_IN_LEVEL,
        on_speech: Callable[[], None] | None = None,
    ) -> None:
        self._write = write
        self._clock = clock
        self._on_speech = on_speech
        self._hang_up_at: float | None = None
        self._barge_in_level = barge_in_level
        self._gate = TalkerGate()
        self._levels: dict[bool, list[float]] = {True: [], False: []}
        self._gated = 0
        self._echo: deque[tuple[float, float]] = deque()  # (sent until, level) per playback chunk
        self._cut_item: str | None = None
        self._open_until = 0.0
        self._lock = threading.Lock()
        self._upsampler = Upsampler()
        self._input: deque[bytes] = deque()
        self._input_bytes = 0
        self._input_paused = False
        self._dropped_input_blocks = 0
        self._playback: deque[tuple[str | None, bytes]] = deque()
        self._queued_playback_bytes = 0
        self._dropped_playback_bytes = 0
        self._consumed_playback_bytes = 0
        self._discarded_playback_bytes = 0
        self._played_item_id: str | None = None
        self._played_frames = 0
        self._sent_until = 0.0
        self._started_reply = False
        self._last_activity = clock()
        self._last_spoke = clock()
        self._pacer: asyncio.Task | None = None

    # -- lifecycle ------------------------------------------------------------

    def start(self) -> None:
        if self._pacer is None or self._pacer.done():
            self._pacer = asyncio.get_running_loop().create_task(self._pace())

    def stop(self) -> None:
        if self._pacer is not None:
            self._pacer.cancel()
            self._pacer = None

    def end_event(self, *, failed: bool = False) -> bytes:
        """What hands the Echo back to its wake word when the session is over."""

        if failed:
            return encode_event(
                "error", {"text": "Talk session failed", "code": "talk-session-failed"}
            )
        if self._started_reply:
            return encode_event("audio-stop")
        return encode_event("error", {"text": "Talk session ended", "code": QUIET_END_CODE})

    # -- from the Echo ------------------------------------------------------------

    def feed_microphone(self, pcm16k: bytes) -> None:
        pcm = self._upsampler.feed(pcm16k)
        if not pcm:
            return
        level = talk_audio._pcm16_rms(pcm)
        with self._lock:
            now = self._clock()
            echo_playing = self._sent_until + ECHO_TAIL_S > now and now >= self._open_until
            levels = self._levels[echo_playing]
            if len(levels) < 20_000:
                levels.append(level)
            if echo_playing and level < self._echo_floor(now):
                pcm = bytes(len(pcm))
                self._gated += 1
            elif level >= SPEECH_LEVEL:
                self._last_activity = self._last_spoke = self._clock()
            if self._input_paused:
                return
            pcm = self._gate.feed(pcm)
            if not pcm:
                return
            self._input.append(pcm)
            self._input_bytes += len(pcm)
            # Newest audio wins, as on the terminal: stale speech corrupts the turn.
            while self._input_bytes > MAX_INPUT_BYTES:
                self._input_bytes -= len(self._input.popleft())
                self._dropped_input_blocks += 1

    def _echo_floor(self, now: float) -> float:
        while self._echo and self._echo[0][0] < now - ECHO_SPAN_S:
            self._echo.popleft()
        reply = max((level for _, level in self._echo), default=0.0)
        return max(self._barge_in_level, ECHO_RATIO * reply)

    def cut_reply(self) -> None:
        """The wake word was said over the reply: stop it and hear what follows.

        The rest of the reply still streaming in is dropped, and the microphone
        is open even over what the Echo has already been sent. What was heard is
        kept for the session's own barge-in, which follows the operator's words.
        """

        with self._lock:
            self._cut_item = self._played_item_id
            self._echo.clear()
            self._open_until = self._clock() + ECHO_SPAN_S
            self._playback.clear()
            self._discarded_playback_bytes += self._queued_playback_bytes
            self._queued_playback_bytes = 0

    def level_report(self) -> str:
        """Microphone levels with and without the Echo speaking, to tune the barge-in level."""

        def summary(values: list[float]) -> str:
            if not values:
                return "none"
            ordered = sorted(values)
            median, p95, top = (ordered[int(q * (len(ordered) - 1))] for q in (0.5, 0.95, 1.0))
            return f"median {median:.3f}, p95 {p95:.3f}, max {top:.3f} ({len(ordered)} chunks)"

        with self._lock:
            return (
                f"microphone while the Echo spoke: {summary(self._levels[True])}, "
                f"{self._gated} gated under {self._barge_in_level} or {ECHO_RATIO}x the reply; "
                f"otherwise: {summary(self._levels[False])}"
            )

    def since_spoke(self) -> float:
        """Seconds since the operator's voice was last heard: the latency measure in the log."""

        return self._clock() - self._last_spoke

    def mark_activity(self) -> None:
        self._last_activity = self._clock()

    def call_started(self) -> None:
        with self._lock:
            self._gate.skip_wake_word()
        self.mark_activity()

    def request_hang_up(self) -> None:
        if self._hang_up_at is None:
            self._hang_up_at = self._clock()

    def hung_up(self) -> bool:
        """end_call was asked for, and the goodbye has been said (or never came)."""

        if self._hang_up_at is None:
            return False
        waited = self._clock() - self._hang_up_at
        with self._lock:
            silent = (
                self._queued_playback_bytes == 0
                and self._sent_until + ECHO_TAIL_S <= self._clock()
            )
        return waited >= HANG_UP_MAX_S or (waited >= HANG_UP_MIN_S and silent)

    def quiet_s(self) -> float:
        """Seconds since anyone spoke, the Echo included."""

        with self._lock:
            speaking = self._queued_playback_bytes > 0 or self._sent_until > self._clock()
        if speaking:
            self._last_activity = self._clock()
        return self._clock() - self._last_activity

    # -- pacing ----------------------------------------------------------------

    async def _pace(self) -> None:
        while True:
            self.pace_once()
            await asyncio.sleep(PACE_TICK_S)

    def pace_once(self) -> None:
        """Send whatever is due at this moment. One tick of the pacing task."""

        now = self._clock()
        with self._lock:
            if not self._playback:
                return
            fresh = self._sent_until + ECHO_TAIL_S < now
            self._sent_until = max(self._sent_until, now)
            due = round((now + PLAYBACK_LEAD_S - self._sent_until) * RATE) * FRAME_BYTES
            if due <= 0:
                return
            chunk = self._take_playback(due)
            self._sent_until += len(chunk) / (RATE * FRAME_BYTES)
            self._echo.append((self._sent_until, talk_audio._pcm16_rms(chunk)))
            begin = not self._started_reply
            self._started_reply = True
        if fresh:
            _log.info("echo: reply audio, %.2f s after the operator last spoke", self.since_spoke())
        if begin:
            if self._on_speech is not None:
                self._on_speech()
            self._write(encode_event("audio-start", {"rate": RATE, "width": 2, "channels": 1}))
        self._write(encode_event("audio-chunk", {"rate": RATE, "width": 2, "channels": 1}, chunk))

    def _take_playback(self, wanted: int) -> bytes:
        parts: list[bytes] = []
        while wanted > 0 and self._playback:
            item_id, data = self._playback.popleft()
            taken, rest = data[:wanted], data[wanted:]
            if rest:
                self._playback.appendleft((item_id, rest))
            parts.append(taken)
            wanted -= len(taken)
            self._queued_playback_bytes -= len(taken)
            self._consumed_playback_bytes += len(taken)
            if item_id != self._played_item_id:
                self._played_item_id = item_id
                self._played_frames = 0
            self._played_frames += len(taken) // FRAME_BYTES
        return b"".join(parts)

    # -- session interface (talk_audio.DuplexAudio) --------------------------------

    def read_input_chunk(self) -> bytes | None:
        with self._lock:
            if self._input_paused or not self._input:
                return None
            chunk = self._input.popleft()
            self._input_bytes -= len(chunk)
            return chunk

    def pause_input(self) -> None:
        with self._lock:
            self._input_paused = True
            self._gate = TalkerGate()
            self._input.clear()
            self._input_bytes = 0

    def resume_input(self) -> None:
        with self._lock:
            self._input_paused = False

    @property
    def input_paused(self) -> bool:
        with self._lock:
            return self._input_paused

    def queue_playback(self, pcm: bytes, item_id: str | None = None) -> None:
        if not pcm:
            return
        with self._lock:
            if item_id is not None and item_id == self._cut_item:
                return
            if self._queued_playback_bytes + len(pcm) > MAX_PLAYBACK_BYTES:
                self._dropped_playback_bytes += len(pcm)
                return
            self._playback.append((item_id, pcm))
            self._queued_playback_bytes += len(pcm)

    async def play_out(self, pcm: bytes) -> None:
        """Queue ``pcm`` and return once the Echo has played it."""

        self.queue_playback(pcm, "bye-tone")
        deadline = self._clock() + HANG_UP_MAX_S
        while self._clock() < deadline:
            with self._lock:
                if self._queued_playback_bytes == 0 and self._sent_until <= self._clock():
                    return
            await asyncio.sleep(PACE_TICK_S)

    def drain_playback(self) -> tuple[str | None, int]:
        """Discard unsent audio and return the heard boundary.

        Up to :data:`PLAYBACK_LEAD_S` already on the Echo still plays; it is
        counted as heard, which it will be.
        """

        with self._lock:
            self._playback.clear()
            self._discarded_playback_bytes += self._queued_playback_bytes
            self._queued_playback_bytes = 0
            boundary = (self._played_item_id, int(self._played_frames * 1000 / RATE))
            self._played_item_id = None
            self._played_frames = 0
            return boundary

    @property
    def playback_pending(self) -> bool:
        with self._lock:
            return self._queued_playback_bytes > 0

    @property
    def playback_progress(self) -> talk_audio.PlaybackProgress:
        with self._lock:
            return talk_audio.PlaybackProgress(
                self._consumed_playback_bytes,
                self._discarded_playback_bytes + self._dropped_playback_bytes,
                self._queued_playback_bytes > 0,
            )

    @property
    def played_ms(self) -> int:
        with self._lock:
            return int(self._played_frames * 1000 / RATE)

    def reset_played_ms(self) -> None:
        with self._lock:
            self._played_item_id = None
            self._played_frames = 0

    @property
    def dropped_input_blocks(self) -> int:
        with self._lock:
            return self._dropped_input_blocks

    @property
    def dropped_playback_bytes(self) -> int:
        with self._lock:
            return self._dropped_playback_bytes


# -- the surface --------------------------------------------------------------------

SessionRunner = Callable[..., Awaitable[int]]


class _Conversation:
    """One Talk session on the Echo: connected on standby, then the call.

    A standby session hears nothing (hassmic streams the microphone only
    during a run) and says nothing unless it has something to tell. It becomes
    the call at the wake word (:meth:`activate`) or when it starts speaking on
    its own, and then ends after a quiet spell or ``end_call``. A standby that
    is never woken is replaced every :data:`STANDBY_REFRESH_S`.
    """

    def __init__(
        self,
        write: Callable[[bytes], None],
        runner: SessionRunner,
        idle_s: float,
        on_end: Callable[[_Conversation, bool], None],
        barge_in_level: float = DEFAULT_BARGE_IN_LEVEL,
        *,
        active: bool = True,
    ) -> None:
        self.audio = EchoAudio(write, barge_in_level=barge_in_level, on_speech=self.activate)
        self.announcements: asyncio.Queue = asyncio.Queue()
        self.active = False
        self._write = write
        self._idle_s = idle_s
        self._on_end = on_end
        self._closed = False
        loop = asyncio.get_running_loop()
        self._session = loop.create_task(
            runner(audio=self.audio, lane=LANE, announce_queue=self.announcements)
        )
        self._watch = loop.create_task(self._watch_session())
        if active:
            self.activate()

    def activate(self) -> None:
        global _calling
        if self.active:
            return
        self.active = True
        self.audio.call_started()
        _calling = self.audio
        # A standby's prompt clock is from when it connected, up to
        # STANDBY_REFRESH_S ago: the call brings it up to date.
        self.announcements.put_nowait(clock_commands())
        _log.info("echo: call started")

    async def _watch_session(self) -> None:
        loop = asyncio.get_running_loop()
        born, born_wall = loop.time(), time.time()
        while not self._session.done():
            if self.active:
                if self.audio.hung_up():
                    _log.info("echo: end_call, ending the session")
                    await self.audio.play_out(BYE_TONE)
                    break
                if self.audio.quiet_s() >= self._idle_s:
                    _log.info("echo: quiet for %.0f s, ending the session", self._idle_s)
                    break
            elif loop.time() - born >= STANDBY_REFRESH_S:
                break
            elif talk_runs.owed_since(RETURN_SESSION_ID, born_wall):
                _log.info("echo: a background result landed, a new session picks it up")
                break
            await asyncio.sleep(0.25)
        failed = False
        if self._session.done():
            failed = (
                self._session.cancelled()
                or self._session.exception() is not None
                or bool(self._session.result())
            )
            if failed:
                _log.warning("echo: the Talk session ended unsuccessfully")
        await self.close(failed=failed)

    async def close(self, *, failed: bool = False, notify_echo: bool = True) -> None:
        global _calling
        if self._closed:
            return
        self._closed = True
        if _calling is self.audio:
            _calling = None
        self._on_end(self, failed)
        if not self._session.done():
            self._session.cancel()
        await asyncio.gather(self._session, return_exceptions=True)
        self.audio.stop()
        if self.active:
            _log.info("echo: %s", self.audio.level_report())
            if notify_echo:
                self._write(self.audio.end_event(failed=failed))
        if asyncio.current_task() is not self._watch:
            self._watch.cancel()


#: The call's audio while one is live: where end_call lands.
_calling: EchoAudio | None = None

END_CALL_TOOL: dict = {
    "type": "function",
    "name": "end_call",
    "description": (
        "Hang up this call on the Echo. Use it when the operator says goodbye, "
        "says that is all, or asks you to stop listening. Say only 'Bye.' first, in "
        "the same turn; nothing is said after it, and the call closes once it is heard."
    ),
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}


def note_action(what: str) -> None:
    """Log a tool call or delegation with its latency, for comparing voice models."""

    if _calling is not None:
        _log.info("echo: action %s, %.2f s after the operator last spoke",
                  what[:120], _calling.since_spoke())


HANGING_UP = "Hanging up after your goodbye."


def run_end_call(arguments: dict) -> str:
    audio = _calling
    if audio is None:
        return "There is no call on the Echo to end."
    audio.request_hang_up()
    return HANGING_UP


def clock_commands() -> list:
    try:
        from . import talk_identity, talk_realtime
    except ImportError:  # pragma: no cover - flat-module fallback
        import talk_identity
        import talk_realtime
    return [talk_realtime.AddContext(
        item_id=f"talkclock{uuid.uuid4().hex[:20]}",
        text=f"The call starts now. {talk_identity.current_moment()} "
        "This is only for when the operator asks the time or date.",
    )]


def say_commands(text: str) -> list:
    """What makes the session speak a message Hermes asked it to deliver.

    Unlike a background result this is no untrusted data: only the Hermes
    user can reach the socket. So it is an instruction to say the words as
    they are, not a report to sum up, which the model answered with a
    generic "what would you like help with?" instead of the message. It is
    not deleted after the response either: the delete raced the response and
    the model spoke without having read it.
    """

    try:
        from . import talk_realtime
    except ImportError:  # pragma: no cover - flat-module fallback
        import talk_realtime
    item_id = f"talksay{uuid.uuid4().hex[:20]}"
    return [
        talk_realtime.AddContext(
            item_id=item_id,
            text=f"Hermes has a message for the operator. Say exactly this, word for word, "
            f"and nothing else: {text}",
        ),
        talk_realtime.StartResponse(allow_tools=False),
    ]


def time_event() -> bytes:
    """This host's clock and zone for the Echo, which has no other source in Wyoming mode.

    After a power loss its clock restarts in 2010 and every TLS call fails, so it
    is sent on each link. The zone is the POSIX rule that ends a TZif file.
    """

    try:
        tz = Path("/etc/localtime").read_bytes().rstrip(b"\n").rsplit(b"\n", 1)[-1].decode("ascii")
    except (OSError, UnicodeDecodeError):
        tz = ""
    return encode_event("time", {"epoch": int(time.time()), "tz": tz})


def say_socket_path() -> Path:
    try:
        from . import talk_config
    except ImportError:  # pragma: no cover - flat-module fallback
        import talk_config
    return Path(talk_config.get_hermes_home()) / SAY_SOCKET


async def _linked(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    runner: SessionRunner,
    idle_s: float,
    barge_in_level: float,
    say_queue: asyncio.Queue | None = None,
) -> None:
    """Drive one connection to the Echo until it closes.

    One session at a time: the standby, or the call it became.
    """

    loop = asyncio.get_running_loop()
    current: _Conversation | None = None
    retry_s = STANDBY_RETRY_MIN_S
    restart: asyncio.TimerHandle | None = None

    def standby() -> None:
        nonlocal current
        if current is None:
            current = _Conversation(
                writer.write, runner, idle_s, ended, barge_in_level, active=False
            )

    def ended(c: _Conversation, failed: bool) -> None:
        nonlocal current, retry_s, restart
        if current is not c:
            return
        current = None
        if failed and not c.active:
            delay, retry_s = retry_s, min(retry_s * 2, STANDBY_RETRY_MAX_S)
        else:
            delay, retry_s = 0.5, STANDBY_RETRY_MIN_S
        restart = loop.call_later(delay, standby)

    async def forward_says() -> None:
        while True:
            text = await say_queue.get()
            if current is None:
                standby()
            current.activate()
            current.announcements.put_nowait(say_commands(text))

    writer.write(encode_event("describe"))
    writer.write(encode_event("run-satellite"))
    writer.write(time_event())
    await writer.drain()
    standby()
    says = loop.create_task(forward_says()) if say_queue is not None else None
    try:
        while (event := await read_event(reader)) is not None:
            if event.type == "audio-chunk":
                if current is not None and current.active:
                    current.audio.feed_microphone(event.payload)
            elif event.type == "run-pipeline":
                if current is None:
                    _log.info("echo: wake word, starting a Talk session")
                    current = _Conversation(writer.write, runner, idle_s, ended, barge_in_level)
                elif not current.active:
                    _log.info("echo: wake word, the standby session takes the call")
                    current.activate()
            elif event.type == "detection":
                if current is not None and current.active:
                    _log.info("echo: wake word during the call, the reply stops")
                    current.audio.cut_reply()
            elif event.type == "ping":
                writer.write(encode_event("pong"))
            elif event.type == "info":
                name = (event.data.get("satellite") or {}).get("name")
                _log.info("echo: linked to %s", name or "an unnamed satellite")
    finally:
        if says is not None:
            says.cancel()
        if restart is not None:
            restart.cancel()
        if current is not None:
            await current.close(notify_echo=False)


async def _serve_says(queue: asyncio.Queue) -> asyncio.AbstractServer | None:
    """Accept ``hermes talk echo-say`` messages: one UTF-8 line per connection."""

    path = say_socket_path()

    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), 5.0)
            text = line.decode("utf-8", "replace").strip()[:MAX_SAY_CHARS]
            if text:
                queue.put_nowait(text)
                writer.write(b"ok\n")
            else:
                writer.write(b"empty\n")
            await writer.drain()
        except (OSError, TimeoutError):
            pass
        finally:
            writer.close()

    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    try:
        server = await asyncio.start_unix_server(accept, path=str(path))
    except OSError as exc:
        _log.warning("echo: no echo-say socket at %s (%s)", path, exc)
        return None
    os.chmod(path, 0o600)
    return server


def send_say(text: str) -> str:
    """``hermes talk echo-say TEXT``: hand a message to the running Echo surface."""

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5.0)
        sock.connect(str(say_socket_path()))
        sock.sendall(" ".join(text.split()).encode() + b"\n")
        return sock.recv(64).decode().strip()


async def serve(
    host: str,
    port: int = DEFAULT_PORT,
    *,
    idle_s: float = DEFAULT_IDLE_S,
    barge_in_level: float = DEFAULT_BARGE_IN_LEVEL,
    runner: SessionRunner | None = None,
) -> None:
    """Stay linked to the Echo at ``host``, reconnecting with backoff. Runs until cancelled."""

    if runner is None:
        try:
            from . import talk_cli, talk_config, talk_echo_live
        except ImportError:  # pragma: no cover - flat-module fallback
            import talk_cli
            import talk_config
            import talk_echo_live
        live = talk_config.voice_mode() == "live"
        runner = (
            talk_echo_live.run_live_session
            if live
            else functools.partial(talk_cli.run_talk_session, return_session_id=RETURN_SESSION_ID)
        )
        _log.info("echo: voice %s", "GPT-Live" if live else "Realtime")
    say_queue: asyncio.Queue = asyncio.Queue()
    says = await _serve_says(say_queue)
    try:
        await _stay_linked(host, port, runner, idle_s, barge_in_level, say_queue)
    finally:
        if says is not None:
            says.close()


def _detect_silent_power_loss(sock: socket.socket) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, LINK_PROBE_IDLE_S)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, LINK_PROBE_EVERY_S)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, LINK_DEAD_AFTER_S * 1000)


async def _stay_linked(host, port, runner, idle_s, barge_in_level, say_queue) -> None:
    delay = RECONNECT_MIN_S
    while True:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, limit=MAX_HEADER_BYTES), CONNECT_TIMEOUT_S
            )
        except (OSError, TimeoutError) as exc:
            _log.warning(
                "echo: cannot reach %s:%d (%s), retrying in %.0f s", host, port, exc, delay
            )
        else:
            delay = RECONNECT_MIN_S
            try:
                _detect_silent_power_loss(writer.get_extra_info("socket"))
                await _linked(reader, writer, runner, idle_s, barge_in_level, say_queue)
                _log.warning("echo: %s:%d closed the connection", host, port)
            except (OSError, WyomingError) as exc:
                _log.warning("echo: link to %s:%d lost (%s)", host, port, exc)
            finally:
                writer.close()
        await asyncio.sleep(delay)
        delay = min(delay * 2, RECONNECT_MAX_S)


def cli_entry(
    host: str,
    port: int = DEFAULT_PORT,
    idle_s: float = DEFAULT_IDLE_S,
    barge_in_level: float = DEFAULT_BARGE_IN_LEVEL,
) -> int:
    """``hermes talk echo HOST``: serve one Echo until Ctrl+C."""

    # Our own handler: the Hermes host configures the root logger first, which
    # makes logging.basicConfig a no-op and these lines vanish.
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d talk: %(message)s", "%H:%M:%S"))
    _log.addHandler(handler)
    _log.setLevel(logging.INFO)
    _log.propagate = False
    print(f"talk: serving the Echo at {host}:{port}; say its wake word. Ctrl+C to stop.")
    try:
        asyncio.run(serve(host, port, idle_s=idle_s, barge_in_level=barge_in_level))
    except KeyboardInterrupt:
        print("\ntalk: stopped.")
    return 0


__all__ = [
    "DEFAULT_IDLE_S",
    "DEFAULT_PORT",
    "END_CALL_TOOL",
    "LANE",
    "EchoAudio",
    "Upsampler",
    "WyomingError",
    "WyomingEvent",
    "cli_entry",
    "encode_event",
    "read_event",
    "run_end_call",
    "send_say",
    "serve",
]
