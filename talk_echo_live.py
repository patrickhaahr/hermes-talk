"""GPT-Live on the Echo: the runner ``hermes talk echo`` uses under ``TALK_VOICE_MODE=live``.

GPT-Live has no function tools. It hands work to the client as a delegation
with a plain-text prompt, so the tools the Realtime session calls directly
become delegation prompts here: ``home_control DEVICE COMMAND [TIMES]`` runs
:mod:`talk_home` at once, ``end_call`` hangs up, and anything else goes to a
Hermes agent whose answer is handed back to the delegation.

The subscription transport is WebRTC, which keeps sending frames while the
model is silent. Only audible audio, plus a short tail that keeps the pauses
between words, reaches the Echo: silence queued as playback would hold every
call open and wake a standby session that has nothing to say.
"""

from __future__ import annotations

import array
import asyncio
import re
import time
import uuid

try:
    from . import talk_config, talk_echo, talk_home, talk_host, talk_identity, talk_runs
    from . import talk_realtime as rt
    from .talk_cli import WORK_STARTED_RE, _active_parent_session_id
    from .talk_live_config import resolve_live_auth, resolve_live_config
    from .talk_live_realtime import LiveRealtimeSession
except ImportError:  # pragma: no cover - flat-module fallback
    import talk_config
    import talk_echo
    import talk_home
    import talk_host
    import talk_identity
    import talk_realtime as rt
    import talk_runs
    from talk_cli import WORK_STARTED_RE, _active_parent_session_id
    from talk_live_config import resolve_live_auth, resolve_live_config
    from talk_live_realtime import LiveRealtimeSession

_log = talk_echo._log

AUDIBLE_LEVEL = 500
#: Silence kept after audible audio: the pauses between words and sentences.
SPEECH_TAIL_BYTES = int(24000 * 2 * 0.6)
PREBUFFER_BYTES = int(24000 * 2 * 0.25)
OUTPUT_GAIN = 1.25
INPUT_POLL_S = 0.01
RUN_POLL_S = 0.5
MAX_RESULT_CHARS = 4000

#: Live ignores any delegation format it is asked for and delegates in its own words
#: ("Turn the TV volume up three steps"), so home requests are read from plain English.
#: Anything this cannot pin to one device and one command goes to Hermes instead.
NUMBERS = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5", "six": "6",
           "seven": "7", "eight": "8", "nine": "9", "ten": "10", "once": "1 times",
           "twice": "2 times", "a couple": "2", "a few": "3"}
DEVICE_WORDS = {
    "lights": r"\blights?\b|\blamps?\b",
    "tv": r"\btv\b|\btelevision\b|\bt\.v\.",
    "speaker": r"\bspeakers?\b|\bsound ?bar\b|\bedifier\b",
}
REMOTE_RULES = (
    (r"\bhdmi\W*([123])\b", "hdmi-{}"),
    (r"\b(?:un)?mute", "mute"),
    (r"\bchannel\W+up\b|\bnext channel\b", "channel-up"),
    (r"\bchannel\W+down\b|\bprevious channel\b", "channel-down"),
    (r"louder|\bvolume\W+up\b|\b(?:turn|crank)\b.*\bup\b|\braise\b|\bincrease\b", "volume-up"),
    (r"quieter|softer|\bvolume\W+down\b|\bturn\b.*\bdown\b|\blower\b|\bdecrease\b",
     "volume-down"),
    (r"\bbluetooth\b", "bluetooth"),
    (r"\boptical\b", "optical"),
    (r"\binput\b|\bsource\b", "input"),
    (r"\bon\b|\boff\b|\bpower\b", "power"),
)
TIMES = re.compile(r"\b(\d+)\s*(?:steps?|times|notch(?:es)?|clicks?|press(?:es)?|levels?|ticks?)\b"
                   r"|\bby\s+(\d+)\b")
#: Live has no clock, so it delegates the time; answering here saves a whole agent run.
CLOCK_PROMPT = re.compile(
    r"\bwhat(?:'s| is)? (?:the )?(?:time|date|day)\b|\bwhat time\b|\bwhich day\b"
    r"|\bhow much time is it\b",
    re.IGNORECASE,
)
GOODBYE_MAX_WORDS = 8
END_CALL_PROMPT = re.compile(
    r"\bend_call\b|\bhang\W*up\b|\b(?:good)?bye\b|\bstop listening\b"
    r"|\b(?:end|turn off|stop|close|finish) (?:the|this|our) (?:call|conversation)\b",
    re.IGNORECASE,
)

GUIDE = """
# How you act on this call
You cannot run tools yourself; you delegate. Never claim something is done before the delegation's
answer arrives, and never make up a result.
- The lights, the TV and the speaker: the operator has pre-approved these. Delegate at once with no
  confirmation, in one short sentence naming the device, the action and how many steps, like
  "Turn off the lights" or "Turn the TV volume up 3 steps". Say nothing before the answer; then
  at most a word or two, like "Done."
- Hanging up: when the operator says goodbye or that is all, say a short goodbye and then delegate
  "Hang up".
- Chat, general knowledge and opinions: answer yourself, without delegating.
- Anything else that needs Hermes (its files, servers, memory, the web, longer work): delegate it in
  plain words, say something short like "One moment", and speak the answer when it arrives.
"""


def home_arguments(prompt: str | None) -> dict | None:
    """The home_control call a delegation prompt asks for, or None when it is not exactly one."""

    text = (prompt or "").lower()
    for word, value in NUMBERS.items():
        text = re.sub(rf"\b{word}\b", value, text)
    devices = [device for device, words in DEVICE_WORDS.items() if re.search(words, text)]
    if len(devices) != 1:
        return None
    device = devices[0]
    if device == "lights":
        on, off = re.search(r"\bon\b", text), re.search(r"\boff\b", text)
        command = "on" if on and not off else "off" if off and not on else None
    else:
        command = next((template.format(*match.groups()) for pattern, template in REMOTE_RULES
                        if (match := re.search(pattern, text))), None)
    if command is None or talk_home.argv(device, command) is None:
        return None
    times = TIMES.search(text)
    repeat = int(next(group for group in times.groups() if group)) if times else 1
    return {"device": device, "command": command,
            "repeat": max(1, min(repeat, talk_home.MAX_REPEAT))}


def audible(pcm: bytes) -> bool:
    samples = array.array("h")
    samples.frombytes(pcm[: len(pcm) - len(pcm) % 2])
    return bool(samples) and max(map(abs, samples)) > AUDIBLE_LEVEL


class OutputShaper:
    """Turns Live's WebRTC output into what the Echo should play.

    - Silence between replies is dropped; audible audio and a short tail are kept.
    - Each reply starts with :data:`PREBUFFER_BYTES` held back. WebRTC delivers in real
      time, 20 ms at a time, so without a cushion every bit of network jitter left the
      Echo with nothing to play, which was heard as crackle.
    - :data:`OUTPUT_GAIN` brings Live up to Realtime's level (speech RMS about 1900
      against 2300), clamped so it never clips.
    """

    def __init__(self, tail_bytes: int = SPEECH_TAIL_BYTES,
                 prebuffer_bytes: int = PREBUFFER_BYTES, gain: float = OUTPUT_GAIN) -> None:
        self._tail_bytes = tail_bytes
        self._prebuffer_bytes = prebuffer_bytes
        self._gain = gain
        self._left = 0
        self._held: list[bytes] = []
        self._playing = False

    def feed(self, pcm: bytes) -> bytes:
        """The audio to queue for playback now, often empty."""

        if audible(pcm):
            self._left = self._tail_bytes
        elif self._left > 0:
            self._left -= len(pcm)
        else:
            self._playing = False
            return self._release()
        if self._playing:
            return self._amplify(pcm)
        self._held.append(pcm)
        if sum(map(len, self._held)) < self._prebuffer_bytes:
            return b""
        self._playing = True
        return self._release()

    def _release(self) -> bytes:
        held, self._held = b"".join(self._held), []
        return self._amplify(held)

    def _amplify(self, pcm: bytes) -> bytes:
        samples = array.array("h")
        samples.frombytes(pcm[: len(pcm) - len(pcm) % 2])
        gain = self._gain
        return array.array(
            "h", (max(-32768, min(32767, round(x * gain))) for x in samples)
        ).tobytes()


def announcement_texts(item) -> list[str]:
    """The text of each context message in an announcement batch (echo-say)."""

    commands = item if isinstance(item, list) else getattr(item, "commands", [])
    return [command.text for command in commands if isinstance(command, rt.AddContext)]


async def _agent_answer(prompt: str, owner: str) -> str:
    receipt = await asyncio.to_thread(talk_host.host().run_agent, prompt, True)
    match = WORK_STARTED_RE.search(receipt or "")
    if match is None:
        return receipt or "Hermes gave no answer."
    run_id = int(match.group(1))
    deadline = time.monotonic() + talk_config.agent_timeout_s()
    while time.monotonic() < deadline:
        await asyncio.sleep(RUN_POLL_S)
        run = talk_runs.get_run(run_id)
        if run is None:
            return "Hermes lost track of that task."
        if run["status"] in talk_runs.TERMINAL_STATUSES:
            # Delivered here, so no later session adopts and repeats it.
            if talk_runs.claim_delivery(run_id, claimant=owner):
                talk_runs.mark_delivered(run_id, claimant=owner)
            output = str(run.get("output") or "(no output)")
            if run["status"] == "failed":
                output = f"The Hermes task failed: {output}"
            return output
    return "Hermes is still working on it; nothing is confirmed yet."


async def _keep_call_open(audio) -> None:
    mark = getattr(audio, "mark_activity", None)
    while callable(mark):
        mark()
        await asyncio.sleep(1.0)


def is_goodbye(said: str) -> bool:
    """A short goodbye from the operator; longer sentences only mention one."""

    return len(said.split()) <= GOODBYE_MAX_WORDS and bool(END_CALL_PROMPT.search(said))


def _request_hang_up(audio) -> bool:
    request = getattr(audio, "request_hang_up", None)
    if callable(request):
        request()
    return callable(request)


async def _answer(audio, prompt: str | None, owner: str) -> str:
    arguments = home_arguments(prompt)
    done = await asyncio.to_thread(talk_home.run, arguments) if arguments else None
    if END_CALL_PROMPT.search(prompt or ""):
        _request_hang_up(audio)
        bye = "Hanging up after your goodbye. Say only a short goodbye."
        return f"{done}. {bye}" if done else bye
    if done is not None:
        return done
    if CLOCK_PROMPT.search(prompt or ""):
        return time.strftime("It is %H:%M on %A, %d %B %Y (%Z).")
    if not prompt:
        return "The delegation carried no request."
    return await _agent_answer(prompt, owner)


async def run_live_session(*, audio, lane: str, announce_queue: asyncio.Queue | None = None,
                           session_factory=None) -> int:
    """One GPT-Live session on ``audio``; returns 0 when it ended cleanly. Runs until cancelled."""

    config = resolve_live_config()
    instructions = talk_identity.build_instructions(
        talk_host.host().identity_sections(), tools=[], lane=lane
    ) + GUIDE
    if session_factory is not None:
        session = session_factory()
    else:
        session = LiveRealtimeSession(auth=resolve_live_auth(config=config), config=config)
    announce_queue = announce_queue or asyncio.Queue()
    pending: set[asyncio.Task] = set()

    async def send_microphone() -> None:
        while True:
            chunk = audio.read_input_chunk()
            if chunk is None:
                await asyncio.sleep(INPUT_POLL_S)
                continue
            await session.send([rt.AppendInputAudio(chunk)])

    async def delegate(event: rt.DelegationRequested) -> None:
        talk_echo.note_action(f"delegation {event.prompt or ''!r}")
        # The call stays open while Hermes works; a quiet spell must not drop the answer.
        keep_open = asyncio.create_task(_keep_call_open(audio))
        try:
            answer = (await _answer(audio, event.prompt, owner))[:MAX_RESULT_CHARS]
        finally:
            keep_open.cancel()
        answer = answer or "(no output)"
        _log.info("echo: live delegation answered: %s", answer[:200])
        try:
            await session.send([rt.SubmitDelegationResult(event.delegation_id, answer)])
        except rt.RealtimeSessionError:
            # The delegation was retired while Hermes worked; tell the operator anyway.
            await session.send([rt.AppendLiveContext(answer, kind="message")])

    async def announce() -> None:
        while True:
            item = await announce_queue.get()
            for text in announcement_texts(item):
                await session.send([rt.AppendLiveContext(text, kind="message")])

    async def receive() -> int:
        shaper = OutputShaper()
        async for event in session:
            if isinstance(event, rt.OutputAudio):
                if pcm := shaper.feed(event.data):
                    audio.queue_playback(pcm)
            elif isinstance(event, rt.DelegationRequested):
                task = asyncio.create_task(delegate(event))
                pending.add(task)
                task.add_done_callback(pending.discard)
            elif isinstance(event, rt.Transcript) and event.final and event.text:
                _log.info("echo: live %s: %s", event.role, event.text[:200])
                if event.role is rt.TranscriptRole.USER and is_goodbye(event.text):
                    # Live often answers "Bye." without delegating the hang-up.
                    _request_hang_up(audio)
            elif isinstance(event, rt.ProviderFailure):
                _log.warning("echo: live: %s", event.detail)
            elif isinstance(event, rt.SessionTerminated):
                return 0 if event.state is rt.SessionState.CLOSED else 1
        return 1

    await session.connect(rt.SessionSetup(model=config.model, voice=config.voice,
                                          instructions=instructions))
    # Hermes refuses agent runs until a Talk connection owns their results.
    owner = uuid.uuid4().hex
    talk_runs.attach_owner(
        talk_session_id=owner, generation_id=uuid.uuid4().hex[:12],
        hermes_session_id=_active_parent_session_id(),
        operator=getattr(getattr(session, "auth", None), "source", "live"),
        profile=talk_config.agent_profile(),
    )
    audio.start()
    workers = [asyncio.create_task(send_microphone()), asyncio.create_task(announce())]
    try:
        return await receive()
    finally:
        for task in (*workers, *pending):
            task.cancel()
        await asyncio.gather(*workers, *pending, return_exceptions=True)
        audio.stop()
        # The next session may own runs already; release only our own binding.
        if (talk_runs.current_owner() or {}).get("talkSessionId") == owner:
            talk_runs.detach_owner()
        await session.close()


__all__ = ["GUIDE", "OutputShaper", "announcement_texts", "home_arguments", "run_live_session"]
