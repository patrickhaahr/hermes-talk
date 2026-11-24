"""End-to-end Echo test: a fake hassmic drives the real talk_echo.serve().

Real Realtime voice (cedar), real Hermes delegations through the api_server;
only home_control is stubbed. Files runs under its own return session id and
say socket so it never touches the live Echo service.
  e2e_echo.py OUTDIR
"""
import array, asyncio, json, os, sys, time, wave
from pathlib import Path

sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
# `hermes` loads ~/.hermes/.env itself; without the key the run falls back to `hermes -z`.
for line in Path("~/.hermes/.env").expanduser().read_text().splitlines():
    if line.startswith("API_SERVER_KEY="):
        os.environ["API_SERVER_KEY"] = line.split("=", 1)[1].strip().strip('"')
import talk_echo, talk_runs, talk_tools
from talk_echo import encode_event, read_event

OUT = Path(sys.argv[1]); OUT.mkdir(parents=True, exist_ok=True)
CLIPS = Path(__file__).parent / "e2e-clips"
SESSION = "talk-echo-bench"
CHUNK = 256                       # samples per hassmic mic chunk at 16 kHz
CHUNK_S = CHUNK / 16000

talk_echo.RETURN_SESSION_ID = SESSION
talk_echo.say_socket_path = lambda: Path("/tmp/talk-echo-bench.sock")
homes = []
talk_tools._HANDLERS["home_control"] = lambda a: homes.append(a) or f"done: {a.get('device')} {a.get('command')}"

T0 = time.monotonic()
def log(msg): print(f"{time.strftime('%H:%M:%S')} bench +{time.monotonic() - T0:7.2f}s: {msg}", flush=True)

def to16k(pcm24: bytes) -> bytes:
    a = array.array("h"); a.frombytes(pcm24)
    out = array.array("h")
    for j in range(len(a) * 2 // 3):
        x = j * 1.5; i = int(x); f = x - i
        b = a[i + 1] if i + 1 < len(a) else a[i]
        out.append(int(a[i] * (1 - f) + b * f))
    return out.tobytes()

class Echo:
    """hassmic's side of the link: mic chunks at real time, playback recorded."""
    def __init__(self):
        self.writer = None
        self.pending = bytearray()
        self.out_last = 0.0          # monotonic time of the last playback chunk
        self.out_bytes = 0
        self.ends = []               # (time, event type) for audio-stop / error
        self.segment = bytearray()
        self.segments = 0

    async def handle(self, reader, writer):
        self.writer = writer
        asyncio.create_task(self.mic())
        while (e := await read_event(reader)) is not None:
            if e.type == "audio-chunk":
                if not self.segment:
                    log(f"echo speaks")
                self.segment += e.payload; self.out_bytes += len(e.payload); self.out_last = time.monotonic()
            elif e.type in ("audio-stop", "error"):
                self.ends.append((time.monotonic(), e.type))
                log(f"echo ends the call ({e.type})")
            elif e.type not in ("audio-start", "describe", "run-satellite"):
                log(f"event {e.type}")
            if e.type in ("audio-stop", "error") or (e.type == "audio-start" and self.segment):
                self.flush()

    def flush(self):
        if self.segment:
            self.segments += 1
            p = OUT / f"out{self.segments:02d}.wav"
            with wave.open(str(p), "wb") as w:
                w.setnchannels(1); w.setsampwidth(2); w.setframerate(24000); w.writeframes(bytes(self.segment))
            log(f"saved {p.name} ({len(self.segment) / 48000:.1f} s)")
            self.segment = bytearray()

    async def mic(self):
        nxt = time.monotonic()
        while True:
            chunk = bytes(self.pending[:CHUNK * 2]); del self.pending[:CHUNK * 2]
            chunk = chunk.ljust(CHUNK * 2, b"\0")
            self.writer.write(encode_event("audio-chunk", {"rate": 16000, "width": 2, "channels": 1}, chunk))
            nxt += CHUNK_S
            await asyncio.sleep(max(0, nxt - time.monotonic()))

    # -- steps -------------------------------------------------------------------------
    def wake(self):
        log("wake word")
        self.writer.write(encode_event("run-pipeline", {"start_stage": "asr", "end_stage": "tts"}))
        # as hassmic does: 0.8 s of silence, the end of the wake word, then the room
        tail = array.array("h", to16k((CLIPS / "hello.pcm").read_bytes()))
        loud = next(i for i, x in enumerate(tail) if abs(x) > 1500)
        self.pending += bytes(25600) + tail[loud:loud + 5600].tobytes() + array.array("h", [40, -40] * 1600).tobytes()

    async def say(self, clip):
        pcm = to16k((CLIPS / f"{clip}.pcm").read_bytes())
        log(f"operator says {clip!r}")
        self.pending += pcm
        while self.pending:
            await asyncio.sleep(0.02)
        self.spoke_at = time.monotonic()
        log(f"operator done ({clip})")

    async def quiet(self, s, limit=120):
        """Wait until the Echo has played nothing for ``s`` seconds."""
        start = time.monotonic()
        while time.monotonic() - max(self.out_last, start) < s + talk_echo.PLAYBACK_LEAD_S:
            await asyncio.sleep(0.1)
            if time.monotonic() - start > limit:
                break

    async def ended(self, limit):
        n = len(self.ends); start = time.monotonic()
        while len(self.ends) == n and time.monotonic() - start < limit:
            await asyncio.sleep(0.05)
        if len(self.ends) > n:
            log(f"call ended {self.ends[-1][0] - self.spoke_at:.2f} s after the operator stopped")
        else:
            log("call did NOT end")

    async def delivered(self, limit):
        """Wait until every bench run is terminal and delivered."""
        start = time.monotonic()
        while time.monotonic() - start < limit:
            mine = [r for r in talk_runs._RUNS.values()
                    if (r.get("ticket") or {}).get("hermesSessionId") == SESSION]
            if mine and all(r["status"] in talk_runs.TERMINAL_STATUSES
                            and r.get("delivery") == talk_runs.DELIVERED for r in mine):
                log(f"all {len(mine)} runs delivered")
                return
            await asyncio.sleep(0.5)
        log(f"NOT delivered in time: {[(k, r['status'], r.get('delivery')) for k, r in talk_runs._RUNS.items()]}")


async def main():
    echo = Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    surface = asyncio.create_task(talk_echo.serve("127.0.0.1", port))
    await asyncio.sleep(12)  # standby connects

    log("== 1. lights: silent")
    echo.wake(); await echo.say("lights"); await echo.quiet(4)
    log(f"home stub calls: {homes}")

    log("== 2. delegate: 'On it.', then goodbye")
    await echo.say("pi_disk"); await echo.quiet(4)
    await echo.say("bye"); await echo.ended(15)

    log("== 3. result after the call: spoken on its own")
    await echo.delivered(300); await echo.quiet(4); await echo.ended(30)

    log("== 4. parallel: two tasks in one request")
    echo.wake(); await echo.say("parallel"); await echo.quiet(4)
    await echo.delivered(400); await echo.quiet(4)

    log("== 5. follow-up through voice history")
    echo.wake(); await echo.say("followup"); await echo.quiet(2)
    await echo.delivered(300); await echo.quiet(3)
    await echo.say("bye"); await echo.ended(15)

    for r in talk_runs._RUNS.values():
        if (r.get("ticket") or {}).get("hermesSessionId") == SESSION:
            log(f"run: {r.get('status')} lane={(r.get('meta') or {}).get('lane')} label={r.get('label')!r}")
    surface.cancel(); server.close()

if __name__ == "__main__":
    asyncio.run(main())
