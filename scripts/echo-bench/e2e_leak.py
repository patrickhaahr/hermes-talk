"""Echo leak E2E: the fake Echo hands the reply back into its mic, as the canceller does at its worst.

  e2e_leak.py OUTDIR [TALK_ECHO_PY]   (TALK_ECHO_PY: an alternative talk_echo.py to test, e.g. the old one)
Step 1: a long announcement plays with the leak on; nobody talks. Every transcript is a phantom.
Step 2: another announcement; a wake word ("detection") cuts it, then the operator says goodbye.
"""
import array, asyncio, importlib.util, os, socket, sys, time
from pathlib import Path
os.environ.setdefault("TALK_ECHO_NO_DUMP", "1")
if len(sys.argv) > 2:
    sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
    spec = importlib.util.spec_from_file_location("talk_echo", sys.argv[2])
    mod = importlib.util.module_from_spec(spec); sys.modules["talk_echo"] = mod; spec.loader.exec_module(mod)
import e2e_echo as E
from e2e_echo import talk_echo, encode_event, log

LEAK_RATIO, LEAK_DELAY_S = 1.2, 0.4
LONG = ("Tell the operator, in your own words and at some length, this message: the nightly backup finished, "
        "all twelve pods on zaza are healthy, the pi has forty gigabytes free, Loki recovered after a short wobble, "
        "two search engines failed to load and need a look tomorrow, and the weather in Copenhagen stays grey with drizzle "
        "through the evening.")

def from24(pcm24: bytes) -> array.array:
    a = array.array("h"); a.frombytes(pcm24)
    return array.array("h", (a[int(j * 1.5)] for j in range(len(a) * 2 // 3)))

class LeakyEcho(E.Echo):
    def __init__(self):
        super().__init__(); self.leak = []          # (due time, 16 kHz samples)
        self.transcripts = 0
    async def handle(self, reader, writer):
        self.writer = writer
        asyncio.create_task(self.mic())
        while (e := await E.read_event(reader)) is not None:
            if e.type == "audio-chunk":
                if not self.segment: log("echo speaks")
                self.segment += e.payload; self.out_bytes += len(e.payload); self.out_last = time.monotonic()
                self.leak.append((time.monotonic() + LEAK_DELAY_S, from24(e.payload)))
            elif e.type in ("audio-stop", "error"):
                self.ends.append((time.monotonic(), e.type)); log(f"echo ends the call ({e.type})"); self.flush()
    async def mic(self):
        nxt = time.monotonic(); buf = array.array("h")
        while True:
            now = time.monotonic()
            while self.leak and self.leak[0][0] <= now: buf.extend(self.leak.pop(0)[1])
            speech = array.array("h"); speech.frombytes(bytes(self.pending[:E.CHUNK * 2])); del self.pending[:E.CHUNK * 2]
            out = array.array("h", [0] * E.CHUNK)
            for i in range(E.CHUNK):
                v = (speech[i] if i < len(speech) else 0) + (int(buf[i] * LEAK_RATIO) if i < len(buf) else 0)
                out[i] = max(-32768, min(32767, v))
            del buf[:E.CHUNK]
            self.writer.write(encode_event("audio-chunk", {"rate": 16000, "width": 2, "channels": 1}, out.tobytes()))
            nxt += E.CHUNK_S
            await asyncio.sleep(max(0, nxt - time.monotonic()))

def say(text):
    s = socket.socket(socket.AF_UNIX); s.connect(str(talk_echo.say_socket_path())); s.sendall(text.encode() + b"\n"); s.close()

async def main():
    echo = LeakyEcho()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    log("== 1. long announcement with the leak on, nobody talks")
    say(LONG); await asyncio.sleep(3); await echo.quiet(3, limit=90)
    log(f"step 1 done: {echo.segments} playback segments so far")
    log("== 2. announcement, wake word over it, then goodbye")
    say(LONG); 
    while echo.out_bytes == 0 or time.monotonic() - echo.out_last > 0.5: await asyncio.sleep(0.05)
    await asyncio.sleep(3)
    t = time.monotonic(); log("wake word (detection)")
    echo.writer.write(encode_event("detection"))
    while time.monotonic() - echo.out_last < 0.05 and time.monotonic() - t < 10: await asyncio.sleep(0.02)
    log(f"Echo silent {echo.out_last - t:.2f} s after the wake word")
    await echo.say("bye"); await echo.ended(15)
    surface.cancel(); server.close()

asyncio.run(main())
