"""One clip through the real Realtime session, real tools (home stubbed); prints what the model said.
Usage: check_realtime.py CLIP LISTEN_SECONDS"""
import asyncio, contextlib, io, os, sys, time
sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
import talk_cli, talk_echo, talk_tools
talk_tools._HANDLERS["home_control"] = lambda a: "done"
FRAME = 960
CLIP, LISTEN = sys.argv[1], float(sys.argv[2])

class BenchAudio(talk_echo.EchoAudio):
    def __init__(self):
        super().__init__(lambda _: None)
    def feed24(self, pcm):
        with self._lock:
            self._input.append(pcm); self._input_bytes += len(pcm)

async def main():
    pcm = open(f"clips/{CLIP}.pcm", "rb").read()
    audio = BenchAudio()
    t0 = time.monotonic()
    class Tee(io.TextIOBase):
        def write(self, s):
            sys.stderr.write(s.replace("\n", f"\n[{time.monotonic()-t0:5.1f}] ")); return len(s)
    async def run():
        with contextlib.redirect_stdout(Tee()):
            await talk_cli.run_talk_session(audio=audio, lane="echo")
    task = asyncio.create_task(run())
    await asyncio.sleep(4)
    for i in range(0, len(pcm), FRAME):
        audio.feed24(pcm[i:i+FRAME]); await asyncio.sleep(0.02)
    end = time.monotonic() + LISTEN
    while time.monotonic() < end:
        audio.feed24(bytes(FRAME)); await asyncio.sleep(0.02)
    task.cancel(); await asyncio.gather(task, return_exceptions=True)
asyncio.run(main())
