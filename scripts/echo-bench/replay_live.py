"""Replay bench clips through talk_echo_live.run_live_session (the real Echo Live path), home/agent stubbed."""
import asyncio, logging, os, sys, time
sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
import talk_echo, talk_echo_live, talk_home, talk_host
logging.basicConfig(level=logging.INFO, format="%(relativeCreated)6.0f %(message)s")
talk_echo._log.setLevel(logging.INFO)
talk_home.run = lambda a: print("  >> HOME", a) or f"done: {a['device']} {a['command']}"
real_host = talk_host.host()
class H:
    def __getattr__(s, n): return getattr(real_host, n)
    def run_agent(s, p, *a, **k): print("  >> AGENT", p); return "Zaza has 120 GB free on the main drive."
if not os.environ.get("REAL"): talk_host.host = lambda: H()
class Audio:
    def __init__(s, pcm): s.chunks=[pcm[i:i+960] for i in range(0,len(pcm),960)]; s.t=None; s.heard=0
    def start(s): pass
    def stop(s): pass
    def read_input_chunk(s):
        now=time.monotonic(); s.t=s.t or now
        if s.chunks and now-s.t >= 0.02: s.t+=0.02; return s.chunks.pop(0)
        return None
    def queue_playback(s, pcm, item_id=None): s.heard+=len(pcm); open(f"/tmp/live-out.pcm","ab").write(pcm)
    def request_hang_up(s): print("  >> HANG UP")
async def one(clip):
    pcm = bytes(48000) + open(f"clips/{clip}.pcm","rb").read() + bytes(48000*10)
    audio = Audio(pcm)
    task = asyncio.create_task(talk_echo_live.run_live_session(audio=audio, lane="echo"))
    await asyncio.sleep(float(os.environ.get("WAIT", 25))); task.cancel(); await asyncio.gather(task, return_exceptions=True)
    print(f"== {clip}: heard {audio.heard/48000:.1f}s of reply audio\n")
async def main():
    for c in sys.argv[1:]: await one(c)
asyncio.run(main())
