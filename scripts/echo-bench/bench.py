"""Voice benchmark: the real Talk session, a fake microphone playing recorded prompts in real time.

Per run: connect (mint + socket), response latency (end of speech -> first audio out), tool latency
(end of speech -> tool call) and whether the tool and its arguments were right. Tools are stubbed:
nothing switches, no agent starts.  Usage: bench.py LABEL REPEATS [ids...]
"""
import array, asyncio, contextlib, io, json, os, statistics, sys, time
sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
import talk_cli, talk_echo, talk_tools

LABEL, REPEATS = sys.argv[1], int(sys.argv[2])
PROMPTS = [p for p in json.load(open("prompts.json")) if len(sys.argv) < 4 or p["id"] in sys.argv[3:]]
FRAME = 480 * 2          # 20 ms at 24 kHz
LISTEN_AFTER_S = 12.0

def speech_end(pcm):
    a = array.array("h"); a.frombytes(pcm)
    last = max((i for i in range(0, len(a), 240) if max(abs(x) for x in a[i:i+240]) > 800), default=len(a))
    return (last + 240) * 2

class BenchAudio(talk_echo.EchoAudio):
    def __init__(self):
        super().__init__(lambda _: None)
        self.first_audio = None
    def feed24(self, pcm):
        with self._lock:
            self._input.append(pcm); self._input_bytes += len(pcm)
    def queue_playback(self, pcm, item_id=None):
        if pcm and self.first_audio is None:
            self.first_audio = time.monotonic()
        super().queue_playback(pcm, item_id)

calls = []
def stub(name, reply):
    def run(arguments):
        calls.append((time.monotonic(), name, arguments)); return reply
    return run
talk_tools._HANDLERS["home_control"] = stub("home_control", "done")
talk_tools._HANDLERS["delegate_task"] = stub("delegate_task", "Started background run 1.")

async def one(p):
    pcm = open(f"clips/{p['id']}.pcm", "rb").read()
    end_at = speech_end(pcm)
    audio = BenchAudio()
    calls.clear()
    connected = asyncio.Event()
    out = io.StringIO()
    t0 = time.monotonic()
    class Tee(io.TextIOBase):
        def write(self, s):
            if "talk: connected" in s and not connected.is_set():
                connected.set()
            out.write(s); return len(s)
    async def run():
        with contextlib.redirect_stdout(Tee()):
            await talk_cli.run_talk_session(audio=audio, lane="echo")
    task = asyncio.create_task(run())
    await asyncio.wait_for(connected.wait(), 30)
    t_conn = time.monotonic() - t0
    await asyncio.sleep(0.5)
    for _ in range(25):  # 0.5 s of room before speaking
        audio.feed24(bytes(FRAME)); await asyncio.sleep(0.02)
    start = time.monotonic()
    for i in range(0, len(pcm), FRAME):
        audio.feed24(pcm[i:i+FRAME]); await asyncio.sleep(0.02)
    t_end = start + end_at / 48000
    deadline = time.monotonic() + LISTEN_AFTER_S
    while time.monotonic() < deadline:
        audio.feed24(bytes(FRAME)); await asyncio.sleep(0.02)
        if audio.first_audio and not audio.playback_pending and time.monotonic() - audio.first_audio > 3:
            break
    task.cancel(); await asyncio.gather(task, return_exceptions=True)
    tool = next(((t, n, a) for t, n, a in calls), None)
    ok = None
    if p["kind"] == "home":
        ok = bool(tool) and tool[1] == "home_control" and all(str(tool[2].get(k, "")).lower() == v for k, v in p["expect"].items())
    elif p["kind"] == "delegate":
        ok = bool(tool) and tool[1] == "delegate_task"
    elif p["kind"] == "answer":
        ok = tool is None
    said = " ".join(l for l in out.getvalue().splitlines() if l and not l.startswith("talk:")).strip()
    return {"id": p["id"], "connect_s": round(t_conn, 3),
            "response_s": round(audio.first_audio - t_end, 3) if audio.first_audio else None,
            "tool_s": round(tool[0] - t_end, 3) if tool else None,
            "tool": [tool[1], tool[2]] if tool else None, "ok": ok, "said": said[:160]}

async def main():
    rows = []
    for r in range(REPEATS):
        for p in PROMPTS:
            row = await one(p); row["run"] = r; rows.append(row)
            print(json.dumps(row), flush=True)
    json.dump(rows, open(f"results-{LABEL}.json", "w"), indent=1)
    print("\nsummary", LABEL)
    for key in ("connect_s", "response_s", "tool_s"):
        v = [x[key] for x in rows if x[key] is not None]
        if v: print(f"  {key:11} median {statistics.median(v):.2f}  min {min(v):.2f}  max {max(v):.2f}  (n={len(v)})")
    for p in PROMPTS:
        sub = [x for x in rows if x["id"] == p["id"]]
        rs = [x["response_s"] for x in sub if x["response_s"] is not None]
        ts = [x["tool_s"] for x in sub if x["tool_s"] is not None]
        print(f"  {p['id']:9} response {statistics.median(rs) if rs else float('nan'):.2f}s  tool {statistics.median(ts) if ts else float('nan'):.2f}s  correct {sum(1 for x in sub if x['ok'])}/{len(sub)}")

asyncio.run(main())
