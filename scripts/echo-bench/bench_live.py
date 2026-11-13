"""GPT-Live counterpart of bench.py: same clips, same metrics, a delegation counts as the tool call.

Usage: bench_live.py LABEL REPEATS [ids...]   (TALK_LIVE_MODEL / TALK_LIVE_VOICE pick model and voice)
"""
import array, asyncio, json, os, statistics, sys, time
sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
import talk_host, talk_identity, talk_live_config, talk_realtime as rt
from talk_live_realtime import LiveRealtimeSession

LABEL, REPEATS = sys.argv[1], int(sys.argv[2])
PROMPTS = [p for p in json.load(open("prompts.json")) if len(sys.argv) < 4 or p["id"] in sys.argv[3:]]
FRAME = 480 * 2
LISTEN_AFTER_S = 12.0
CONFIG = talk_live_config.resolve_live_config()
INSTRUCTIONS = talk_identity.build_instructions(talk_host.host().identity_sections(), tools=[], lane="echo")

def speech_end(pcm):
    a = array.array("h"); a.frombytes(pcm)
    last = max((i for i in range(0, len(a), 240) if max(abs(x) for x in a[i:i+240]) > 800), default=len(a))
    return (last + 240) * 2

def audible(pcm):
    # WebRTC keeps sending frames while the model is silent: only sound counts.
    a = array.array("h"); a.frombytes(pcm[: len(pcm) - len(pcm) % 2])
    return bool(a) and max(abs(x) for x in a) > 500

async def one(p):
    pcm = open(f"clips/{p['id']}.pcm", "rb").read()
    end_at = speech_end(pcm)
    auth = talk_live_config.resolve_live_auth(config=CONFIG)
    session = LiveRealtimeSession(auth=auth, config=CONFIG)
    t0 = time.monotonic()
    await session.connect(rt.SessionSetup(model=CONFIG.model, voice=CONFIG.voice, instructions=INSTRUCTIONS))
    t_conn = time.monotonic() - t0
    first_audio = delegation = None
    heard, said = [], []
    last_audio = [0.0]
    async def receive():
        nonlocal first_audio, delegation
        async for ev in session:
            now = time.monotonic()
            if isinstance(ev, rt.OutputAudio) and audible(ev.data):
                first_audio = first_audio or now; last_audio[0] = now
            elif isinstance(ev, rt.DelegationRequested) and delegation is None:
                delegation = (now, ev.prompt)
                await session.send([rt.SubmitDelegationResult(ev.delegation_id, "Done.", kind="commentary")])
            elif isinstance(ev, rt.Transcript) and ev.final:
                (heard if ev.role is rt.TranscriptRole.USER else said).append(ev.text)
    rx = asyncio.create_task(receive())
    await asyncio.sleep(0.5)
    for _ in range(25):
        await session.send([rt.AppendInputAudio(bytes(FRAME))]); await asyncio.sleep(0.02)
    start = time.monotonic()
    for i in range(0, len(pcm), FRAME):
        await session.send([rt.AppendInputAudio(pcm[i:i+FRAME])]); await asyncio.sleep(0.02)
    t_end = start + end_at / 48000
    deadline = time.monotonic() + LISTEN_AFTER_S
    while time.monotonic() < deadline:
        await session.send([rt.AppendInputAudio(bytes(FRAME))]); await asyncio.sleep(0.02)
        if first_audio and time.monotonic() - last_audio[0] > 2.0 and time.monotonic() - first_audio > 3:
            break
    await session.close(); rx.cancel(); await asyncio.gather(rx, return_exceptions=True)
    ok = (delegation is not None) if p["kind"] in ("home", "delegate") else delegation is None
    return {"id": p["id"], "connect_s": round(t_conn, 3),
            "response_s": round(first_audio - t_end, 3) if first_audio else None,
            "tool_s": round(delegation[0] - t_end, 3) if delegation else None,
            "tool": ["delegation", delegation[1]] if delegation else None, "ok": ok,
            "heard": " ".join(heard)[:120], "said": " ".join(said)[:160]}

async def main():
    rows = []
    for r in range(REPEATS):
        for p in PROMPTS:
            try:
                row = await one(p)
            except Exception as exc:
                row = {"id": p["id"], "error": f"{type(exc).__name__}: {exc}"[:200], "connect_s": None, "response_s": None, "tool_s": None, "ok": False}
            row["run"] = r; rows.append(row)
            print(json.dumps(row), flush=True)
    json.dump(rows, open(f"results-{LABEL}.json", "w"), indent=1)
    print("\nsummary", LABEL, CONFIG.model, CONFIG.voice)
    for key in ("connect_s", "response_s", "tool_s"):
        v = [x[key] for x in rows if x.get(key) is not None]
        if v: print(f"  {key:11} median {statistics.median(v):.2f}  min {min(v):.2f}  max {max(v):.2f}  (n={len(v)})")
    for p in PROMPTS:
        sub = [x for x in rows if x["id"] == p["id"]]
        rs = [x["response_s"] for x in sub if x.get("response_s") is not None]
        ts = [x["tool_s"] for x in sub if x.get("tool_s") is not None]
        print(f"  {p['id']:9} response {statistics.median(rs) if rs else float('nan'):.2f}s  tool {statistics.median(ts) if ts else float('nan'):.2f}s  correct {sum(1 for x in sub if x['ok'])}/{len(sub)}")

asyncio.run(main())
