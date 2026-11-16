"""Hang-up and home-action latency under one turn-detection setting.

Reuses e2e_echo's fake hassmic; adds a low noise floor to the microphone like a room.
  TALK_SEMANTIC_EAGERNESS=high e2e_bye.py OUTDIR ROUNDS
"""
import asyncio, os, random, statistics, sys, time
sys.argv = [sys.argv[0], sys.argv[1], *sys.argv[2:]]
ROUNDS = int(sys.argv[2])
import runpy
ns = runpy.run_path(os.path.join(os.path.dirname(__file__), "e2e_echo.py"), run_name="lib")
Echo, log, homes, encode_event = ns["Echo"], ns["log"], ns["homes"], ns["encode_event"]
CHUNK, CHUNK_S = ns["CHUNK"], ns["CHUNK_S"]
import array
NOISE = 0.004  # rms as a fraction of full scale; the Echo's median mic level is about 0.002

def noisy(chunk: bytes) -> bytes:
    a = array.array("h"); a.frombytes(chunk)
    return array.array("h", (max(-32768, min(32767, x + int(random.gauss(0, NOISE * 32767)))) for x in a)).tobytes()

class NoisyEcho(Echo):
    async def mic(self):
        nxt = time.monotonic()
        while True:
            chunk = bytes(self.pending[:CHUNK * 2]); del self.pending[:CHUNK * 2]
            self.writer.write(encode_event("audio-chunk", {"rate": 16000, "width": 2, "channels": 1},
                                           noisy(chunk.ljust(CHUNK * 2, b"\0"))))
            nxt += CHUNK_S
            await asyncio.sleep(max(0, nxt - time.monotonic()))

async def main():
    import talk_echo
    echo = NoisyEcho()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    acts, byes = [], []
    for r in range(ROUNDS):
        echo.wake(); await asyncio.sleep(0.5)
        n = len(homes); await echo.say("lights"); t = echo.spoke_at
        stamps = ns.setdefault("stamps", [])
        for _ in range(100):
            if len(homes) > n: break
            await asyncio.sleep(0.05)
        acts.append(time.monotonic() - t if len(homes) > n else float("nan"))
        await echo.quiet(2)
        await echo.say("bye"); e = len(echo.ends); await echo.ended(15)
        byes.append(echo.ends[-1][0] - echo.spoke_at if len(echo.ends) > e else float("nan"))
        log(f"round {r + 1}: lights action {acts[-1]:.2f} s, call ended {byes[-1]:.2f} s")
        await asyncio.sleep(3)
    ok = lambda xs: [x for x in xs if x == x]
    log(f"SUMMARY eagerness={os.environ.get('TALK_SEMANTIC_EAGERNESS')}: lights median "
        f"{statistics.median(ok(acts)):.2f} s, hang-up median {statistics.median(ok(byes)):.2f} s "
        f"(max {max(ok(byes)):.2f}), failures {len(acts) - len(ok(acts))}/{len(byes) - len(ok(byes))}")
    surface.cancel(); server.close()

asyncio.run(main())
