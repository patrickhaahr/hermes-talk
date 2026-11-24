"""Wake-tail and fragment E2E: scraps get at most a "Sorry?" and no run, and the Echo must still take the real request.
  e2e_noise.py OUTDIR [TALK_ECHO_DIR_OVERRIDE unused]
"""
import asyncio, sys, time, os
import e2e_echo as E
from e2e_echo import log, talk_runs

async def spoke_since(echo, n, wait):
    await asyncio.sleep(wait)
    return echo.out_bytes > n

async def main():
    echo = E.Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(E.talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    results = []
    for scrap in ["eo", "mi", "frag"]:
        log(f"== wake, scrap {scrap!r}, silence")
        echo.wake(); n = echo.out_bytes
        await echo.say(scrap)
        spoke = await spoke_since(echo, n, 5)
        results.append((scrap, "SPOKE" if spoke else "quiet")); log(f"{scrap}: {'SPOKE' if spoke else 'quiet'}")
        await echo.quiet(1); await echo.say("bye"); await echo.ended(15)
        await asyncio.sleep(3)
    log("== wake, scrap, then the request right after (the 18:47 call)")
    echo.wake(); n = echo.out_bytes
    await echo.say("eo"); await asyncio.sleep(0.4); await echo.say("pi_disk")
    await echo.quiet(4)
    runs = [r for r in talk_runs._RUNS.values() if (r.get("ticket") or {}).get("hermesSessionId") == E.SESSION]
    results.append(("eo+pi_disk", f"{len(runs)} run(s) started"))
    log("== the time, asked for")
    n = echo.out_bytes; await echo.say("time"); spoke = await spoke_since(echo, n, 5)
    results.append(("time", "SPOKE" if spoke else "quiet"))
    await echo.quiet(2); await echo.say("bye"); await echo.ended(15)
    log(f"RESULTS {results}")
    surface.cancel(); server.close()

asyncio.run(main())
