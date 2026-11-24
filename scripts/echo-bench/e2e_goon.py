"""A handoff, then more commands at once: the call must stay for them and close after.

Also counts real goodbyes that end the call. Tools are stubbed.
  e2e_goon.py OUTDIR ROUNDS
"""
import asyncio, sys
import e2e_echo as E
from e2e_echo import log

E.talk_tools._HANDLERS["delegate_task"] = lambda a: "WORK_STARTED run 1"

async def main():
    echo = E.Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(E.talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    stayed, closed = 0, 0
    rounds = int(sys.argv[2])
    for _ in range(rounds):
        echo.wake(); await asyncio.sleep(0.5)
        n, ends = len(E.homes), len(echo.ends)
        await echo.say("en_weather"); await asyncio.sleep(2.5)
        await echo.say("da_taend_lys"); await asyncio.sleep(2)
        await echo.say("da_tv"); await asyncio.sleep(2)
        got = [(a.get("device"), a.get("command")) for a in E.homes[n:]]
        stayed += got == [("lights", "on"), ("tv", "power")]
        log(f"after the handoff: {got}, ended early: {len(echo.ends) > ends}")
        if len(echo.ends) == ends:
            await echo.ended(15)
        closed += len(echo.ends) > ends
        await asyncio.sleep(4)
    log(f"SCORE went on {stayed}/{rounds}, closed by itself {closed}/{rounds}")
    surface.cancel(); server.close()

asyncio.run(main())
