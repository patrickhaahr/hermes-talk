"""One real handoff end to end: 'On it.', the call hangs up, and the result is spoken later.

Real Hermes work; home_control is stubbed.
  e2e_handoff.py OUTDIR CLIP...
"""
import asyncio, sys
import e2e_echo as E
from e2e_echo import log

async def main():
    echo = E.Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(E.talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    for clip in sys.argv[2:]:
        log(f"== {clip}")
        echo.wake(); await asyncio.sleep(0.5)
        await echo.say(clip)
        await echo.ended(15)
        await echo.delivered(400); await echo.quiet(4); await echo.ended(60)
        await asyncio.sleep(4)
    surface.cancel(); server.close()

asyncio.run(main())
