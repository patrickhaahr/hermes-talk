"""One Echo call: wake, play each clip with QUIET seconds (default 4) after it, then 'Tak, farvel'.

  e2e_call.py OUTDIR clip1,clip2,...   (clip ids from clips.json; replays a real call from the journal)
"""
import asyncio, os, sys, logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
import e2e_echo as E
from e2e_echo import talk_echo
CLIPS = sys.argv[2].split(",")
async def main():
    echo = E.Echo(); server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    s = asyncio.create_task(talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1])); await asyncio.sleep(12)
    echo.wake(); await asyncio.sleep(0.5)
    for clip in CLIPS:
        await echo.say(clip); await echo.quiet(float(os.environ.get("QUIET", 4)))
    await echo.say("da_bye"); await echo.ended(15)
asyncio.run(main())
