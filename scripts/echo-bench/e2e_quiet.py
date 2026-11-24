"""A call after one command in a loud room with desk clicks: it must close by itself and not talk.

  e2e_quiet.py OUTDIR ROUNDS
"""
import array, asyncio, logging, random, sys
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
import e2e_echo as E
from e2e_echo import log

def room(seconds, level=0.025, clicks=True):
    n = int(seconds * 16000)
    out = array.array("h", (int(max(-32768, min(32767, random.gauss(0, level * 32767)))) for _ in range(n)))
    for start in range(8000, n - 1600, 24000) if clicks else ():
        for j in range(random.choice((800, 1600, 2400))):
            out[start + j] = int(random.gauss(0, 0.12 * 32767)) % 30000 - 15000
    return out.tobytes()

async def main():
    echo = E.Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(E.talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    for _ in range(int(sys.argv[2])):
        echo.wake(); await asyncio.sleep(0.5)
        n, ends, out = len(E.homes), len(echo.ends), echo.out_bytes
        await echo.say("lights")
        echo.pending += room(40)
        await echo.ended(38)
        echo.pending.clear()
        log(f"ROUND closed={len(echo.ends) > ends} homes={len(E.homes) - n} spoke={(echo.out_bytes - out) / 48000:.1f}s")
        await asyncio.sleep(4)
    surface.cancel(); server.close()

asyncio.run(main())
