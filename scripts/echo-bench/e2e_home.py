"""Danish and English home phrases: each must become exactly the expected home_control call, silently."""
import asyncio, os, sys
import e2e_echo as E
from e2e_echo import log

EXPECT = [
    ("da_lys_off", "lights", "off"), ("da_lys_on", "lights", "on"), ("da_tv_on", "tv", "power"),
    ("da_tv_off", "tv", "power"), ("da_spk_tv", "speaker", "optical"), ("da_spk_opt", "speaker", "optical"),
    ("da_spk_bt", "speaker", "bluetooth"),
    ("en_lights_off", "lights", "off"), ("en_lights_on", "lights", "on"), ("en_tv_on", "tv", "power"),
    ("en_tv_off", "tv", "power"), ("en_spk_tv", "speaker", "optical"), ("en_spk_bt", "speaker", "bluetooth"),
]

async def main():
    echo = E.Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(E.talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    rows = []
    for i, (clip, device, command) in enumerate(EXPECT):
        if i % 7 == 0:
            if i: await echo.say("bye"); await echo.ended(15); await asyncio.sleep(3)
            echo.wake(); await asyncio.sleep(0.5)
        n, before = len(E.homes), echo.out_bytes
        await echo.say(clip); await echo.quiet(2.5)
        got = [(a.get("device"), a.get("command")) for a in E.homes[n:]]
        ok = got == [(device, command)]
        spoke = echo.out_bytes > before
        rows.append((clip, ok, got, "spoke" if spoke else "silent"))
        log(f"{clip}: {'OK' if ok else 'WRONG'} {got} {'spoke' if spoke else 'silent'}")
    log("== Danish question gets a Danish answer")
    await echo.say("da_question"); await echo.quiet(2.5)
    await echo.say("da_bye"); await echo.ended(15)
    log(f"SCORE {sum(r[1] for r in rows)}/{len(rows)} correct, {sum(r[3]=='silent' for r in rows)} silent")
    surface.cancel(); server.close()

asyncio.run(main())
