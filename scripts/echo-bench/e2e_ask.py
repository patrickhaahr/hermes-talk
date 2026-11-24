"""The operator's own phrases, one call each: home, time and date on the Echo, the rest to Hermes.

A handoff must hang up by itself. delegate_task and home_control are stubbed.
  e2e_ask.py OUTDIR
"""
import asyncio
import e2e_echo as E
from e2e_echo import log

handoffs = []
E.talk_tools._HANDLERS["delegate_task"] = lambda a: handoffs.append(a.get("task")) or "WORK_STARTED run 1"
HOME = {
    "da_taend_lys": ("lights", "on"), "da_sluk_lys": ("lights", "off"), "da_tv": ("tv", "power"),
    "youtube": ("tv", "1"), "play": ("tv", "ok"), "da_tilbage": ("tv", "back"),
    "mute": ("speaker", "mute"), "bluetooth": ("speaker", "bluetooth"), "optical": ("speaker", "optical"),
}
SPOKEN = ["da_klokken", "da_dag"]
HANDOFF = ["da_status", "da_nyheder", "da_cando", "whatcan", "en_weather"]

async def main():
    echo = E.Echo()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    surface = asyncio.create_task(E.talk_echo.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(12)
    good = 0
    clips = [*HOME, *SPOKEN, *HANDOFF]
    for clip in clips:
        echo.wake(); await asyncio.sleep(0.5)
        h, d, spoken, ends = len(E.homes), len(handoffs), echo.out_bytes, len(echo.ends)
        await echo.say(clip); await echo.quiet(3)
        if len(handoffs) > d and len(echo.ends) == ends:
            await echo.ended(8)
        got = [(a.get("device"), a.get("command")) for a in E.homes[h:]]
        sent, spoke, ended = handoffs[d:], (echo.out_bytes - spoken) / 48000, len(echo.ends) > ends
        ok = ((got, sent) == ([HOME[clip]], []) and not spoke if clip in HOME else
              not got and len(sent) == 1 and ended and 0 < spoke < 3 if clip in HANDOFF else
              not got and not sent and spoke > 0)
        good += ok
        log(f"{clip}: {'OK' if ok else 'WRONG'} home={got} handoff={sent} spoke={spoke:.1f}s ended={ended}")
        if not ended:
            await echo.say("bye"); await echo.ended(15)
        await asyncio.sleep(4)
    log(f"SCORE {good}/{len(clips)}")
    surface.cancel(); server.close()

asyncio.run(main())
