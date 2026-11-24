"""A/B: play pre-mixed 16 kHz mic streams (command at 1.5 s over music) against one plugin revision.
  e2e_room.py PLUGIN_DIR STREAM_DIR LABEL stream1,stream2,...   (every stream is one fresh call; tools are stubbed)"""
import asyncio, os, sys, json, pathlib
PLUG, DIR, LABEL, NAMES = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(",")
sys.argv = ["x", f"/tmp/e2e-room-{os.getpid()}"]
import re
src = open(os.path.expanduser("~/bench/e2e_echo.py")).read().replace("~/.hermes/plugins/hermes-talk", PLUG)
E = {"__file__": os.path.expanduser("~/bench/e2e_echo.py"), "__name__": "e2e_echo"}
exec(compile(src, "e2e_echo", "exec"), E)
tt, te = E["talk_tools"], E["talk_echo"]
te.say_socket_path = lambda: pathlib.Path(f"/tmp/e2e-room-{os.getpid()}.sock")
acts = []
real = dict(tt._HANDLERS)
def stub(n):
    def f(a):
        acts.append((n, a))
        if n == "home_control": return f"done: {a.get('device')} {a.get('command')}"
        if n == "not_for_me": return "Ignored; still listening."
        return "WORK_STARTED: run 1" if n == "delegate_task" else "Nothing."
    return f
for n in real:
    if n != "end_call": tt._HANDLERS[n] = stub(n)
WANT = {"lights_on": ("lights","on"), "lights_off": ("lights","off"), "tv_on": ("tv","power"), "spk_bt": ("speaker","bluetooth"), "lys_off": ("lights","off")}
async def one(name):
    acts.clear()
    echo = E["Echo"]()
    server = await asyncio.start_server(echo.handle, "127.0.0.1", 0)
    s = asyncio.create_task(te.serve("127.0.0.1", server.sockets[0].getsockname()[1]))
    await asyncio.sleep(10)
    echo.wake(); echo.pending += (pathlib.Path(DIR) / f"{name}.pcm16").read_bytes()
    await asyncio.sleep(17)
    home = [(a.get("device"), a.get("command")) for n, a in acts if n == "home_control"]
    want = next(v for k, v in WANT.items() if k in name)
    print("ROW", json.dumps([LABEL, name, home == [want], home, [n for n, _ in acts], echo.out_bytes // 48]), flush=True)
    s.cancel(); server.close(); await asyncio.sleep(1)
async def main():
    for n in NAMES:
        try: await one(n)
        except Exception as e: print("ROW", json.dumps([LABEL, n, None, repr(e)]), flush=True)
    os._exit(0)
asyncio.run(main())
