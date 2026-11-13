"""Speak each benchmark prompt once with gpt-realtime-2.1 and keep it as 24 kHz PCM16."""
import asyncio, base64, json, os, sys
sys.path.insert(0, os.path.expanduser("~/.hermes/plugins/hermes-talk"))
import aiohttp, talk_auth, talk_wire

PROMPTS = json.load(open(sys.argv[1]))
OUT = sys.argv[2]

async def speak(text):
    auth = talk_auth.resolve_auth()
    d = talk_wire.mint_ephemeral_session(auth_token=auth.token, model="gpt-realtime-2.1", voice="ash",
        instructions="You are a voice actor. Read the user's line aloud exactly as written, in a natural casual voice, nothing else.")
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(f"{talk_wire.OPENAI_REALTIME_WS_URL}?model=gpt-realtime-2.1",
                                headers={"Authorization": f"Bearer {d.client_secret}"}) as ws:
            await ws.send_json({"type": "session.update", "session": {"type": "realtime", "turn_detection": None} if False else {"type": "realtime"}})
            await ws.send_json({"type": "conversation.item.create", "item": {"type": "message", "role": "user",
                "content": [{"type": "input_text", "text": text}]}})
            await ws.send_json({"type": "response.create"})
            pcm = bytearray()
            async for m in ws:
                e = json.loads(m.data)
                if e["type"] in ("response.output_audio.delta", "response.audio.delta"):
                    pcm += base64.b64decode(e["delta"])
                elif e["type"] == "response.done":
                    return bytes(pcm)
                elif e["type"] == "error":
                    raise RuntimeError(e)

async def main():
    os.makedirs(OUT, exist_ok=True)
    for p in PROMPTS:
        pcm = await speak(p["say"])
        open(f"{OUT}/{p['id']}.pcm", "wb").write(pcm)
        print(p["id"], f"{len(pcm)/48000:.2f}s")

asyncio.run(main())
