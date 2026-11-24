"""Danish commands at a casual level over a quiet or loud room, as 16 kHz streams for e2e_room.py.

Voices: `uvx edge-tts -v da-DK-JeppeNeural -t "Sluk lyset." --write-media lys_off_jeppe.mp3`, then ffmpeg
to 16 kHz mono s16le `.raw`; the file name must hold an e2e_room WANT key.
  uv run --no-project --with numpy python make_soft.py RAW_DIR STREAM_DIR
"""
import pathlib, sys
import numpy as np

RATE, FRAME = 16000, 1024
LEVELS = ((0.022, 0.004), (0.028, 0.005), (0.05, 0.012))
rng = np.random.default_rng(1)
out = pathlib.Path(sys.argv[2]); out.mkdir(parents=True, exist_ok=True)
for raw in sorted(pathlib.Path(sys.argv[1]).glob("*.raw")):
    voice = np.frombuffer(raw.read_bytes(), dtype="<i2") / 32768
    loud = np.where(abs(voice) > 0.01)[0]
    voice = voice[max(0, loud[0] - 400):loud[-1] + 400]
    peak = max(np.sqrt((voice[i:i + FRAME] ** 2).mean()) for i in range(0, len(voice) - FRAME, FRAME))
    for level, room in LEVELS:
        mix = np.convolve(rng.normal(size=RATE * 8), np.ones(6) / 6, "same")
        mix *= room / mix.std()
        mix[RATE * 2:RATE * 2 + len(voice)] += voice * level / peak
        name = f"{raw.stem}_{round(level * 1000)}_{round(room * 1000)}.pcm16"
        (out / name).write_bytes((np.clip(mix, -1, 1) * 32767).astype("<i2").tobytes())
