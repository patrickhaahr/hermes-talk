"""Home control — the one live action a voice call runs itself, with no agent in between.

Switching a light or pressing a remote button is instant and undone by the
next command, so it is the case the DAMAGE rule calls reversible. Before this
existed, "turn off the lights" became a spoken plan, an approval, and a
background agent run that took 20 to 85 seconds. Here it is one ``openhome``
CLI call that finishes in well under a second.

The tool is advertised only where the ``openhome`` binary is on PATH, the same
rule as ``search_vault``: a tool that cannot run is never offered.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time

BINARY = "openhome"
TIMEOUT_S = 8.0
MAX_REPEAT = 10
POWER_REPEAT_S = 6.0

#: Spoken device name → the CLI words before the command.
DEVICES = {
    "lights": ("lights",),
    "backlight": ("backlight",),
    "tv": ("ir", "lgtv"),
    "speaker": ("ir", "edifier"),
}
_BACKLIGHT_SINGLE = frozenset({"red", "tv", "off", "automatic"})
_BACKLIGHT_BRIGHTNESS = frozenset({"red", "tv", "color"})

#: What each device takes, for the model: read by the tool here and by GPT-Live's guide.
COMMANDS = (
    "lights: on or off. "
    "backlight (TV backlight LEDs): red, tv (TV Sync, follows the screen), off, automatic "
    "(reset to normal/automatic), or 'color <6-digit hex>' (e.g. 'color 0080ff' blue, "
    "'color 00ff00' green, 'color ff00ff' purple, 'color ffa500' orange, 'color ffffff' white); "
    "pass optional brightness (0-100) when asked. "
    "tv: power (it toggles, so it serves both on and off), volume-up, volume-down, mute, "
    "input, hdmi-1, hdmi-2, hdmi-3, home, back, ok, up, down, left, right, channel-up, "
    "channel-down, 1. "
    "speaker: power, volume-up, volume-down, mute, bluetooth, optical (the TV's sound). "
    "Danish works the same: 'tænd/sluk lys' or 'lyset' is lights on/off, 'rødt lys' or "
    "'rød LED'/'rød baggrundsbelysning' is backlight red, 'blåt/grønt/lilla/orange/hvidt/varmt lys' "
    "is backlight 'color <hex>', 'tv sync'/'følg tv'/'normalt lys'/'automatisk lys' is backlight "
    "tv or automatic, 'sluk baggrundslys'/'sluk tv-lys'/'sluk LED' is backlight off, "
    "'tænd/sluk TV' is tv power, 'skru op/ned' is volume-up/down, 'skift højtaler til TV' or "
    "'til optical' is speaker optical, and 'skift højtaler til bluetooth' is speaker bluetooth. "
    "One word is enough: 'YouTube' is tv 1, 'play' is tv ok, 'tilbage' is tv back (never power), "
    "'TV' is tv power, and 'mute', 'bluetooth' and 'optical' are the speaker."
)

DONE_PREFIX = "done: "
_power_pressed: dict[str, float] = {}

TOOL: dict = {
    "type": "function",
    "name": "home_control",
    "description": (
        "Switch the operator's lights, set the TV backlight LEDs, or press a button on the "
        "TV or speaker remote. The operator has pre-approved every call: the moment you are "
        "asked, call this. Do not confirm, do not announce it, and do not delegate it. Say "
        "nothing before it, and stay silent when it succeeds. If it fails, tell the operator "
        "in one short English sentence. "
        + COMMANDS
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "device": {"type": "string", "enum": sorted(DEVICES)},
            "command": {
                "type": "string",
                "description": (
                    "on/off for lights; red, tv, off, automatic, or 'color <hex>' for backlight; "
                    "remote button for tv/speaker."
                ),
            },
            "brightness": {
                "type": "integer",
                "minimum": 0,
                "maximum": 100,
                "description": "Optional brightness (0-100) for backlight red, tv, or color.",
            },
            "repeat": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_REPEAT,
                "description": "How many presses, for volume steps. Default 1.",
            },
        },
        "required": ["device", "command"],
        "additionalProperties": False,
    },
}


def worked(output: str) -> bool:
    """Whether a :func:`run` result reports every press as done."""

    return output.startswith(DONE_PREFIX)


def available() -> bool:
    return shutil.which(BINARY) is not None


def argv(device: str, command: str, *, brightness: int | None = None) -> list[str] | None:
    """The CLI call for one press, or None when the request names no real device."""

    prefix = DEVICES.get(device)
    command = command.strip().lower()
    if prefix is None or not command or command.startswith("-"):
        return None
    if device == "backlight":
        parts = command.split()
        if len(parts) == 1 and parts[0] in _BACKLIGHT_SINGLE:
            args = parts
        elif len(parts) == 2 and parts[0] == "color" and not parts[1].startswith("-"):
            args = parts
        else:
            return None
        call = [BINARY, *prefix, *args]
        if brightness is not None and parts[0] in _BACKLIGHT_BRIGHTNESS:
            try:
                level = int(brightness)
            except (TypeError, ValueError):
                return None
            if not 0 <= level <= 100:
                return None
            call += ["--brightness", str(level)]
        return call
    return [BINARY, *prefix, command]


def _backlight_failed(stdout: str) -> bool:
    try:
        payload = json.loads(stdout)
    except ValueError:
        return False
    return isinstance(payload, dict) and payload.get("command") == "failed"


def run(arguments: dict, *, runner=subprocess.run, clock=time.monotonic) -> str:
    device = str(arguments.get("device") or "").strip().lower()
    command = str(arguments.get("command") or "")
    call = argv(device, command, brightness=arguments.get("brightness"))
    if call is None:
        return f"home_control: unknown device or command ({device!r} {command!r})."
    label = " ".join(call[1 + len(DEVICES[device]) :])
    power = device != "backlight" and call[-1] == "power"
    if power and clock() - _power_pressed.get(device, float("-inf")) < POWER_REPEAT_S:
        return f"{DONE_PREFIX}{device} power, already pressed"
    try:
        repeat = (
            1
            if power or device == "backlight"
            else max(1, min(int(arguments.get("repeat") or 1), MAX_REPEAT))
        )
    except (TypeError, ValueError):
        repeat = 1
    for _ in range(repeat):
        try:
            done = runner(call, capture_output=True, text=True, timeout=TIMEOUT_S, check=False)
        except subprocess.TimeoutExpired:
            return f"{device} did not answer within {TIMEOUT_S:.0f} seconds; nothing confirmed."
        if done.returncode != 0:
            reason = (done.stderr or done.stdout).strip().splitlines()
            return (
                f"{device} {label} failed: {reason[-1] if reason else f'exit {done.returncode}'}"
            )
        if device == "backlight" and _backlight_failed(done.stdout):
            return f"{device} {label} failed: backlight device did not accept the command"
    if power:
        _power_pressed[device] = clock()
    return f"{DONE_PREFIX}{device} {label}" + (f" x{repeat}" if repeat > 1 else "")
