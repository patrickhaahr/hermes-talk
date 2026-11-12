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

import shutil
import subprocess

BINARY = "openhome"
TIMEOUT_S = 8.0
MAX_REPEAT = 10

#: Spoken device name → the CLI words before the command.
DEVICES = {"lights": ("lights",), "tv": ("ir", "lgtv"), "speaker": ("ir", "edifier")}

TOOL: dict = {
    "type": "function",
    "name": "home_control",
    "description": (
        "Switch the operator's lights or press a button on the TV or speaker "
        "remote. The operator has pre-approved every call: the moment you are "
        "asked, call this. Do not confirm, do not announce it, and do not "
        "delegate it. Afterwards say at most a word or two, like 'Done.' "
        "lights: on or off. tv: power (it toggles, so it serves both on and "
        "off), volume-up, volume-down, mute, input, hdmi-1, hdmi-2, hdmi-3, "
        "home, back, ok, up, down, left, right, channel-up, channel-down. "
        "speaker: power, volume-up, volume-down, mute, bluetooth, optical."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "device": {"type": "string", "enum": sorted(DEVICES)},
            "command": {
                "type": "string",
                "description": "on or off for the lights, otherwise the remote button.",
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


def available() -> bool:
    return shutil.which(BINARY) is not None


def argv(device: str, command: str) -> list[str] | None:
    """The CLI call for one press, or None when the request names no real device."""

    prefix = DEVICES.get(device)
    command = command.strip().lower()
    if prefix is None or not command or command.startswith("-"):
        return None
    return [BINARY, *prefix, command]


def run(arguments: dict, *, runner=subprocess.run) -> str:
    device = str(arguments.get("device") or "").strip().lower()
    command = str(arguments.get("command") or "")
    call = argv(device, command)
    if call is None:
        return f"home_control: unknown device or command ({device!r} {command!r})."
    try:
        repeat = max(1, min(int(arguments.get("repeat") or 1), MAX_REPEAT))
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
                f"{device} {call[-1]} failed: {reason[-1] if reason else f'exit {done.returncode}'}"
            )
    return f"done: {device} {call[-1]}" + (f" x{repeat}" if repeat > 1 else "")
