"""The link to an Echo that loses power, which closes no connection."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

VANISHING_ECHO = """
import asyncio, subprocess
import talk_echo

talk_echo.LINK_PROBE_IDLE_S = talk_echo.LINK_PROBE_EVERY_S = 1
talk_echo.LINK_DEAD_AFTER_S = 2

async def echo(reader, writer):
    while await reader.read(1024):
        pass

async def session(**_):
    await asyncio.Event().wait()

async def go():
    ended = asyncio.Event()
    linked = talk_echo._linked

    async def observed(*args):
        try:
            await linked(*args)
        finally:
            ended.set()

    talk_echo._linked = observed
    server = await asyncio.start_server(echo, "127.0.0.1", 16700)
    surface = asyncio.create_task(
        talk_echo._stay_linked("127.0.0.1", 16700, session, 20.0, 0.02, None)
    )
    await asyncio.sleep(0.5)
    assert not ended.is_set(), "the link ended before the Echo vanished"
    subprocess.run(["ip", "link", "set", "lo", "down"], check=True)
    await asyncio.wait_for(ended.wait(), 8)
    print("link given up")

asyncio.run(go())
"""


def _isolated_network_available() -> bool:
    if not (shutil.which("unshare") and shutil.which("ip")):
        return False
    return subprocess.run(["unshare", "-rn", "true"], capture_output=True).returncode == 0


@pytest.mark.skipif(
    not _isolated_network_available(), reason="needs an unprivileged network namespace"
)
def test_an_echo_that_loses_power_is_noticed_without_any_traffic():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            "unshare", "-rn", "sh", "-c",
            'ip link set lo up && exec "$0" -c "$1"', sys.executable, VANISHING_ECHO,
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert "link given up" in result.stdout, result.stderr
