#!/usr/bin/env python3
"""Loopback-only classic-MySQL COMMIT fault injector, not a general SQL proxy."""

import argparse
import asyncio
import json
import signal
import struct
import time


async def packet(reader):
    header = await reader.readexactly(4)
    length = struct.unpack("<I", header)[0] & 0xFFFFFF
    if length >= 1024 * 1024:
        raise ValueError("Test proxy supports only small, unfragmented MySQL packets")
    payload = await reader.readexactly(length)
    return header + payload, header[3], payload


class Proxy:
    def __init__(self, log):
        self.log = log
        self.events = []
        self.armed = None
        self.held = {}
        self.next_connection = 0

    def event(self, name, **details):
        item = {"event": name, "monotonic": time.monotonic(), **details}
        self.events.append(item)
        self.log.write(json.dumps(item) + "\n")
        self.log.flush()

    async def connection(self, reader, writer):
        self.next_connection += 1
        connection = self.next_connection
        upstream = None
        tasks = []
        state = {"commit_fault": None, "case": None}
        try:
            remote_reader, upstream = await asyncio.open_connection("127.0.0.1", 3306)

            async def client_to_mysql():
                first = True
                while True:
                    raw, sequence, payload = await packet(reader)
                    if first:
                        first = False
                        flags = struct.unpack_from("<I", payload)[0]
                        if flags & (0x800 | 0x08000000):
                            raise ValueError("TLS and query attributes are unsupported by this test proxy")
                    if sequence == 0 and payload[:1] == b"\x03":
                        query = payload[1:].strip().rstrip(b";").upper()
                        if query == b"COMMIT" and self.armed:
                            state.update(commit_fault=self.armed["mode"], case=self.armed["case"])
                            self.armed = None
                            self.event("commit_intercepted", connection=connection, **state)
                            if state["commit_fault"] == "before_commit":
                                self.event("commit_not_forwarded", connection=connection, **state)
                                upstream.close()
                                writer.close()
                                return
                    upstream.write(raw)
                    await upstream.drain()

            async def mysql_to_client():
                while True:
                    raw, sequence, payload = await packet(remote_reader)
                    if state["commit_fault"] and sequence == 1:
                        if payload[:1] != b"\x00":
                            raise ValueError("COMMIT did not return a MySQL OK packet")
                        self.event("mysql_commit_ok_seen", connection=connection, **state)
                        if state["commit_fault"] == "drop_commit_ack":
                            self.event("commit_ok_dropped", connection=connection, **state)
                            writer.close()
                            upstream.close()
                            return
                        release = asyncio.Event()
                        self.held[connection] = release
                        self.event("commit_ok_held", connection=connection, **state)
                        try:
                            await release.wait()
                        finally:
                            self.held.pop(connection, None)
                        self.event("commit_ok_released", connection=connection, **state)
                        state["commit_fault"] = None
                    writer.write(raw)
                    await writer.drain()

            tasks = [asyncio.create_task(client_to_mysql()), asyncio.create_task(mysql_to_client())]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                task.result()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        except Exception as exc:
            self.event("proxy_error", connection=connection, message=str(exc))
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.held.pop(connection, None)
            if upstream:
                upstream.close()
            writer.close()

    async def control(self, reader, writer):
        try:
            request = json.loads(await asyncio.wait_for(reader.readline(), timeout=2))
            command = request["command"]
            if command == "arm":
                if self.armed or self.held:
                    raise ValueError("Previous fault still armed or held")
                if request["mode"] not in ("before_commit", "drop_commit_ack", "hold_commit_ack"):
                    raise ValueError("Unsupported fault")
                self.events = []
                self.armed = {"mode": request["mode"], "case": request["case"]}
            elif command == "release":
                for release in list(self.held.values()):
                    release.set()
            elif command != "status":
                raise ValueError("Unsupported command")
            response = {"ok": True, "armed": self.armed, "held": len(self.held), "events": self.events}
        except Exception as exc:
            response = {"ok": False, "error": str(exc)}
        writer.write((json.dumps(response) + "\n").encode())
        await writer.drain()
        writer.close()


async def run(args):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    with open(args.events, "a", encoding="utf-8") as log:
        proxy = Proxy(log)
        async with await asyncio.start_server(proxy.connection, "127.0.0.1", 13307), \
                await asyncio.start_server(proxy.control, "127.0.0.1", 13308):
            print("READY mysql=127.0.0.1:13307 control=127.0.0.1:13308", flush=True)
            await stop.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True)
    asyncio.run(run(parser.parse_args()))
