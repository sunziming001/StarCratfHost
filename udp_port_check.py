#!/usr/bin/env python3
import argparse
import socket
import sys
import time


def check_udp(host, port, message, timeout, count):
    addr = (host, port)
    received = 0
    refused = 0
    timed_out = 0

    for i in range(1, count + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)

            try:
                # UDP connect only records the default peer. It does not prove
                # the remote service is listening until we send and wait.
                sock.connect(addr)
                start = time.monotonic()
                sock.send(message)

                try:
                    data = sock.recv(4096)
                    elapsed_ms = (time.monotonic() - start) * 1000
                    received += 1
                    print(
                        f"[{i}] reply from {host}:{port}, "
                        f"{len(data)} bytes, {elapsed_ms:.1f} ms"
                    )
                except socket.timeout:
                    timed_out += 1
                    print(f"[{i}] no UDP reply within {timeout:.1f}s")
                except ConnectionRefusedError as exc:
                    refused += 1
                    print(f"[{i}] ICMP port unreachable / refused: {exc}")
                except OSError as exc:
                    refused += 1
                    print(f"[{i}] socket error after send: {exc}")

            except socket.gaierror as exc:
                print(f"DNS lookup failed for {host!r}: {exc}", file=sys.stderr)
                return 2
            except OSError as exc:
                print(f"send failed: {exc}", file=sys.stderr)
                return 2

    print()
    if received:
        print("Result: reachable, remote UDP service replied.")
        return 0
    if refused:
        print("Result: likely closed, host reported UDP port unreachable/refused.")
        return 1

    print("Result: unknown. UDP got no reply; the port may be open, filtered, or silent.")
    return 1


def main():
    parser = argparse.ArgumentParser(
        description="Send UDP probes and report whether the remote port replies."
    )
    parser.add_argument("host", help="remote host or IP")
    parser.add_argument("port", type=int, help="remote UDP port")
    parser.add_argument(
        "-m",
        "--message",
        default="ping",
        help="payload to send, default: ping",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=2.0,
        help="seconds to wait for each reply, default: 2",
    )
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=1,
        help="number of probes to send, default: 1",
    )
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        print("port must be between 1 and 65535", file=sys.stderr)
        return 2
    if args.count < 1:
        print("count must be >= 1", file=sys.stderr)
        return 2

    return check_udp(
        args.host,
        args.port,
        args.message.encode("utf-8"),
        args.timeout,
        args.count,
    )


if __name__ == "__main__":
    raise SystemExit(main())
