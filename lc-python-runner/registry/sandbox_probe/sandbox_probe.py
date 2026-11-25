"""Adversarial functions used by the sandbox e2e suite. Each returns what it could observe."""
import ctypes
import errno
import json
import os
import resource
import socket
import subprocess
import time

import pyarrow as pa


def _one(batch, **row):
    return [row] * batch.num_rows


def _try(fn):
    try:
        return "ok:" + str(fn())[:200]
    except Exception as e:  # noqa: BLE001
        return "denied:%s:%s" % (type(e).__name__, getattr(e, "errno", ""))


def fs(batch):
    """arg0 = a host path that must NOT be visible (e.g. the Spark local dir)."""
    host_path = batch.column(0)[0].as_py()
    r = {
        "uid": os.getuid(), "pid": os.getpid(), "hostname": socket.gethostname(),
        "root_listing": sorted(os.listdir("/")),
        "env": dict(os.environ),
        "etc_shadow": _try(lambda: open("/etc/shadow").read()),
        "etc_listing": sorted(os.listdir("/etc")),
        "k8s_token": _try(lambda: open("/var/run/secrets/kubernetes.io/serviceaccount/token").read()),
        "host_path": _try(lambda: os.listdir(host_path)),
        "opt_spark": _try(lambda: os.listdir("/opt")),
        "sys": _try(lambda: os.listdir("/sys/fs/cgroup")),
        "write_usr": _try(lambda: open("/usr/lcpy-escape", "w").write("x")),
        "write_registry": _try(lambda: open("/lcpy/registry/escape", "w").write("x")),
        "write_tmp": _try(lambda: open("/tmp/ok", "w").write("x")),
        "proc_pids": sorted(int(p) for p in os.listdir("/proc") if p.isdigit()),
        "proc1_cmd": _try(lambda: open("/proc/1/cmdline", "rb").read().replace(b"\0", b" ").decode()),
    }
    return _one(batch, report=json.dumps(r))


def connect(batch):
    """args: host string, port int -> how a TCP connect ended."""
    out = []
    for host, port in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()):
        s = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        t = time.time()
        try:
            s.connect((host, port))
            res = "connected"
        except socket.timeout:
            res = "timeout"
        except OSError as e:
            res = "error:%s" % errno.errorcode.get(e.errno, e.errno)
        finally:
            s.close()
        out.append({"result": "%s (%.2fs)" % (res, time.time() - t)})
    return out


def fork_bomb(batch):
    n = 0
    try:
        for _ in range(10000):
            pid = os.fork()
            if pid == 0:
                time.sleep(60)
                os._exit(0)
            n += 1
    except OSError:
        pass
    return _one(batch, forks=n)


def mem_hog(batch):
    print("before-oom-marker: allocating until the cgroup limit", flush=True)
    hog = []
    while True:
        hog.append(bytearray(16 * 1024 * 1024))
        for i in range(0, len(hog[-1]), 4096):
            hog[-1][i] = 1


def orphans(batch):
    """Leave processes behind on purpose: a detached session leader and a double-forked daemon."""
    p = subprocess.Popen(["sleep", "301"], start_new_session=True)
    if os.fork() == 0:
        os.setsid()
        if os.fork() == 0:
            os.execvp("sleep", ["sleep", "302"])
        os._exit(0)
    return _one(batch, pids=str(p.pid))


def cpu_burn(batch):
    t0, c0 = time.time(), resource.getrusage(resource.RUSAGE_SELF).ru_utime
    while time.time() - t0 < 3.0:
        sum(i * i for i in range(10000))
    return _one(batch, cpu=resource.getrusage(resource.RUSAGE_SELF).ru_utime - c0,
                wall=time.time() - t0)


def tmp_marker(batch):
    seen = os.path.exists("/tmp/lcpy-marker")
    open("/tmp/lcpy-marker", "w").write("x")
    return _one(batch, seen_previous=seen)


def syscalls(batch):
    libc = ctypes.CDLL(None, use_errno=True)

    def unshare_user():
        if libc.unshare(0x10000000) != 0:
            raise OSError(ctypes.get_errno(), "unshare")
        return "unshared"

    def mount_tmpfs():
        if libc.mount(b"none", b"/tmp", b"tmpfs", 0, None) != 0:
            raise OSError(ctypes.get_errno(), "mount")
        return "mounted"

    r = {
        "af_unix": _try(lambda: socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)),
        "abstract_connect": _try(lambda: socket.socket(socket.AF_UNIX).connect("\0lcpy")),
        "netlink": _try(lambda: socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 0)),
        "socketpair": _try(lambda: socket.socketpair()),
        "unshare_user": _try(unshare_user),
        "mount": _try(mount_tmpfs),
        "ptrace_attach_1": _try(lambda: libc.ptrace(16, 1, 0, 0) or ctypes.get_errno()),
        "raw_socket": _try(lambda: socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)),
        "setuid0": _try(lambda: os.setuid(0)),
    }
    return _one(batch, report=json.dumps(r))


def sleepy(batch):
    time.sleep(float(os.environ.get("LCPY_SLEEP", "4")))
    return pa.RecordBatch.from_arrays([batch.column(0).cast(pa.int64())], names=["x"])
