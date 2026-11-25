#!/usr/bin/env python3
"""Generate the worker's seccomp filter (BPF) with libseccomp. Default ALLOW plus a deny-list:
the worker is ordinary Python + pyarrow; it needs no kernel attack surface beyond that.

Denied with EPERM:
  - socket() for every family except AF_INET/AF_INET6: no AF_UNIX (abstract sockets are per
    network namespace, which the worker shares with the pod), netlink, packet, vsock, ...
    (socketpair() stays allowed: multiprocessing uses it, and it cannot reach anything.)
  - namespace / mount / module / kexec / keyring / bpf / perf / ptrace / userfaultfd / io_uring
  - clone() with any CLONE_NEW* flag; clone3() returns ENOSYS so libc falls back to clone()
"""
import errno
import sys

import seccomp

ALLOWED_FAMILIES = {2, 10}  # AF_INET, AF_INET6
DENY = [
    "ptrace", "process_vm_readv", "process_vm_writev", "kcmp",
    "keyctl", "add_key", "request_key",
    "bpf", "perf_event_open", "userfaultfd",
    "io_uring_setup", "io_uring_enter", "io_uring_register",
    "mount", "umount2", "pivot_root", "chroot", "move_mount", "open_tree", "fsopen", "fsmount",
    "fsconfig", "fspick", "mount_setattr",
    "setns", "unshare",
    "kexec_load", "kexec_file_load", "init_module", "finit_module", "delete_module",
    "reboot", "swapon", "swapoff", "acct", "quotactl", "syslog",
    "open_by_handle_at", "name_to_handle_at", "fanotify_init", "lookup_dcookie",
    "iopl", "ioperm", "settimeofday", "clock_settime", "adjtimex", "clock_adjtime",
]
CLONE_NEW = {
    "CLONE_NEWNS": 0x00020000, "CLONE_NEWCGROUP": 0x02000000, "CLONE_NEWUTS": 0x04000000,
    "CLONE_NEWIPC": 0x08000000, "CLONE_NEWUSER": 0x10000000, "CLONE_NEWPID": 0x20000000,
    "CLONE_NEWNET": 0x40000000,
}


def main(out):
    f = seccomp.SyscallFilter(defaction=seccomp.ALLOW)
    eperm = seccomp.ERRNO(errno.EPERM)
    for name in DENY:
        try:
            f.add_rule(eperm, name)
        except (RuntimeError, ValueError):
            print("seccomp: syscall %s unknown on this arch, skipped" % name, file=sys.stderr)
    # socket(domain, ...): deny every family except the allowed ones
    for fam in range(0, 64):
        if fam not in ALLOWED_FAMILIES:
            f.add_rule(seccomp.ERRNO(errno.EAFNOSUPPORT), "socket", seccomp.Arg(0, seccomp.EQ, fam))
    for flag in CLONE_NEW.values():
        f.add_rule(eperm, "clone", seccomp.Arg(0, seccomp.MASKED_EQ, flag, flag))
    f.add_rule(seccomp.ERRNO(errno.ENOSYS), "clone3")
    with open(out, "wb") as fh:
        f.export_bpf(fh)
    print("seccomp filter written to %s" % out, file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1])
