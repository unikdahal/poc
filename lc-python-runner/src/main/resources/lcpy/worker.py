"""
lc_python_runner worker: runs registered Python functions over Arrow data, inside the sandbox.

Protocol (stdin -> worker -> stdout), all on inherited pipes, no sockets:
  stdin : int32 big-endian header length, JSON header, then ONE Arrow IPC stream of input batches
  stdout: ONE Arrow IPC stream of results, columns r0..rN-1 (one struct per call)
  stderr: free-form diagnostics (user prints are redirected here too); the JVM keeps a tail

Row mode (1-to-1): exactly one output batch per input batch, same number of rows, same order.
Grouped mode (N-to-1): the input carries a group-id column; rows of a group are contiguous.
  One output row per group, in group order. Global mode: exactly one row, even for no input.

Flushing: output is flushed only when input has no data ready (i.e. right before we would block
reading), plus at the end. The JVM flushes the same way, so neither side waits on the other's
buffer, and no timer-driven flushes are needed.

Exit codes: 0 ok, 3 user function error, 4 setup/protocol error.
"""
import base64
import hashlib
import importlib.util
import io
import json
import os
import select
import struct
import sys
import traceback

# --- take the protocol fds before any user code can touch them -----------------------------------
_IN_FD = os.dup(0)    # dup() fds are non-inheritable: subprocesses never see the protocol
_OUT_FD = os.dup(1)
_null = os.open(os.devnull, os.O_RDONLY)
os.dup2(_null, 0)
os.dup2(2, 1)         # print() in user code goes to stderr, never into the Arrow stream
os.close(_null)
sys.stdout = io.TextIOWrapper(os.fdopen(1, "wb", buffering=0), write_through=True)
sys.stderr = io.TextIOWrapper(os.fdopen(2, "wb", buffering=0), write_through=True)

EXIT_USER = 3
EXIT_SETUP = 4


def _log(msg):
    sys.stderr.write("[lcpy-worker] " + msg + "\n")


def _die(code, msg):
    _log(msg)
    try:
        sys.stderr.flush()
    finally:
        os._exit(code)


try:
    import pyarrow as pa
    import pyarrow.compute as pc  # noqa: F401
except Exception:  # pragma: no cover
    _die(EXIT_SETUP, "pyarrow is not importable in the sandbox:\n" + traceback.format_exc())

REGISTRY = os.environ.get("LCPY_REGISTRY", "/lcpy/registry")
FLUSH_RESULTS = 1024  # grouped mode: emit a batch at the latest after this many groups


def module_sha256(path):
    """Same algorithm as Registry.moduleSha256 on the JVM side."""
    files = []
    for root, dirs, names in os.walk(path):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for n in names:
            full = os.path.join(root, n)
            if os.path.isfile(full):
                files.append(os.path.relpath(full, path).replace(os.sep, "/"))
    h = hashlib.sha256()
    for rel in sorted(files):
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        with open(os.path.join(path, rel), "rb") as f:
            h.update(f.read())
        h.update(b"\0")
    return h.hexdigest()


_loaded = {}


def load_function(module, function, sha):
    mdir = os.path.join(REGISTRY, module)
    if not os.path.isdir(mdir):
        _die(EXIT_SETUP, "module %r not found in registry %s" % (module, REGISTRY))
    if module not in _loaded:
        actual = module_sha256(mdir)
        if actual != sha:
            _die(EXIT_SETUP, "module %r changed since the query was planned (sha256 %s, planned %s)"
                 % (module, actual, sha))
        src = os.path.join(mdir, module + ".py")
        if not os.path.isfile(src):
            src = os.path.join(mdir, "__init__.py")
        if mdir not in sys.path:
            sys.path.insert(0, mdir)
        spec = importlib.util.spec_from_file_location("lcpy_user_" + module, src)
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except BaseException:
            _die(EXIT_USER, "importing module %r failed:\n%s" % (module, traceback.format_exc()))
        _loaded[module] = mod
    fn = getattr(_loaded[module], function, None)
    if not callable(fn):
        _die(EXIT_SETUP, "%s.%s is not a callable in the module" % (module, function))
    return fn


def to_struct(result, typ, n, what):
    """Normalize whatever the user returned into a StructArray of `typ` with `n` rows."""
    try:
        import pandas as pd  # optional
    except Exception:  # pragma: no cover
        pd = None
    if pd is not None and isinstance(result, pd.DataFrame):
        result = pa.Table.from_pandas(result, preserve_index=False)
    if isinstance(result, pa.ChunkedArray):
        result = result.combine_chunks()
    if isinstance(result, (pa.Table, pa.RecordBatch)):
        if isinstance(result, pa.Table):
            result = result.combine_chunks()
        cols = []
        for f in typ:
            if f.name not in result.schema.names:
                raise TypeError("%s: result is missing field %r (has %s)"
                                % (what, f.name, result.schema.names))
            c = result.column(f.name)
            if isinstance(c, pa.ChunkedArray):
                c = c.combine_chunks()
            cols.append(c.cast(f.type))
        arr = pa.StructArray.from_arrays(cols, fields=list(typ)) if len(typ) else None
    elif isinstance(result, pa.StructArray):
        cols = []
        for f in typ:
            idx = result.type.get_field_index(f.name)
            if idx < 0:
                raise TypeError("%s: result is missing field %r" % (what, f.name))
            cols.append(result.field(idx).cast(f.type))
        mask = result.is_null() if result.null_count else None
        arr = pa.StructArray.from_arrays(cols, fields=list(typ), mask=mask)
    elif result is None or isinstance(result, dict):
        arr = pa.array([result], type=typ)
    elif isinstance(result, (list, tuple)):
        arr = pa.array(list(result), type=typ)
    else:
        raise TypeError("%s: unsupported return type %s; return a pyarrow RecordBatch/Table/"
                        "StructArray, a pandas DataFrame, a dict or a list of dicts"
                        % (what, type(result).__name__))
    if len(arr) != n:
        raise ValueError("%s: returned %d rows, expected %d" % (what, len(arr), n))
    return arr


def main():
    inp = os.fdopen(_IN_FD, "rb", buffering=1 << 16)
    out = os.fdopen(_OUT_FD, "wb", buffering=1 << 20)

    raw = inp.read(4)
    if len(raw) != 4:
        _die(EXIT_SETUP, "no header")
    (hlen,) = struct.unpack(">i", raw)
    header = json.loads(inp.read(hlen).decode("utf-8"))
    mode = header["mode"]
    is_global = bool(header["global"])
    gid_col = int(header["gid_column"])
    out_schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(header["output_schema"])))
    calls = []
    for i, c in enumerate(header["calls"]):
        fn = load_function(c["module"], c["function"], c["sha256"])
        calls.append((fn, c["columns"], c["names"], out_schema.field(i).type,
                      "%s.%s" % (c["module"], c["function"])))

    reader = pa.ipc.open_stream(inp)
    writer = pa.ipc.new_stream(out, out_schema)

    def input_ready():
        # Only the kernel pipe is checked; bytes already in our read buffer are not seen, which
        # can only cause an extra (harmless) flush, never a missing one.
        r, _, _ = select.select([_IN_FD], [], [], 0)
        return bool(r)

    def call_user(fn, data, typ, n, what):
        try:
            res = fn(data)
            return to_struct(res, typ, n, what)
        except BaseException:
            _die(EXIT_USER, "%s raised:\n%s" % (what, traceback.format_exc()))

    def project(batch_or_table, cols, names):
        arrays = [batch_or_table.column(i) for i in cols]
        if isinstance(batch_or_table, pa.RecordBatch):
            return pa.RecordBatch.from_arrays(arrays, names=names)
        return pa.Table.from_arrays(arrays, names=names)

    if mode == "row":
        while True:
            if not input_ready():
                out.flush()
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            n = batch.num_rows
            arrays = [call_user(fn, project(batch, cols, names), typ, n, what)
                      for (fn, cols, names, typ, what) in calls]
            writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=out_schema))
    elif mode == "grouped":
        in_schema = reader.schema
        arg_idx = [i for i in range(len(in_schema)) if i != gid_col]
        pending = [[] for _ in calls]
        state = {"gid": None, "parts": []}

        def emit_pending():
            if pending[0]:
                arrays = [pa.concat_arrays(p) for p in pending]
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=out_schema))
                for p in pending:
                    p.clear()

        def finish_group():
            parts = state["parts"]
            if parts:
                table = pa.Table.from_batches(parts)
            else:
                table = in_schema.empty_table()
            table = table.select(arg_idx)
            for k, (fn, cols, names, typ, what) in enumerate(calls):
                pending[k].append(call_user(fn, project(table, cols, names), typ, 1, what))
            state["parts"] = []
            if len(pending[0]) >= FLUSH_RESULTS:
                emit_pending()

        while True:
            if not input_ready():
                emit_pending()
                out.flush()
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            if batch.num_rows == 0:
                continue
            gids = batch.column(gid_col).to_pylist()
            start = 0
            n = len(gids)
            while start < n:
                g = gids[start]
                end = start
                while end < n and gids[end] == g:
                    end += 1
                if state["gid"] is not None and g != state["gid"]:
                    finish_group()
                state["gid"] = g
                state["parts"].append(batch.slice(start, end - start))
                start = end
        if state["gid"] is not None or is_global:
            finish_group()
        emit_pending()
    else:
        _die(EXIT_SETUP, "unknown mode %r" % mode)

    writer.close()
    out.flush()
    out.close()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException:
        _die(EXIT_SETUP, "worker protocol error:\n" + traceback.format_exc())
    sys.stderr.flush()
    os._exit(0)
