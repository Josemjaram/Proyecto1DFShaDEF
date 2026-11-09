"""Cliente DFSha (biblioteca). El CLI (dfsha/cli.py) es una capa fina sobre esta clase.

El cliente SOLO conoce las URL de los controladores (semilla/DNS). Las direcciones de los nodos de
datos las descubre en cada operación a través del manifiesto (RNF7). Los datos viajan directo
cliente <-> nodos de datos; el controlador solo maneja metadatos.
"""
import bisect
import hashlib
import os
import posixpath
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import requests

from .common import DFSError, http, norm, sha256

ALLOC_BATCH = 16


class DFSClient:
    def __init__(self, controllers, token=None, user=None, ca=None, session=None, parallel=4, cwd="/"):
        self.controllers = [c.rstrip("/") for c in controllers]
        self.token, self.user, self.cwd = token, user, cwd
        self.session = session or uuid.uuid4().hex[:10]  # identifica a este cliente como "dueño" de sus locks
        self.parallel = parallel
        self.idx = 0
        self.locks = {}
        if ca:
            os.environ["DFSHA_CA"] = ca

    # ------------------------------------------------------------------ transporte con descubrimiento de líder
    def _ctl(self, method, path, body=None, params=None, retry=20.0, auth=True):
        headers = {"X-Session": self.session}
        if auth and self.token:
            headers["Authorization"] = "Bearer " + self.token
        deadline, last = time.time() + retry, None
        while True:
            n = len(self.controllers)
            transient = False
            for k in range(n):
                i = (self.idx + k) % n
                try:
                    r = http().request(method, self.controllers[i] + path, json=body, params=params,
                                       headers=headers, timeout=(3, 20))
                except requests.RequestException as e:
                    last = DFSError(503, "UNAVAILABLE", str(e)[:120])
                    continue
                if r.status_code == 503:
                    j = _json(r)
                    if j.get("error") == "NOT_LEADER":
                        hint = (j.get("message") or "").rstrip("/")
                        if hint in self.controllers:
                            self.idx = self.controllers.index(hint)
                        last = DFSError(503, "NOT_LEADER", "sin líder todavía")
                        continue
                    if j.get("error") in ("NO_QUORUM", "INSUFFICIENT_NODES"):
                        last, transient = DFSError(503, j["error"], j.get("message", "")), True
                        break
                self.idx = i
                return _parse(r)
            if time.time() > deadline:
                raise last or DFSError(503, "UNAVAILABLE", "ningún controlador respondió")
            time.sleep(0.4 if not transient else 1.0)

    # ------------------------------------------------------------------ cuentas
    def register(self, user, password):
        return self._ctl("POST", "/v1/register", {"user": user, "password": password}, auth=False)

    def login(self, user, password):
        r = self._ctl("POST", "/v1/login", {"user": user, "password": password}, auth=False)
        self.token, self.user = r["token"], user
        return r

    # ------------------------------------------------------------------ RF1: sistema de archivos jerárquico
    def abspath(self, p):
        return norm(posixpath.join(self.cwd, p or "."))

    def ls(self, path="."):
        return self._ctl("GET", "/v1/ls", params={"path": self.abspath(path)})["entries"]

    def stat(self, path):
        return self._ctl("GET", "/v1/stat", params={"path": self.abspath(path)})

    def cd(self, path):
        p = self.abspath(path)
        if self._ctl("GET", "/v1/stat", params={"path": p})["type"] != "dir":
            raise DFSError(409, "NOT_DIR", p)
        self.cwd = p
        return p

    def mkdir(self, path):
        return self._ctl("POST", "/v1/mkdir", {"path": self.abspath(path)})

    def rmdir(self, path):
        return self._ctl("POST", "/v1/rmdir", {"path": self.abspath(path)})

    def rm(self, path):
        return self._ctl("DELETE", "/v1/files", params={"path": self.abspath(path)})

    # ------------------------------------------------------------------ locks explícitos (lease + fencing token)
    def lock(self, path):
        p = self.abspath(path)
        r = self._ctl("POST", "/v1/locks", {"path": p}, retry=1.0)
        self.locks[p] = r["fence"]
        return r

    def unlock(self, path):
        p = self.abspath(path)
        r = self._ctl("DELETE", "/v1/locks", params={"path": p, "fence": self.locks.pop(p, 0)})
        return r

    # ------------------------------------------------------------------ manifiestos
    def manifest(self, path, version=None):
        params = {"path": self.abspath(path)}
        if version:
            params["version"] = version
        return self._ctl("GET", "/v1/files/manifest", params=params)

    def manifest_by_id(self, fid):
        return self._ctl("GET", f"/v1/files/{fid}/manifest")

    def report_corrupt(self, bid, node):
        try:
            self._ctl("POST", "/v1/blocks/corrupt", {"block_id": bid, "node": node}, retry=2.0)
        except DFSError:
            pass

    # ------------------------------------------------------------------ RF2: put / get (archivos completos)
    def put(self, local, remote, mode="replace", block_size=None, parallel=None, expected_version=None):
        t0 = time.time()
        size = os.path.getsize(local)
        f = self.open(remote, "a" if mode == "append" else ("x" if mode == "create" else "w"),
                      block_size=block_size, parallel=parallel, expected_version=expected_version)
        try:
            with open(local, "rb") as src:
                while True:
                    chunk = src.read(f.block_size)
                    if not chunk:
                        break
                    f.write(chunk)
            r = f.close()
        except BaseException:
            f.abort()
            raise
        r["seconds"], r["bytes"] = time.time() - t0, size
        return r

    def get(self, remote, local, parallel=None):
        t0 = time.time()
        rd = _Reader(self, self.manifest(remote))
        tmp = local + ".dfsha-part"
        try:
            with open(tmp, "wb") as f:
                f.truncate(rd.m["size"])
            fd = os.open(tmp, os.O_WRONLY)
            try:
                with ThreadPoolExecutor(parallel or self.parallel) as ex:
                    futs = [ex.submit(lambda b: (b, rd.fetch(b)), b) for b in rd.m["blocks"]]
                    for fu in futs:
                        b, data = fu.result()
                        os.pwrite(fd, data, b["offset"])
            finally:
                os.close(fd)
            if os.path.getsize(tmp) != rd.m["size"]:
                raise DFSError(422, "SIZE_MISMATCH", "tamaño reconstruido distinto al del manifiesto")
            want = rd.m.get("content_sha256")
            if want:
                h = hashlib.sha256()
                with open(tmp, "rb") as f:
                    for chunk in iter(lambda: f.read(1 << 20), b""):
                        h.update(chunk)
                if h.hexdigest() != want:
                    raise DFSError(422, "HASH_MISMATCH", "el SHA-256 del archivo reconstruido no coincide")
            os.replace(tmp, local)  # el destino solo aparece completo y verificado
        except BaseException:
            if os.path.exists(tmp):
                os.remove(tmp)
            raise
        return {"version": rd.m["version"], "bytes": rd.m["size"], "seconds": time.time() - t0}

    # ------------------------------------------------------------------ RF3: open/read/write/close
    def open(self, path, mode="r", **kw):
        if mode == "r":
            return DFSReadFile(self, path)
        if mode in ("w", "a", "x", "m"):
            return DFSWriteFile(self, path, mode, **kw)
        raise ValueError("modo ∈ r|w|a|x|m  (w=reemplazar completo, a=anexar, x=crear exclusivo, "
                         "m=modificar un rango por bloque; ver write_at)")

    def write_at(self, path, offset, data, **kw):
        """Sobrescribe un rango de un archivo existente (copy-on-write por bloque): solo se reenvían los bloques
        que toca el rango; el resto se comparte con la versión anterior. No extiende el archivo (use anexar)."""
        f = self.open(path, "m", **kw)
        try:
            f.modify(offset, bytes(data))
            return f.close()
        except BaseException:
            f.abort()
            raise

    def read_bytes(self, path, offset=0, length=-1):
        f = self.open(path, "r")
        f.seek(offset)
        return f.read(length)

    def write_bytes(self, path, data, mode="w", **kw):
        f = self.open(path, mode, **kw)
        try:
            f.write(data)
            return f.close()
        except BaseException:
            f.abort()
            raise


# ============================================================================== lectura
class _Reader:
    """Descarga y verifica bloques de un manifiesto; si todas las ubicaciones fallan, renueva el manifiesto."""

    def __init__(self, client, manifest):
        self.c, self.m, self.lk = client, manifest, threading.Lock()
        self.by_id = {b["id"]: b for b in manifest["blocks"]}

    def _try(self, b, token):
        last = None
        for loc in b["locations"]:
            try:
                r = http().get(f"{loc['url']}/blocks/{b['id']}", headers={"Authorization": "Bearer " + token},
                               timeout=(3, 30))
            except requests.RequestException as e:
                last = str(e)[:80]
                continue
            if r.status_code == 200:
                if len(r.content) == b["length"] and sha256(r.content) == b["sha256"]:
                    return r.content
                self.c.report_corrupt(b["id"], loc["node"])  # copia alterada: se descarta y se repara
                last = "hash"
            elif r.status_code == 422:
                self.c.report_corrupt(b["id"], loc["node"])
                last = "corrupt"
            elif r.status_code == 401:
                raise DFSError(401, "TOKEN_EXPIRED", "token de lectura vencido")
            else:
                last = f"http {r.status_code}"
        return None

    def fetch(self, b):
        data = self._try(b, self.m["read_token"])
        if data is not None:
            return data
        with self.lk:  # reintento con ubicaciones frescas (un nodo pudo caer o el token vencer)
            try:
                self.m = self.c.manifest(self.m["path"], self.m["version"])
                self.by_id = {x["id"]: x for x in self.m["blocks"]}
            except DFSError as e:
                if e.status == 401:
                    raise
        nb = self.by_id.get(b["id"], b)
        data = self._try(nb, self.m["read_token"])
        if data is None:
            raise DFSError(503, "BLOCK_UNAVAILABLE", f"ninguna réplica válida del bloque {b['index']}")
        return data


class DFSReadFile:
    """open(path,'r'): instantánea (snapshot) de la última versión confirmada."""

    def __init__(self, client, path):
        self.rd = _Reader(client, client.manifest(path))
        self.pos, self.cache = 0, OrderedDict()
        self.offs = [b["offset"] for b in self.rd.m["blocks"]]

    @property
    def version(self):
        return self.rd.m["version"]

    @property
    def size(self):
        return self.rd.m["size"]

    def seek(self, pos):
        self.pos = max(0, pos)

    def tell(self):
        return self.pos

    def _block(self, b):
        if b["id"] not in self.cache:
            self.cache[b["id"]] = self.rd.fetch(b)
            while len(self.cache) > 4:
                self.cache.popitem(last=False)
        return self.cache[b["id"]]

    def read(self, n=-1):
        size = self.size
        if self.pos >= size:
            return b""  # fin de archivo => 0 bytes
        end = size if n is None or n < 0 else min(size, self.pos + n)
        out = bytearray()
        while self.pos < end:
            b = self.rd.m["blocks"][bisect.bisect_right(self.offs, self.pos) - 1]
            data = self._block(b)
            s, e = self.pos - b["offset"], min(len(data), end - b["offset"])
            out += data[s:e]
            self.pos = b["offset"] + e
        return bytes(out)

    def close(self):
        self.cache.clear()

    __enter__ = lambda self: self
    __exit__ = lambda self, *a: self.close()


# ============================================================================== escritura
class DFSWriteFile:
    """open(path,'w'|'a'|'x'): reserva una nueva versión bajo lock exclusivo; close() publica o falla sin efectos."""

    def __init__(self, client, path, mode, block_size=None, parallel=None, expected_version=None):
        self.c, self.path, self.mode = client, client.abspath(path), mode
        body = {"path": self.path, "mode": {"w": "replace", "a": "append", "x": "create", "m": "modify"}[mode]}
        if block_size:
            body["block_size"] = block_size
        if expected_version is not None:
            body["expected_version"] = expected_version
        u = client._ctl("POST", "/v1/uploads", body, retry=5.0)
        self.u, self.id = u, u["upload_id"]
        self.block_size = u["block_size"]
        self.need = u["replication"] if u["strict"] else u["min_ack"]
        self.R = u["replication"]
        self.token = u["write_token"]
        self.buf, self.hash, self.next = bytearray(), hashlib.sha256(), 0
        par = parallel or client.parallel
        self.sem = threading.Semaphore(par)
        self.pool = ThreadPoolExecutor(par)
        self.fan = ThreadPoolExecutor(par * self.R)
        self.futs, self.targets, self.alloc_next = [], {}, 0
        self.alock, self.done, self.error = threading.Lock(), False, None
        threading.Thread(target=self._keepalive, daemon=True).start()

    # lease: se renueva mientras el upload esté abierto
    def _keepalive(self):
        while not self.done:
            time.sleep(self.u["lease"] / 3)
            if self.done:
                return
            try:
                self.c._ctl("POST", f"/v1/uploads/{self.id}/renew", retry=2.0)
            except DFSError as e:
                self.error = e
                return

    def write(self, data: bytes):
        if self.error:
            raise self.error
        self.hash.update(data)
        self.buf += data
        while len(self.buf) >= self.block_size:
            chunk = bytes(self.buf[:self.block_size])
            del self.buf[:self.block_size]
            self._submit(chunk)

    def modify(self, offset, data):
        """Modo 'm': reescribe solo los bloques de la versión base que toca [offset, offset+len(data))."""
        if self.mode != "m":
            raise ValueError("modify() solo existe en el modo 'm'")
        if self.error:
            raise self.error
        if offset < 0 or not data:
            raise DFSError(400, "BAD_RANGE", "offset >= 0 y datos no vacíos")
        # el lock ya está tomado: la versión base (u['base_version']) no puede cambiar mientras escribimos
        m = self.c.manifest(self.path, self.u["base_version"])
        end = offset + len(data)
        if end > m["size"]:
            raise DFSError(416, "RANGE_OUT_OF_FILE",
                           f"el rango llega a {end} y el archivo mide {m['size']}; use anexar para extenderlo")
        rd = _Reader(self.c, m)
        for b in m["blocks"]:
            bs, be = b["offset"], b["offset"] + b["length"]
            if be <= offset or bs >= end:
                continue
            old = bytearray(rd.fetch(b))  # lectura verificada (SHA-256) del bloque anterior
            s, e = max(offset, bs), min(end, be)
            old[s - bs:e - bs] = data[s - offset:e - offset]
            self._submit(bytes(old), idx=b["index"])

    def _submit(self, chunk, idx=None):
        if idx is None:
            idx = self.next
            self.next += 1
        self.sem.acquire()
        fut = self.pool.submit(self._send_block, idx, chunk)
        fut.add_done_callback(lambda f: self.sem.release())
        self.futs.append(fut)

    def _targets(self, idx):
        with self.alock:
            while idx >= self.alloc_next:
                r = self.c._ctl("POST", f"/v1/uploads/{self.id}/allocate",
                                {"start": self.alloc_next, "count": ALLOC_BATCH})
                self.token = r["write_token"]
                for t in r["targets"]:
                    self.targets[t["index"]] = t["nodes"]
                self.alloc_next += ALLOC_BATCH
            return list(self.targets[idx])

    def _put_one(self, node, bid, data, sha):
        r = http().put(f"{node['url']}/blocks/{bid}", data=data, timeout=(3, 30),
                       headers={"Authorization": "Bearer " + self.token, "X-Sha256": sha})
        if r.status_code != 200:
            raise DFSError(r.status_code, _json(r).get("error", "ERR"))
        return r.json()["ack"]

    def _send_block(self, idx, data):
        """Escribe el bloque en varios nodos (fan-out). Se confirma con 'need' acuses; si faltan, reasigna nodos."""
        bid, sha, t0 = f"{self.id}-{idx}", sha256(data), time.time()
        nodes, acks, tried = self._targets(idx), {}, set()
        for attempt in range(4):
            tried |= {n["node"] for n in nodes}
            futs = {self.fan.submit(self._put_one, n, bid, data, sha): n for n in nodes if n["node"] not in acks}
            remaining, reached = set(futs), None
            while remaining:
                tmo = None
                if len(acks) >= self.need and self.need < self.R:
                    reached = reached or time.time()
                    grace = max(1.0, reached - t0)  # no se espera indefinidamente a la tercera copia
                    tmo = grace - (time.time() - reached)
                    if tmo <= 0:
                        break
                done, remaining = wait(remaining, timeout=tmo, return_when=FIRST_COMPLETED)
                for f in done:
                    try:
                        acks[futs[f]["node"]] = f.result()
                    except Exception:
                        pass
            if len(acks) >= self.need:
                return {"index": idx, "id": bid, "len": len(data), "sha": sha, "acks": list(acks.values())}
            miss = (self.R if self.need == self.R else self.need) - len(acks)
            r = self.c._ctl("POST", f"/v1/uploads/{self.id}/reassign", {"count": miss, "exclude": sorted(tried)})
            nodes = r["nodes"]
            if not nodes:
                break
        raise DFSError(503, "INSUFFICIENT_REPLICAS", f"bloque {idx}: no se logró {self.need} copias confirmadas")

    def abort(self):
        self.done = True
        try:
            self.c._ctl("POST", f"/v1/uploads/{self.id}/abort", retry=2.0)
        except Exception:
            pass
        self.pool.shutdown(wait=False, cancel_futures=True)
        self.fan.shutdown(wait=False, cancel_futures=True)

    def close(self):
        try:
            if self.error:
                raise self.error
            if self.buf:
                chunk, self.buf = bytes(self.buf), bytearray()
                self._submit(chunk)
            blocks = [f.result() for f in self.futs]  # propaga cualquier fallo de bloque
            body = {"blocks": blocks, "content_sha256": self.hash.hexdigest()}
            return self.c._ctl("POST", f"/v1/uploads/{self.id}/commit", body, retry=15.0)
        except BaseException:
            self.abort()
            raise
        finally:
            self.done = True
            self.pool.shutdown(wait=False)
            self.fan.shutdown(wait=False)

    __enter__ = lambda self: self

    def __exit__(self, et, ev, tb):
        if et:
            self.abort()
        else:
            self.close()


# ============================================================================== helpers
def _json(r):
    try:
        return r.json()
    except Exception:
        return {}


def _parse(r):
    if 200 <= r.status_code < 300:
        return r.json()
    j = _json(r)
    raise DFSError(r.status_code, j.get("error", f"HTTP_{r.status_code}"), j.get("message", r.text[:200]))
