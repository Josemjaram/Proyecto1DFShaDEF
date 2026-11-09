"""Controlador DFSha (plano de control).

- Metadatos (usuarios, namespace, versiones, locks, uploads) replicados entre 3 controladores con Raft
  (librería PySyncObj). Solo el líder atiende; los seguidores responden 503 NOT_LEADER.
- No toca datos de archivos: asigna ubicaciones, emite tokens acotados, valida los acuses firmados
  de los nodos y publica atómicamente cada versión (commit).
- Tareas del líder: monitor de nodos (sospechoso/caído), reparación de réplicas, GC y expiración de uploads.
"""
import argparse
import hashlib
import json
import os
import posixpath
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import uvicorn
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pysyncobj import SyncObj, SyncObjConf, SyncObjConsumer, replicated
from pysyncobj.syncobj import SyncObjException

from .common import DFSError, bearer, http, load_config, norm, require_secret, sign, svc_headers, verify

STATE_LOCK = threading.RLock()
PH = PasswordHasher()


# =============================================================================================
# Máquina de estados replicada (determinista: el tiempo y los ids llegan como argumentos)
# =============================================================================================
class Meta(SyncObjConsumer):
    def __init__(self):
        super().__init__()
        self.users = {}     # user -> {"pw", "uid"}
        self.dirs = {}      # user -> set(rutas de directorios)  ("/" implícita)
        self.files = {}     # user -> {ruta: file_id}
        self.finfo = {}     # file_id -> {"owner","path","current","versions":{n:manifest},"deleted"}
        self.blocks = {}    # block_id -> {"replicas":[node], "refs":int, "sha", "len"}
        self.locks = {}     # "user\0ruta" -> {"holder","fence","until","explicit"}
        self.uploads = {}   # upload_id -> registro
        self.fence = 0      # fencing token global y creciente

    # --------------------------------------------------------------- usuarios / directorios
    @replicated
    def register(self, user, pw, uid, ts):
        with STATE_LOCK:
            if user in self.users:
                return {"err": "EXISTS"}
            self.users[user] = {"pw": pw, "uid": uid, "ts": ts}
            self.dirs[user], self.files[user] = set(), {}
            return {"ok": 1}

    @replicated
    def mkdir(self, user, path, ts):
        with STATE_LOCK:
            d, f = self.dirs[user], self.files[user]
            if path == "/" or path in d or path in f:
                return {"err": "EXISTS"}
            parent = posixpath.dirname(path)
            if parent != "/" and parent not in d:
                return {"err": "NO_PARENT"}
            d.add(path)
            return {"ok": 1}

    @replicated
    def rmdir(self, user, path):
        with STATE_LOCK:
            d, f = self.dirs[user], self.files[user]
            if path == "/":
                return {"err": "FORBIDDEN"}
            if path in f:
                return {"err": "NOT_DIR"}
            if path not in d:
                return {"err": "NOT_FOUND"}
            pre = path + "/"
            if any(p.startswith(pre) for p in d) or any(p.startswith(pre) for p in f):
                return {"err": "NOT_EMPTY"}
            d.discard(path)
            return {"ok": 1}

    @replicated
    def rm(self, user, path, ts, holder):
        """Borrado lógico (tombstone): el archivo desaparece del namespace; los bloques se liberan tras la retención."""
        with STATE_LOCK:
            f = self.files[user]
            if path in self.dirs[user]:
                return {"err": "IS_DIR"}
            if path not in f:
                return {"err": "NOT_FOUND"}
            cur = self.locks.get(user + "\0" + path)
            if cur and cur["until"] > ts and cur["holder"] != holder:
                return {"err": "LOCKED", "until": cur["until"]}
            fid = f.pop(path)
            self.finfo[fid]["deleted"] = ts
            self.locks.pop(user + "\0" + path, None)
            return {"ok": 1, "file_id": fid}

    # --------------------------------------------------------------- locks (lease + fencing token)
    def _lock_try(self, key, holder, now, lease, explicit):
        cur = self.locks.get(key)
        if cur and cur["until"] > now and cur["holder"] != holder:
            return None, cur
        if cur and cur["until"] > now and cur["holder"] == holder:
            cur["until"] = now + lease
            cur["explicit"] = cur["explicit"] or explicit
            return cur, None
        self.fence += 1
        self.locks[key] = {"holder": holder, "fence": self.fence, "until": now + lease, "explicit": explicit}
        return self.locks[key], None

    @replicated
    def lock_acquire(self, user, path, holder, now, lease):
        with STATE_LOCK:
            lk, other = self._lock_try(user + "\0" + path, holder, now, lease, True)
            if lk is None:
                return {"err": "LOCKED", "until": other["until"]}
            return {"ok": 1, "fence": lk["fence"], "until": lk["until"]}

    @replicated
    def lock_renew(self, user, path, holder, fence, now, lease):
        with STATE_LOCK:
            cur = self.locks.get(user + "\0" + path)
            if not cur or cur["holder"] != holder or cur["fence"] != fence or cur["until"] < now:
                return {"err": "LEASE_EXPIRED"}
            cur["until"] = now + lease
            return {"ok": 1, "until": cur["until"]}

    @replicated
    def lock_release(self, user, path, holder, fence):
        with STATE_LOCK:
            key = user + "\0" + path
            cur = self.locks.get(key)
            if cur and cur["holder"] == holder and cur["fence"] == fence:
                del self.locks[key]
                return {"ok": 1}
            return {"err": "NOT_HOLDER"}

    # --------------------------------------------------------------- escritura: begin / commit / abort
    @replicated
    def begin_upload(self, upload_id, user, path, mode, holder, now, lease, expected_version):
        with STATE_LOCK:
            d, f = self.dirs[user], self.files[user]
            if path == "/" or path in d:
                return {"err": "IS_DIR"}
            parent = posixpath.dirname(path)
            if parent != "/" and parent not in d:
                return {"err": "NO_PARENT"}
            fid = f.get(path)
            if mode == "create" and fid:
                return {"err": "EXISTS"}
            if mode == "modify" and not fid:
                return {"err": "NOT_FOUND"}  # modificar un rango exige un archivo existente
            base = self.finfo[fid]["current"] if fid else 0
            if expected_version is not None and expected_version != base:
                return {"err": "VERSION_CONFLICT", "current": base}
            key = user + "\0" + path
            cur = self.locks.get(key)
            explicit = bool(cur and cur["holder"] == holder and cur["until"] > now and cur["explicit"])
            lk, other = self._lock_try(key, holder, now, lease, False)
            if lk is None:
                return {"err": "LOCKED", "until": other["until"]}
            self.uploads[upload_id] = {
                "user": user, "path": path, "mode": mode, "file_id": fid or uuid.uuid4().hex[:12],
                "base": base, "version": base + 1, "holder": holder, "fence": lk["fence"],
                "state": "PREPARED", "created": now, "explicit": explicit,
            }
            up = self.uploads[upload_id]
            return {"ok": 1, "file_id": up["file_id"], "version": up["version"], "base_version": base,
                    "fence": lk["fence"], "until": lk["until"]}

    @replicated
    def commit_upload(self, upload_id, user, new_blocks, content_sha, now):
        """Publicación atómica de una nueva versión. new_blocks ya fue validado (acuses firmados) por el líder."""
        with STATE_LOCK:
            up = self.uploads.get(upload_id)
            if not up or up["user"] != user:
                return {"err": "NOT_FOUND"}
            if up["state"] == "COMMITTED":  # idempotente ante reintentos
                return {"ok": 1, "version": up["version"], "size": up.get("size"), "again": True}
            if up["state"] != "PREPARED":
                return {"err": "BAD_STATE", "state": up["state"]}
            key = up["user"] + "\0" + up["path"]
            lk = self.locks.get(key)
            if not lk or lk["holder"] != up["holder"] or lk["fence"] != up["fence"] or lk["until"] < now:
                up["state"] = "ABORTED"
                return {"err": "LEASE_EXPIRED"}
            fid = up["file_id"]
            info = self.finfo.get(fid)
            cur = info["current"] if info else 0
            if cur != up["base"]:
                return {"err": "VERSION_CONFLICT", "current": cur}
            keep = []
            if up["mode"] in ("append", "modify") and cur:
                keep = [dict(b) for b in info["versions"][cur]["blocks"]]
            if up["mode"] == "modify":
                # copy-on-write por bloque: cada bloque nuevo reemplaza al de la misma posición y mismo tamaño;
                # los demás bloques de la versión base se comparten con la nueva versión (conteo de referencias)
                idxs = [b["index"] for b in new_blocks]
                if not new_blocks or len(set(idxs)) != len(idxs) or \
                        any(not (0 <= i < len(keep)) or keep[i]["len"] != b["len"] for i, b in zip(idxs, new_blocks)):
                    return {"err": "BAD_MODIFY"}
                for b in new_blocks:
                    keep[b["index"]] = {"id": b["id"], "off": keep[b["index"]]["off"], "len": b["len"], "sha": b["sha"]}
                    self.blocks[b["id"]] = {"replicas": list(b["replicas"]), "refs": 0, "sha": b["sha"], "len": b["len"], "ts": now}
                off = sum(b["len"] for b in keep)
            else:
                off = sum(b["len"] for b in keep)
                for b in new_blocks:
                    keep.append({"id": b["id"], "off": off, "len": b["len"], "sha": b["sha"]})
                    off += b["len"]
                    self.blocks[b["id"]] = {"replicas": list(b["replicas"]), "refs": 0, "sha": b["sha"], "len": b["len"], "ts": now}
            for b in keep:
                self.blocks[b["id"]]["refs"] += 1
            mh = hashlib.sha256(json.dumps([(b["id"], b["len"], b["sha"]) for b in keep]).encode()).hexdigest()
            if info is None:
                info = self.finfo[fid] = {"owner": user, "path": up["path"], "current": 0, "versions": {}, "deleted": None}
            if cur:
                info["versions"][cur]["superseded"] = now
            ver = up["version"]
            info["versions"][ver] = {"version": ver, "size": off, "blocks": keep, "mhash": mh, "ts": now,
                                     "content_sha": content_sha if up["mode"] not in ("append", "modify") else None,
                                     "superseded": None}
            info["current"] = ver
            self.files[user][up["path"]] = fid
            up["state"], up["size"] = "COMMITTED", off
            if not (lk.get("explicit") or up["explicit"]):
                self.locks.pop(key, None)
            return {"ok": 1, "version": ver, "size": off, "file_id": fid}

    @replicated
    def abort_upload(self, upload_id, user):
        with STATE_LOCK:
            up = self.uploads.get(upload_id)
            if not up or up["user"] != user or up["state"] != "PREPARED":
                return {"err": "NOT_FOUND"}
            up["state"] = "ABORTED"
            key = up["user"] + "\0" + up["path"]
            lk = self.locks.get(key)
            if lk and lk["fence"] == up["fence"] and not lk["explicit"]:
                del self.locks[key]
            return {"ok": 1}

    @replicated
    def expire_uploads(self, now, grace):
        with STATE_LOCK:
            out = []
            for uid, up in self.uploads.items():
                if up["state"] == "PREPARED":
                    lk = self.locks.get(up["user"] + "\0" + up["path"])
                    if not lk or lk["fence"] != up["fence"] or lk["until"] + grace < now:
                        up["state"] = "ABORTED"
                        out.append(uid)
            for uid in [u for u, up in self.uploads.items() if up["state"] != "PREPARED" and now - up["created"] > 3600]:
                del self.uploads[uid]
            return out

    # --------------------------------------------------------------- réplicas / GC
    @replicated
    def add_replica(self, bid, node):
        with STATE_LOCK:
            b = self.blocks.get(bid)
            if b and node not in b["replicas"]:
                b["replicas"].append(node)
            return {"ok": 1}

    def owns_block(self, user, bid):
        with STATE_LOCK:
            return any(i["owner"] == user and any(b["id"] == bid for v in i["versions"].values() for b in v["blocks"])
                       for i in self.finfo.values())

    @replicated
    def remove_replica(self, bid, node):
        with STATE_LOCK:
            b = self.blocks.get(bid)
            if b and node in b["replicas"]:
                b["replicas"].remove(node)
            return {"ok": 1}

    @replicated
    def drop_versions(self, now, retention):
        """Descarta versiones reemplazadas/borradas tras la retención y libera bloques sin referencias."""
        with STATE_LOCK:
            freed = []
            for fid in list(self.finfo):
                info = self.finfo[fid]
                dead_file = info["deleted"] is not None and now - info["deleted"] > retention
                for n in list(info["versions"]):
                    v = info["versions"][n]
                    old = n != info["current"] and v["superseded"] is not None and now - v["superseded"] > retention
                    if dead_file or old:
                        for b in v["blocks"]:
                            blk = self.blocks.get(b["id"])
                            if blk:
                                blk["refs"] -= 1
                                if blk["refs"] <= 0:
                                    freed.append((b["id"], list(blk["replicas"])))
                                    del self.blocks[b["id"]]
                        del info["versions"][n]
                if dead_file:
                    del self.finfo[fid]
            return freed


# =============================================================================================
# Servicio (solo el líder atiende)
# =============================================================================================
class Controller:
    def __init__(self, cfg, my_id):
        self.cfg, self.my_id = cfg, my_id
        me = next(c for c in cfg["controllers"] if c["id"] == my_id)
        others = [c["raft"] for c in cfg["controllers"] if c["id"] != my_id]
        self.api_by_raft = {c["raft"]: c["api"] for c in cfg["controllers"]}
        self.meta = Meta()
        state_dir = os.environ.get("DFSHA_STATE_DIR")
        conf = dict(password=require_secret("DFSHA_CLUSTER_KEY", "dev-only-change-me"), useFork=False,
                    raftMinTimeout=1.0, raftMaxTimeout=2.0, appendEntriesPeriod=0.15,
                    leaderFallbackTimeout=8.0, commandsWaitLeader=False, connectionRetryTime=2.0,
                    logCompactionMinEntries=20000)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
            conf.update(journalFile=os.path.join(state_dir, f"{my_id}.journal"),
                        fullDumpFile=os.path.join(state_dir, f"{my_id}.dump"))
        self.raft = SyncObj(me["raft"], others, SyncObjConf(**conf), consumers=[self.meta])
        self.nodes = {}  # soft state: node_id -> {"url","last","free","blocks":{bid:mtime},"br":ts}
        self.nlock = threading.RLock()
        self.assigned = {}
        self.pool = ThreadPoolExecutor(max_workers=8)

    # ------------------------------------------------------------ utilidades
    def is_leader(self):
        return self.raft._isLeader()

    def leader_api(self):
        ld = self.raft._getLeader()
        return self.api_by_raft.get(getattr(ld, "address", str(ld))) if ld else None

    def guard(self):
        if not self.is_leader():
            raise DFSError(503, "NOT_LEADER", self.leader_api() or "")

    def call(self, fn, *a):
        try:
            return fn(*a, sync=True, timeout=self.cfg["raft_timeout"])
        except SyncObjException as e:
            raise DFSError(503, "NO_QUORUM", f"sin mayoría de controladores: {e}")

    def node_state(self, nid, now=None):
        now = now or time.time()
        n = self.nodes.get(nid)
        if not n:
            return "DEAD"
        age = now - n["last"]
        if age <= self.cfg["hb_interval"] * self.cfg["suspect_after"]:
            return "ALIVE"
        return "SUSPECT" if age <= self.cfg["dead_after"] else "DEAD"

    def alive_nodes(self):
        with self.nlock:
            return [n for n in self.nodes if self.node_state(n) == "ALIVE"]

    def locations(self, bid):
        blk = self.meta.blocks.get(bid)
        if not blk:
            return []
        with self.nlock:
            ok = [r for r in blk["replicas"] if r in self.nodes and self.node_state(r) == "ALIVE"]
            sus = [r for r in blk["replicas"] if r in self.nodes and self.node_state(r) == "SUSPECT"]
            random.shuffle(ok)
            return [{"node": r, "url": self.nodes[r]["url"]} for r in ok + sus]

    def place(self, count, exclude=()):
        """Elige 'count' nodos distintos vivos, los menos cargados primero (balanceo)."""
        with self.nlock:
            cand = [n for n in self.alive_nodes() if n not in exclude]
            cand.sort(key=lambda n: (self.assigned.get(n, 0) + len(self.nodes[n]["blocks"]), random.random()))
            chosen = cand[:count]
            for n in chosen:
                self.assigned[n] = self.assigned.get(n, 0) + 1
            return [{"node": n, "url": self.nodes[n]["url"]} for n in chosen]

    # ------------------------------------------------------------ vistas
    def manifest_view(self, fid, ver, user):
        info = self.meta.finfo[fid]
        v = info["versions"][ver]
        ids = [b["id"] for b in v["blocks"]]
        blocks = [{"index": i, "id": b["id"], "offset": b["off"], "length": b["len"], "sha256": b["sha"],
                   "locations": self.locations(b["id"])} for i, b in enumerate(v["blocks"])]
        tok = sign({"typ": "data", "op": "get", "sub": user, "file": fid, "ver": ver, "blk": ids},
                   ttl=self.cfg["read_ttl"])
        return {"file_id": fid, "path": info["path"], "version": ver, "size": v["size"],
                "content_sha256": v["content_sha"], "manifest_hash": v["mhash"], "blocks": blocks,
                "read_token": tok}

    def cluster_view(self):
        now = time.time()
        R = self.cfg["replication"]
        with self.nlock:
            nodes = {n: {"url": d["url"], "state": self.node_state(n, now), "age": round(now - d["last"], 1),
                         "blocks": len(d["blocks"]), "free": d["free"]} for n, d in self.nodes.items()}
        under = lost = 0
        with STATE_LOCK:
            total = len(self.meta.blocks)
            for b in list(self.meta.blocks.values()):
                live = [r for r in b["replicas"] if nodes.get(r, {}).get("state", "DEAD") != "DEAD"]
                if not live:
                    lost += 1
                elif len(live) < R:
                    under += 1
        return {"leader": self.is_leader(), "leader_api": self.leader_api() or None, "nodes": nodes,
                "blocks_total": total, "under_replicated": under, "lost": lost,
                "files": len(self.meta.finfo), "term": self.raft._getTerm()}

    # ------------------------------------------------------------ tareas de fondo (solo líder)
    def background(self):
        last = {"monitor": 0, "repair": 0, "gc": 0}
        while True:
            time.sleep(0.5)
            try:
                if not self.is_leader():
                    continue
                now = time.time()
                if now - last["monitor"] > 2:
                    last["monitor"] = now
                    self.monitor()
                if now - last["repair"] > 3:
                    last["repair"] = now
                    self.repair()
                if now - last["gc"] > 5:
                    last["gc"] = now
                    self.gc()
            except Exception as e:  # el bucle nunca debe morir
                print("background error:", repr(e), flush=True)

    def monitor(self):
        """Nodo sin heartbeat => sospechoso; se sondea activamente antes de darlo por caído."""
        with self.nlock:
            ids = list(self.nodes)
        for nid in ids:
            st = self.node_state(nid)
            if st == "ALIVE":
                continue
            try:
                r = http().get(self.nodes[nid]["url"] + "/health", timeout=(1, 1.5))
                if r.status_code == 200:  # heartbeat perdido no prueba que el nodo esté caído
                    self.nodes[nid]["last"] = time.time()
            except Exception:
                pass

    def repair(self):
        R = self.cfg["replication"]
        now = time.time()
        jobs = []
        with STATE_LOCK:
            items = list(self.meta.blocks.items())
        for bid, b in items:
            with self.nlock:
                # copia que el nodo ya no reporta (borrada/corrupta): se retira de la lista de réplicas
                for r in list(b["replicas"]):
                    n = self.nodes.get(r)
                    if n and self.node_state(r) == "ALIVE" and n["br"] and now - n["br"] < 25 \
                            and bid not in n["blocks"] and b["ts"] + 15 < n["br"]:
                        self.call(self.meta.remove_replica, bid, r)
                live = [r for r in b["replicas"] if r in self.nodes and self.node_state(r) != "DEAD"]
            if len(live) >= R:
                continue
            if not live:
                continue  # sin copias válidas: el bloque está perdido (se reporta en /v1/cluster)
            jobs.append((bid, b, live))
            if len(jobs) >= 24:
                break
        for bid, b, live in jobs:
            self.pool.submit(self._repair_one, bid, b, live)

    def _repair_one(self, bid, b, live):
        with self.nlock:
            src = next((r for r in live if self.node_state(r) == "ALIVE"), None)
            if not src:
                return
            tgt = self.place(1, exclude=set(b["replicas"]))
        if not tgt:
            return
        tgt = tgt[0]
        tok = sign({"typ": "data", "op": "get", "sub": "repair", "blk": [bid]}, ttl=60)
        try:
            r = http().post(tgt["url"] + "/internal/replicate", headers=svc_headers("controller"), timeout=(3, 60),
                            json={"block_id": bid, "source_url": self.nodes[src]["url"], "read_token": tok,
                                  "sha256": b["sha"]})
            if r.status_code == 200:
                self.call(self.meta.add_replica, bid, tgt["node"])
        except Exception as e:
            print("repair fallo", bid, repr(e), flush=True)

    def gc(self):
        now = time.time()
        for uid in self.call(self.meta.expire_uploads, now, 5.0) or []:
            pass
        freed = self.call(self.meta.drop_versions, now, self.cfg["retention"]) or []
        for bid, reps in freed:
            for r in reps:
                n = self.nodes.get(r)
                if n and self.node_state(r) != "DEAD":
                    self.pool.submit(self._delete_block, n["url"], bid)

    def _delete_block(self, url, bid):
        try:
            http().delete(f"{url}/blocks/{bid}", headers=svc_headers("controller"), timeout=(2, 5))
        except Exception:
            pass

    def process_blockreport(self, node, url, blocks):
        now = time.time()
        with self.nlock:
            n = self.nodes.setdefault(node, {"url": url, "last": now, "free": 0, "blocks": {}, "br": 0})
            n["url"], n["blocks"], n["br"] = url, {b: m for b, m in blocks}, now
        if not self.is_leader():
            return
        orphans, adopt = [], []
        with STATE_LOCK:
            for bid, mtime in blocks:
                blk = self.meta.blocks.get(bid)
                if blk is None:
                    upid = bid.split("-")[0]
                    up = self.meta.uploads.get(upid)
                    if (up and up["state"] == "PREPARED") or now - mtime < self.cfg["orphan_grace"]:
                        continue
                    orphans.append(bid)
                elif node not in blk["replicas"] and len(blk["replicas"]) < self.cfg["replication"]:
                    adopt.append(bid)  # el nodo regresó con una copia que sirve: cuenta como réplica
        for bid in orphans:
            self.pool.submit(self._delete_block, url, bid)  # reconciliación: bloque sin dueño
        for bid in adopt:
            self.call(self.meta.add_replica, bid, node)


# =============================================================================================
# API HTTP
# =============================================================================================
def build_app(ctl: Controller):
    cfg = ctl.cfg
    app = FastAPI(title="DFSha controller")
    meta = ctl.meta

    @app.exception_handler(DFSError)
    async def _h(_, e: DFSError):
        return JSONResponse({"error": e.code, "message": e.msg}, status_code=e.status)

    def user_of(authorization):
        c = verify(bearer(authorization))
        if c.get("typ") != "user":
            raise DFSError(403, "FORBIDDEN", "se requiere token de usuario")
        return c["sub"]

    def auth(authorization):
        user = user_of(authorization)
        ctl.guard()
        if user not in meta.users:
            raise DFSError(401, "BAD_TOKEN", "usuario inexistente")
        return user

    def svc_only(authorization):
        if verify(bearer(authorization)).get("typ") != "svc":
            raise DFSError(403, "FORBIDDEN", "se requiere token de servicio")

    def holder(user, session):
        return f"{user}:{session or 'default'}"

    MAP = {"EXISTS": (409, "EXISTS"), "NO_PARENT": (404, "NO_PARENT"), "NOT_EMPTY": (409, "NOT_EMPTY"),
           "NOT_FOUND": (404, "NOT_FOUND"), "NOT_DIR": (409, "NOT_DIR"), "IS_DIR": (409, "IS_DIR"),
           "LOCKED": (423, "LOCKED"), "LEASE_EXPIRED": (409, "LEASE_EXPIRED"),
           "VERSION_CONFLICT": (412, "VERSION_CONFLICT"), "FORBIDDEN": (403, "FORBIDDEN"),
           "BAD_STATE": (409, "BAD_STATE"), "NOT_HOLDER": (409, "NOT_HOLDER")}

    def res(r):
        if "err" in r:
            st, code = MAP.get(r["err"], (400, r["err"]))
            raise DFSError(st, code, json.dumps({k: v for k, v in r.items() if k != "err"}))
        return r

    # ------------------------------------------------------------ salud / cluster
    @app.get("/health")
    def health():
        return {"id": ctl.my_id, "leader": ctl.is_leader(), "ready": ctl.raft.isReady()}

    @app.get("/v1/cluster")
    def cluster(x_admin_key: str = Header(None)):
        if x_admin_key != require_secret("DFSHA_ADMIN_KEY", "admin-dev"):
            raise DFSError(403, "FORBIDDEN", "clave de operador requerida")
        ctl.guard()
        return ctl.cluster_view()

    # ------------------------------------------------------------ cuentas
    @app.post("/v1/register")
    def register(req: dict):
        ctl.guard()
        u, p = str(req.get("user", "")), str(req.get("password", ""))
        if not u.isidentifier() or len(p) < 6:
            raise DFSError(400, "BAD_REQUEST", "usuario alfanumérico y contraseña de al menos 6 caracteres")
        res(ctl.call(meta.register, u, PH.hash(p), uuid.uuid4().hex[:8], time.time()))
        return {"ok": True}

    @app.post("/v1/login")
    def login(req: dict):
        ctl.guard()
        u = meta.users.get(str(req.get("user")))
        try:
            if not u:
                raise VerifyMismatchError()
            PH.verify(u["pw"], str(req.get("password", "")))
        except VerifyMismatchError:
            raise DFSError(401, "BAD_CREDENTIALS", "usuario o contraseña incorrectos")
        return {"token": sign({"typ": "user", "sub": req["user"]}, ttl=3600), "expires_in": 3600}

    # ------------------------------------------------------------ namespace (RF1)
    @app.get("/v1/ls")
    def ls(path: str = "/", authorization: str = Header(None)):
        user = auth(authorization)
        path = norm(path)
        with STATE_LOCK:
            d, f = meta.dirs[user], meta.files[user]
            if path in f:
                fid = f[path]
                v = meta.finfo[fid]["versions"][meta.finfo[fid]["current"]]
                return {"path": path, "type": "file", "entries": [
                    {"name": posixpath.basename(path), "type": "file", "size": v["size"], "version": v["version"]}]}
            if path != "/" and path not in d:
                raise DFSError(404, "NOT_FOUND", path)
            pre = "/" if path == "/" else path + "/"
            out = []
            for p in sorted(d):
                if p.startswith(pre) and "/" not in p[len(pre):]:
                    out.append({"name": p[len(pre):], "type": "dir"})
            for p, fid in sorted(f.items()):
                if p.startswith(pre) and "/" not in p[len(pre):]:
                    info = meta.finfo[fid]
                    v = info["versions"][info["current"]]
                    out.append({"name": p[len(pre):], "type": "file", "size": v["size"], "version": v["version"]})
            return {"path": path, "type": "dir", "entries": out}

    @app.get("/v1/stat")
    def stat(path: str, authorization: str = Header(None)):
        user = auth(authorization)
        path = norm(path)
        with STATE_LOCK:
            if path == "/" or path in meta.dirs[user]:
                return {"path": path, "type": "dir"}
            fid = meta.files[user].get(path)
            if not fid:
                raise DFSError(404, "NOT_FOUND", path)
            info = meta.finfo[fid]
            v = info["versions"][info["current"]]
            return {"path": path, "type": "file", "file_id": fid, "size": v["size"], "version": v["version"]}

    @app.post("/v1/mkdir")
    def mkdir(req: dict, authorization: str = Header(None)):
        user = auth(authorization)
        return res(ctl.call(meta.mkdir, user, norm(req["path"]), time.time()))

    @app.post("/v1/rmdir")
    def rmdir(req: dict, authorization: str = Header(None)):
        user = auth(authorization)
        return res(ctl.call(meta.rmdir, user, norm(req["path"])))

    @app.delete("/v1/files")
    def rm(path: str, authorization: str = Header(None), x_session: str = Header(None)):
        user = auth(authorization)
        return res(ctl.call(meta.rm, user, norm(path), time.time(), holder(user, x_session)))

    # ------------------------------------------------------------ manifiestos (lectura)
    def _manifest(user, fid, version=None):
        info = meta.finfo.get(fid)
        if not info or info["deleted"] is not None:
            raise DFSError(404, "NOT_FOUND", "archivo inexistente")
        if info["owner"] != user:
            raise DFSError(403, "FORBIDDEN", "el archivo pertenece a otro usuario")
        ver = version or info["current"]
        if ver not in info["versions"]:
            raise DFSError(410, "VERSION_EXPIRED", "esa versión ya fue liberada")
        return ctl.manifest_view(fid, ver, user)

    @app.get("/v1/files/manifest")
    def manifest_by_path(path: str, version: int = None, authorization: str = Header(None)):
        user = auth(authorization)
        fid = meta.files[user].get(norm(path))
        if not fid:
            raise DFSError(404, "NOT_FOUND", path)
        return _manifest(user, fid, version)

    @app.get("/v1/files/{fid}/manifest")
    def manifest_by_id(fid: str, authorization: str = Header(None)):
        user = auth(authorization)
        return _manifest(user, fid)

    @app.post("/v1/blocks/corrupt")
    def corrupt(req: dict, authorization: str = Header(None)):
        user = auth(authorization)
        if not meta.owns_block(user, req["block_id"]):  # solo el dueño del archivo puede reportar sus bloques
            raise DFSError(403, "FORBIDDEN", "el bloque no pertenece a un archivo de este usuario")
        ctl.call(meta.remove_replica, req["block_id"], req["node"])
        return {"ok": True}

    # ------------------------------------------------------------ uploads (RF2 / RF3 escritura)
    def _up(upload_id, user):
        up = meta.uploads.get(upload_id)
        if not up or up["user"] != user:
            raise DFSError(404, "NOT_FOUND", "upload inexistente")
        return up

    def _write_token(user, upload_id):
        return sign({"typ": "data", "op": "put", "sub": user, "pfx": [upload_id + "-"]}, ttl=cfg["lease"] * 4)

    @app.post("/v1/uploads")
    def begin(req: dict, authorization: str = Header(None), x_session: str = Header(None)):
        user = auth(authorization)
        mode = req.get("mode", "replace")
        if mode not in ("create", "replace", "append", "modify"):
            raise DFSError(400, "BAD_MODE", "mode ∈ create|replace|append|modify")
        if len(ctl.alive_nodes()) < cfg["min_ack"]:
            raise DFSError(503, "INSUFFICIENT_NODES", "no hay suficientes nodos de datos vivos")
        uid = uuid.uuid4().hex[:12]
        r = res(ctl.call(meta.begin_upload, uid, user, norm(req["path"]), mode, holder(user, x_session),
                         time.time(), cfg["lease"], req.get("expected_version")))
        bs = int(req.get("block_size") or cfg["block_size"])
        bs = max(64 * 1024, min(bs, 64 * 1024 * 1024))
        return {"upload_id": uid, **r, "block_size": bs, "replication": cfg["replication"],
                "min_ack": cfg["min_ack"], "strict": cfg["strict_replication"],
                "write_token": _write_token(user, uid), "lease": cfg["lease"]}

    @app.post("/v1/uploads/{uid}/allocate")
    def allocate(uid: str, req: dict, authorization: str = Header(None), x_session: str = Header(None)):
        user = auth(authorization)
        up = _up(uid, user)
        res(ctl.call(meta.lock_renew, user, up["path"], up["holder"], up["fence"], time.time(), cfg["lease"]))
        out = []
        for i in range(int(req["start"]), int(req["start"]) + int(req["count"])):
            nodes = ctl.place(cfg["replication"])
            if len(nodes) < cfg["min_ack"]:
                raise DFSError(503, "INSUFFICIENT_NODES", "nodos vivos insuficientes")
            out.append({"index": i, "id": f"{uid}-{i}", "nodes": nodes})
        return {"targets": out, "write_token": _write_token(user, uid)}

    @app.post("/v1/uploads/{uid}/reassign")
    def reassign(uid: str, req: dict, authorization: str = Header(None)):
        user = auth(authorization)
        _up(uid, user)
        nodes = ctl.place(int(req.get("count", 1)), exclude=set(req.get("exclude", [])))
        return {"nodes": nodes}

    @app.post("/v1/uploads/{uid}/renew")
    def renew(uid: str, authorization: str = Header(None)):
        user = auth(authorization)
        up = _up(uid, user)
        return res(ctl.call(meta.lock_renew, user, up["path"], up["holder"], up["fence"], time.time(), cfg["lease"]))

    @app.post("/v1/uploads/{uid}/commit")
    def commit(uid: str, req: dict, authorization: str = Header(None)):
        user = auth(authorization)
        up = _up(uid, user)
        need = cfg["replication"] if cfg["strict_replication"] else cfg["min_ack"]
        blocks = sorted(req["blocks"], key=lambda b: b["index"])
        idxs = [b["index"] for b in blocks]
        if up["mode"] == "modify":  # subconjunto de posiciones del archivo base, sin repetir
            if not blocks or len(set(idxs)) != len(idxs) or idxs[0] < 0:
                raise DFSError(422, "INCOMPLETE", "modificación sin bloques o con posiciones repetidas")
        elif idxs != list(range(len(blocks))):
            raise DFSError(422, "INCOMPLETE", "faltan bloques en el manifiesto")
        clean = []
        for b in blocks:
            if b["id"] != f"{uid}-{b['index']}" or b["len"] <= 0:
                raise DFSError(422, "INCOMPLETE", f"bloque inválido {b['index']}")
            reps = set()
            for a in b["acks"]:  # acuses firmados por los nodos: el cliente no puede falsificarlos
                try:
                    c = verify(a)
                except DFSError:
                    continue
                if c.get("typ") == "ack" and c["blk"] == b["id"] and c["sha"] == b["sha"] and c["len"] == b["len"] \
                        and c["node"] in ctl.nodes:
                    reps.add(c["node"])
            if len(reps) < need:
                raise DFSError(422, "INSUFFICIENT_REPLICAS",
                               f"bloque {b['index']}: {len(reps)} copias confirmadas, se requieren {need}")
            clean.append({"index": b["index"], "id": b["id"], "len": b["len"], "sha": b["sha"], "replicas": sorted(reps)})
        r = res(ctl.call(meta.commit_upload, uid, user, clean, req.get("content_sha256"), time.time()))
        return r

    @app.post("/v1/uploads/{uid}/abort")
    def abort(uid: str, authorization: str = Header(None)):
        user = auth(authorization)
        _up(uid, user)
        return res(ctl.call(meta.abort_upload, uid, user))

    # ------------------------------------------------------------ locks explícitos
    @app.post("/v1/locks")
    def lock(req: dict, authorization: str = Header(None), x_session: str = Header(None)):
        user = auth(authorization)
        r = res(ctl.call(meta.lock_acquire, user, norm(req["path"]), holder(user, x_session), time.time(), cfg["lease"]))
        return r

    @app.post("/v1/locks/renew")
    def lock_renew(req: dict, authorization: str = Header(None), x_session: str = Header(None)):
        user = auth(authorization)
        return res(ctl.call(meta.lock_renew, user, norm(req["path"]), holder(user, x_session), int(req["fence"]),
                            time.time(), cfg["lease"]))

    @app.delete("/v1/locks")
    def unlock(path: str, fence: int, authorization: str = Header(None), x_session: str = Header(None)):
        user = auth(authorization)
        return res(ctl.call(meta.lock_release, user, norm(path), holder(user, x_session), fence))

    # ------------------------------------------------------------ internos (nodos de datos)
    @app.post("/internal/heartbeat")
    def heartbeat(req: dict, authorization: str = Header(None)):
        svc_only(authorization)
        with ctl.nlock:
            n = ctl.nodes.setdefault(req["node"], {"url": req["url"], "last": 0, "free": 0, "blocks": {}, "br": 0})
            n["url"], n["free"], n["last"] = req["url"], req["free"], time.time()
        return {"ok": True}

    @app.post("/internal/blockreport")
    def blockreport(req: dict, authorization: str = Header(None)):
        svc_only(authorization)
        ctl.process_blockreport(req["node"], req["url"], req["blocks"])
        return {"ok": True}

    return app


def main():
    ap = argparse.ArgumentParser(description="DFSha controller")
    ap.add_argument("--config", required=True)
    ap.add_argument("--id", required=True)
    ap.add_argument("--listen", default="0.0.0.0:8001")
    a = ap.parse_args()
    cfg = load_config(a.config)
    require_secret("DFSHA_ADMIN_KEY", "admin-dev")  # falla al arrancar si falta (no al primer /v1/cluster)
    ctl = Controller(cfg, a.id)
    threading.Thread(target=ctl.background, daemon=True).start()
    host, port = a.listen.rsplit(":", 1)
    kw = {}
    if os.environ.get("DFSHA_TLS_CERT"):
        kw = {"ssl_certfile": os.environ["DFSHA_TLS_CERT"], "ssl_keyfile": os.environ["DFSHA_TLS_KEY"]}
    uvicorn.run(build_app(ctl), host=host, port=int(port), log_level="warning", **kw)


if __name__ == "__main__":
    main()
