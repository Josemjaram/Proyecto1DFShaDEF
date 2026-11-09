"""Nodo de datos DFSha.

Responsabilidades: guardar bloques inmutables (cifrados en reposo con AES-256-GCM),
validar SHA-256, servir lecturas, copiar bloques desde otro nodo (reparación) y
reportar heartbeats y blockreports a TODOS los controladores.
"""
import argparse
import hashlib
import os
import re
import shutil
import struct
import threading
import time
import uuid

import uvicorn
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from .common import DFSError, bearer, cluster_key, http, load_config, require_secret, sha256, sign, svc_headers, verify

BID_RE = re.compile(r"^[A-Za-z0-9_-]{1,96}$")
MAX_BLOCK = 128 * 1024 * 1024
MAGIC_ENC, MAGIC_PLAIN = b"DFB1", b"DFB0"


class Corrupt(Exception):
    pass


class Store:
    """Almacenamiento local. Formato: MAGIC | sha256(32) | len(8) | [nonce(12)] | datos(cifrados)."""

    def __init__(self, root, key: str, encrypt=True, fsync=True):
        self.blocks = os.path.join(root, "blocks")
        self.tmp = os.path.join(root, "tmp")
        os.makedirs(self.blocks, exist_ok=True)
        shutil.rmtree(self.tmp, ignore_errors=True)  # escrituras interrumpidas: se descartan al arrancar
        os.makedirs(self.tmp, exist_ok=True)
        self.aes = AESGCM(hashlib.sha256(key.encode()).digest())
        self.encrypt, self.fsync = encrypt, fsync

    def path(self, bid):
        d = os.path.join(self.blocks, bid[:2])
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, bid)

    def put(self, bid, data: bytes, sha: str):
        final = self.path(bid)
        if os.path.exists(final):  # idempotente: mismo bloque (inmutable)
            try:
                if self.get(bid)[1] == sha:
                    return
            except Corrupt:
                pass
        head = struct.pack(">32sQ", bytes.fromhex(sha), len(data))
        if self.encrypt:
            nonce = os.urandom(12)
            blob = MAGIC_ENC + head + nonce + self.aes.encrypt(nonce, data, bid.encode())
        else:
            blob = MAGIC_PLAIN + head + data
        tmp = os.path.join(self.tmp, uuid.uuid4().hex)
        with open(tmp, "wb") as f:  # nunca se escribe directo al nombre final
            f.write(blob)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, final)  # publicación atómica del bloque
        if self.fsync:
            fd = os.open(os.path.dirname(final), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def get(self, bid):
        p = self.path(bid)
        with open(p, "rb") as f:
            blob = f.read()
        magic, (sha, n), rest = blob[:4], struct.unpack(">32sQ", blob[4:44]), blob[44:]
        try:
            if magic == MAGIC_ENC:
                data = self.aes.decrypt(rest[:12], rest[12:], bid.encode())
            elif magic == MAGIC_PLAIN:
                data = rest
            else:
                raise Corrupt("magic")
        except Exception as e:
            raise Corrupt(f"descifrado/formato: {e}")
        if len(data) != n or hashlib.sha256(data).digest() != sha:
            raise Corrupt("hash")
        return data, sha.hex()

    def delete(self, bid):
        try:
            os.remove(self.path(bid))
            return True
        except FileNotFoundError:
            return False

    def inventory(self):
        out = []
        for d, _, files in os.walk(self.blocks):
            for fn in files:
                try:
                    out.append([fn, os.path.getmtime(os.path.join(d, fn))])
                except OSError:
                    pass
        return out


def build_app(node_id, advertise, store: Store, cfg):
    app = FastAPI(title=f"DFSha data node {node_id}")

    def err(status, code, msg=""):
        return JSONResponse({"error": code, "message": msg}, status_code=status)

    def check_data(authorization, op, bid):
        c = verify(bearer(authorization))
        if c.get("typ") != "data" or c.get("op") != op:
            raise DFSError(403, "FORBIDDEN", "token no sirve para esta operación")
        ok = bid in c.get("blk", []) or any(bid.startswith(p) for p in c.get("pfx", []))
        if not ok:
            raise DFSError(403, "FORBIDDEN", "token no autoriza este bloque")
        return c

    def check_svc(authorization):
        c = verify(bearer(authorization))
        if c.get("typ") != "svc":
            raise DFSError(403, "FORBIDDEN", "se requiere token de servicio")

    @app.exception_handler(DFSError)
    async def _h(_, e: DFSError):
        return err(e.status, e.code, e.msg)

    @app.get("/health")
    def health():
        return {"node": node_id, "status": "ok", "free": shutil.disk_usage(store.blocks).free}

    @app.put("/blocks/{bid}")
    async def put_block(bid: str, request: Request, authorization: str = Header(None),
                        x_sha256: str = Header(None)):
        if not BID_RE.match(bid):
            return err(400, "BAD_ID")
        check_data(authorization, "put", bid)
        body = await request.body()
        if len(body) > MAX_BLOCK:
            return err(413, "TOO_LARGE")
        sha = sha256(body)
        if x_sha256 and x_sha256 != sha:
            return err(422, "INVALID_HASH", "el hash recibido no coincide (bloque incompleto o alterado)")
        await run_in_threadpool(store.put, bid, body, sha)
        ack = sign({"typ": "ack", "node": node_id, "blk": bid, "sha": sha, "len": len(body)})
        return {"stored": True, "durable": True, "node": node_id, "sha256": sha,
                "length": len(body), "ack": ack}

    @app.get("/blocks/{bid}")
    def get_block(bid: str, authorization: str = Header(None)):
        if not BID_RE.match(bid):
            return err(400, "BAD_ID")
        check_data(authorization, "get", bid)
        try:
            data, sha = store.get(bid)
        except FileNotFoundError:
            return err(404, "NOT_FOUND")
        except Corrupt as e:
            return err(422, "CORRUPT", str(e))
        return Response(content=data, media_type="application/octet-stream", headers={"X-Sha256": sha})

    @app.delete("/blocks/{bid}")
    def del_block(bid: str, authorization: str = Header(None)):
        if not BID_RE.match(bid):
            return err(400, "BAD_ID")
        check_svc(authorization)
        return {"deleted": store.delete(bid)}

    @app.post("/internal/replicate")
    def replicate(req: dict, authorization: str = Header(None)):
        """Reparación: copiar un bloque desde otro nodo (el controlador entrega ubicación y token de lectura)."""
        check_svc(authorization)
        bid, src = req["block_id"], req["source_url"]
        if not BID_RE.match(bid):
            return err(400, "BAD_ID")
        r = http().get(f"{src}/blocks/{bid}", headers={"Authorization": "Bearer " + req["read_token"]},
                       timeout=(3, 60))
        if r.status_code != 200:
            return err(502, "SOURCE_FAILED", f"origen respondió {r.status_code}")
        if sha256(r.content) != req["sha256"]:
            return err(422, "INVALID_HASH", "el bloque copiado no coincide con el hash esperado")
        store.put(bid, r.content, req["sha256"])
        return {"stored": True, "node": node_id}

    # ------------------------------------------------------------ heartbeats
    def reporter():
        last_br = 0.0
        while True:
            now = time.time()
            payload = {"node": node_id, "url": advertise, "free": shutil.disk_usage(store.blocks).free, "ts": now}
            for c in cfg["controllers"]:
                try:
                    http().post(c["api"] + "/internal/heartbeat", json=payload, headers=svc_headers(node_id),
                                timeout=(1.5, 2))
                except Exception:
                    pass
            if now - last_br >= cfg["blockreport_interval"]:
                last_br = now
                rep = {"node": node_id, "url": advertise, "blocks": store.inventory()}
                for c in cfg["controllers"]:
                    try:
                        http().post(c["api"] + "/internal/blockreport", json=rep, headers=svc_headers(node_id),
                                    timeout=(1.5, 10))
                    except Exception:
                        pass
            time.sleep(cfg["hb_interval"])

    @app.on_event("startup")
    def _start():
        threading.Thread(target=reporter, daemon=True).start()

    return app


def main():
    ap = argparse.ArgumentParser(description="DFSha data node")
    ap.add_argument("--config", required=True)
    ap.add_argument("--id", required=True)
    ap.add_argument("--listen", default="0.0.0.0:9001")
    ap.add_argument("--advertise", required=True, help="URL con la que otros componentes alcanzan este nodo")
    ap.add_argument("--data", required=True)
    a = ap.parse_args()
    cfg = load_config(a.config)
    cluster_key()  # falla al arrancar (no en la primera petición) si falta DFSHA_CLUSTER_KEY
    store = Store(a.data, require_secret("DFSHA_NODE_KEY", "dev-node-key"),
                  encrypt=os.environ.get("DFSHA_ENCRYPT", "1") == "1",
                  fsync=os.environ.get("DFSHA_FSYNC", "1") == "1")
    host, port = a.listen.rsplit(":", 1)
    kw = {}
    if os.environ.get("DFSHA_TLS_CERT"):
        kw = {"ssl_certfile": os.environ["DFSHA_TLS_CERT"], "ssl_keyfile": os.environ["DFSHA_TLS_KEY"]}
    uvicorn.run(build_app(a.id, a.advertise, store, cfg), host=host, port=int(port), log_level="warning", **kw)


if __name__ == "__main__":
    main()
