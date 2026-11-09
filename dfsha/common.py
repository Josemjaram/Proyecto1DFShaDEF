"""Utilidades comunes de DFSha: errores, tokens firmados, rutas, configuración y HTTP."""
import base64
import hashlib
import hmac
import json
import os
import posixpath
import threading
import time

import requests
from requests.adapters import HTTPAdapter



def require_secret(name, dev_default):
    """Secretos obligatorios: sin la variable de entorno el proceso no arranca (salvo DFSHA_DEV=1, solo desarrollo)."""
    v = os.environ.get(name)
    if v:
        return v
    if os.environ.get("DFSHA_DEV") == "1":
        return dev_default
    raise SystemExit(f"Falta el secreto {name}. Defínalo en /opt/dfsha/env (ver README). "
                     "DFSHA_DEV=1 permite un valor de desarrollo, nunca lo use en AWS.")


def cluster_key():
    return require_secret("DFSHA_CLUSTER_KEY", "dev-only-change-me")

DEFAULTS = {
    "replication": 3,            # copias objetivo por bloque
    "min_ack": 2,                # copias confirmadas mínimas para publicar (commit)
    "strict_replication": False, # True => exigir todas las copias (política de la propuesta B)
    "block_size": 8 * 1024 * 1024,
    "hb_interval": 2.0,          # heartbeat de nodos de datos (s)
    "suspect_after": 3,          # intervalos sin heartbeat => sospechoso
    "dead_after": 20.0,          # s sin heartbeat (y sonda fallida) => caído
    "blockreport_interval": 10.0,
    "retention": 360.0,          # s que se conserva una versión reemplazada / borrada (debe ser >= read_ttl, D8)
    "lease": 30.0,               # s de vida de un lock/upload sin renovar
    "read_ttl": 300.0,           # s de validez de los tokens de lectura
    "orphan_grace": 60.0,        # s antes de borrar bloques huérfanos
    "raft_timeout": 5.0,
    "controllers": [],
}


class DFSError(Exception):
    def __init__(self, status, code, msg=""):
        super().__init__(f"{status} {code}: {msg}")
        self.status, self.code, self.msg = status, code, msg


# ---------------------------------------------------------------- tokens
def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def sign(claims: dict, ttl=None, key=None) -> str:
    c = dict(claims)
    if ttl:
        c["exp"] = time.time() + ttl
    body = _b64e(json.dumps(c, separators=(",", ":"), sort_keys=True).encode())
    mac = hmac.new((key or cluster_key()).encode(), body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64e(mac)}"


def verify(token: str, key=None) -> dict:
    try:
        body, mac = token.split(".")
        good = hmac.new((key or cluster_key()).encode(), body.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(good, _b64d(mac)):
            raise ValueError("mac")
        claims = json.loads(_b64d(body))
    except Exception:
        raise DFSError(401, "BAD_TOKEN", "token inválido o alterado")
    if "exp" in claims and claims["exp"] < time.time():
        raise DFSError(401, "TOKEN_EXPIRED", "token vencido")
    return claims


def bearer(header) -> str:
    if not header or not header.lower().startswith("bearer "):
        raise DFSError(401, "NO_AUTH", "falta Authorization: Bearer")
    return header.split(" ", 1)[1].strip()


# ---------------------------------------------------------------- rutas / hashes
def norm(path: str) -> str:
    """Normaliza a ruta absoluta dentro de la raíz del usuario ('..' no puede salir de '/')."""
    p = posixpath.normpath("/" + (path or "/").replace("\\", "/"))
    return "/" + p.lstrip("/") if p.startswith("//") else p


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ---------------------------------------------------------------- config / http
def load_config(path=None) -> dict:
    cfg = dict(DEFAULTS)
    if path:
        with open(path) as f:
            cfg.update(json.load(f))
    if cfg["retention"] < cfg["read_ttl"]:  # D8: si no, el GC podría borrar bloques de un lector con token vigente
        print(f"ADVERTENCIA: retention ({cfg['retention']}s) < read_ttl ({cfg['read_ttl']}s): "
              "un lector con token vigente puede recibir 410 VERSION_EXPIRED (solo aceptable en pruebas)", flush=True)
    return cfg


_tls = threading.local()


class _Session(requests.Session):
    """requests ignora Session.verify si hay REQUESTS_CA_BUNDLE en el entorno: se fuerza la CA del clúster."""

    def request(self, *a, **kw):
        ca = os.environ.get("DFSHA_CA")
        if ca and "verify" not in kw:
            kw["verify"] = ca
        return super().request(*a, **kw)


def http() -> requests.Session:
    """Sesión requests por hilo (pool de conexiones), con verificación TLS contra la CA del clúster."""
    s = getattr(_tls, "s", None)
    if s is None:
        s = _Session()
        ad = HTTPAdapter(pool_connections=16, pool_maxsize=32)
        s.mount("http://", ad)
        s.mount("https://", ad)
        _tls.s = s
    return s


def svc_headers(node_id="svc"):
    return {"Authorization": "Bearer " + sign({"typ": "svc", "sub": node_id}, ttl=120)}
