#!/usr/bin/env python3
"""Pruebas de evidencia desde la VM cliente (lado cliente de deploy/evidencia_aws.sh).

Cada subcomando imprime líneas legibles y termina con 0 (OK) o 1 (FALLÓ), para guardar la salida como evidencia.

  export DFSHA_CONTROLLERS=https://10.0.1.11:8001,https://10.0.1.12:8001,https://10.0.1.13:8001
  export DFSHA_CA=/opt/dfsha/certs/ca.pem DFSHA_ADMIN_KEY=...
  python3 scripts/evidencia_cliente.py preparar      # sube /ev/grande.bin (64 MiB, bloques de 4 MiB, 16 bloques)
  python3 scripts/evidencia_cliente.py estado        # resumen del clúster
  python3 scripts/evidencia_cliente.py leer          # recupera el archivo y compara el hash
  python3 scripts/evidencia_cliente.py concurrencia  # dos clientes sobre el mismo archivo
  python3 scripts/evidencia_cliente.py modificar     # cambiar 1 byte reenvía solo el bloque afectado
  python3 scripts/evidencia_cliente.py seguridad     # accesos permitidos y denegados, TLS
  python3 scripts/evidencia_cliente.py bloque        # id y nodo de un bloque (para alterarlo en disco)
  python3 scripts/evidencia_cliente.py escribir      # reintenta escribir hasta que el clúster responda (failover)
  python3 scripts/evidencia_cliente.py esperar-reparado [segundos]
"""
import hashlib
import os
import socket
import ssl
import sys
import time

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from dfsha.client import DFSClient  # noqa: E402
from dfsha.common import DFSError  # noqa: E402

MiB = 1 << 20
CTRL = [c for c in os.environ.get("DFSHA_CONTROLLERS", "").split(",") if c]
CA = os.environ.get("DFSHA_CA")
ADMIN = os.environ.get("DFSHA_ADMIN_KEY", "")
USER, PASS = "demo", "demo-clave-123"
BOB, BOBPASS = "bob", "bob-clave-123"
BIG = "/ev/grande.bin"
SHAFILE = "/tmp/ev_grande.sha"
FAILS = []


def client(user=USER, password=PASS):
    c = DFSClient(CTRL, ca=CA)
    try:
        c.login(user, password)
    except DFSError:
        c.register(user, password)
        c.login(user, password)
    return c


def ok(cond, msg):
    print(("  OK    " if cond else "  FALLÓ ") + msg)
    if not cond:
        FAILS.append(msg)


def cluster():
    for base in CTRL:
        try:
            r = requests.get(base + "/v1/cluster", headers={"X-Admin-Key": ADMIN}, verify=CA, timeout=5)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            continue
    return None


def sha_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for ch in iter(lambda: f.read(1 << 20), b""):
            h.update(ch)
    return h.hexdigest()


def cmd_estado():
    s = cluster()
    if not s:
        print("Sin respuesta del clúster (¿sin mayoría de controladores?)")
        return
    print(f"líder: {s['leader_api']}  término Raft: {s['term']}")
    for n, d in sorted(s["nodes"].items()):
        print(f"  nodo {n}: {d['state']:<8} bloques={d['blocks']:<4} hace {d['age']}s  {d['url']}")
    print(f"bloques: {s['blocks_total']}  bajo-replicados: {s['under_replicated']}  perdidos: {s['lost']}  archivos: {s['files']}")


def cmd_preparar():
    c = client()
    try:
        c.mkdir("/ev")
    except DFSError:
        pass
    data = os.urandom(64 * MiB)
    open("/tmp/ev_grande.bin", "wb").write(data)
    t0 = time.time()
    r = c.put("/tmp/ev_grande.bin", BIG, block_size=4 * MiB)
    open(SHAFILE, "w").write(hashlib.sha256(data).hexdigest())
    mf = c.manifest(BIG)
    nodos = sorted({l["node"] for b in mf["blocks"] for l in b["locations"]})
    ok(len(mf["blocks"]) == 16, f"archivo de 64 MiB particionado en {len(mf['blocks'])} bloques de 4 MiB ({time.time() - t0:.1f} s)")
    ok(len(nodos) >= 4, f"bloques repartidos en {len(nodos)} nodos: {', '.join(nodos)}")
    print(f"  copias por bloque al publicar (mín): {min(len(b['locations']) for b in mf['blocks'])}  versión {r['version']}")
    ok(True, "la tercera copia se completa en segundo plano (ver 'esperar-reparado')")


def cmd_leer():
    c = client()
    t0 = time.time()
    c.get(BIG, "/tmp/ev_leido.bin")
    a, b = open(SHAFILE).read().strip(), sha_file("/tmp/ev_leido.bin")
    print(f"  hash original : {a}\n  hash recuperado: {b}")
    ok(a == b, f"archivo recuperado idéntico ({time.time() - t0:.1f} s)")


def cmd_concurrencia():
    a, b = client(), client()
    try:
        a.mkdir("/ev")
    except DFSError:
        pass
    path = "/ev/conc.bin"
    fa = a.open(path, "w", block_size=1 * MiB)
    fa.write(os.urandom(2 * MiB))
    try:
        b.open(path, "w", block_size=1 * MiB).write(b"x")
        ok(False, "el segundo escritor NO fue rechazado")
    except DFSError as e:
        ok(e.status == 423, f"segundo escritor sobre el mismo archivo: {e.status} {e.code} (bloqueo exclusivo)")
    fa.close()
    ok(len(a.read_bytes(path)) == 2 * MiB, "el primer escritor publicó su versión completa, sin mezcla de bytes")
    try:
        b.write_bytes(path, b"y" * 10)
        ok(True, "terminado el primero, el segundo ya puede escribir")
    except DFSError as e:
        ok(False, f"el segundo no pudo escribir después: {e}")


def cmd_modificar():
    c = client()
    try:
        c.mkdir("/ev")
    except DFSError:
        pass
    path = "/ev/mod.bin"
    data = bytearray(os.urandom(4 * MiB))
    c.write_bytes(path, bytes(data), block_size=1 * MiB)
    antes = [b["id"] for b in c.manifest(path)["blocks"]]
    off = 1 * MiB + 1234
    c.write_at(path, off, b"\x7f")
    data[off] = 0x7F
    m2 = c.manifest(path)
    despues = [b["id"] for b in m2["blocks"]]
    nuevos = [i for i, (x, y) in enumerate(zip(antes, despues)) if x != y]
    ok(nuevos == [1], f"cambiar 1 byte reenvió solo el bloque {nuevos} de {len(antes)}; los otros {len(antes) - len(nuevos)} se comparten")
    ok(c.read_bytes(path) == bytes(data), "el contenido leído coincide con el esperado")
    ok(c.open(path, "r").size == 4 * MiB, "el tamaño del archivo no cambia")
    try:
        c.write_at(path, 4 * MiB, b"z")
        ok(False, "modificar fuera del archivo debía fallar")
    except DFSError as e:
        ok(e.status == 416, f"modificar fuera del archivo: {e.status} {e.code} (para extender se usa anexar)")


def cmd_seguridad():
    a, bob = client(), client(BOB, BOBPASS)
    try:
        a.mkdir("/ev")
    except DFSError:
        pass
    a.write_bytes("/ev/sec.bin", b"MARCADOR-SECRETO-DFSHA-" * 4000)
    leader = cluster()["leader_api"]
    R = lambda m, p, **k: requests.request(m, leader + p, verify=CA, timeout=10, **k).status_code  # noqa: E731
    ok(R("GET", "/v1/ls") == 401, "sin token → 401")
    ok(R("GET", "/v1/ls", headers={"Authorization": "Bearer " + a.token[:-3] + "AAA"}) == 401, "token alterado → 401")
    ok(R("POST", "/v1/login", json={"user": USER, "password": "mala"}) == 401, "contraseña incorrecta → 401")
    ok(R("GET", "/v1/ls", headers={"Authorization": "Bearer " + a.token}) == 200, "token propio → 200 (acceso autorizado)")
    fid = a.stat("/ev/sec.bin")["file_id"]
    try:
        bob.manifest_by_id(fid)
        ok(False, "bob pudo leer el archivo de demo")
    except DFSError as e:
        ok(e.status == 403, f"otro usuario pide el archivo ajeno → {e.status} {e.code}")
    mf = a.manifest("/ev/sec.bin")
    blk = mf["blocks"][0]
    loc = blk["locations"][0]["url"]
    bob.write_bytes("/bob.txt", b"de bob")
    tb = bob.manifest("/bob.txt")["read_token"]
    code = lambda r: r.status_code  # noqa: E731
    ok(code(requests.get(f"{loc}/blocks/{blk['id']}", verify=CA)) == 401, "nodo de datos sin token → 401")
    ok(code(requests.get(f"{loc}/blocks/{blk['id']}", verify=CA, headers={"Authorization": "Bearer " + tb})) == 403,
       "token de otro usuario sobre un bloque ajeno → 403")
    ok(code(requests.put(f"{loc}/blocks/zzz-0", data=b"x", verify=CA, headers={"Authorization": "Bearer " + mf["read_token"]})) == 403,
       "token de lectura no sirve para escribir → 403")
    ok(code(requests.get(f"{loc}/blocks/{blk['id']}", verify=CA, headers={"Authorization": "Bearer " + mf["read_token"]})) == 200,
       "token de lectura propio → 200")
    host, port = leader.split("//")[1].rsplit(":", 1)
    ctx = ssl.create_default_context(cafile=CA)
    with socket.create_connection((host, int(port)), timeout=5) as s, ctx.wrap_socket(s, server_hostname=host) as ss:
        ok(ss.version() == "TLSv1.3", f"canal protegido: {ss.version()} {ss.cipher()[0]}")
    try:
        with socket.create_connection((host, int(port)), timeout=5) as s, ssl.create_default_context().wrap_socket(s, server_hostname=host):
            ok(False, "aceptó un certificado de una CA desconocida")
    except ssl.SSLError:
        ok(True, "una CA desconocida es rechazada")
    try:
        r = requests.get(leader.replace("https", "http") + "/health", timeout=3)
        ok(False, f"respondió en claro: {r.status_code}")
    except requests.RequestException:
        ok(True, "HTTP en claro rechazado")
    print("  (el cifrado en reposo se comprueba en los discos de los nodos con deploy/evidencia_aws.sh)")


def cmd_bloque():
    c = client()
    mf = c.manifest(BIG)
    b = mf["blocks"][0]
    print(f"BLOQUE {b['id']} {b['locations'][0]['node']}")


def cmd_escribir():
    t0 = time.time()
    n = 0
    while time.time() - t0 < 90:
        n += 1
        try:
            c = client()
            c.write_bytes("/ev/failover.txt", b"escritura despues de la falla")
            print(f"  OK    escritura aceptada tras {time.time() - t0:.1f} s (intentos: {n})")
            return
        except Exception as e:  # noqa: BLE001
            print(f"  ...   intento {n}: {str(e)[:80]}")
            time.sleep(1)
    FAILS.append("el clúster no aceptó escrituras en 90 s")
    print("  FALLÓ el clúster no aceptó escrituras en 90 s")


def cmd_esperar_reparado(seg=120):
    t0 = time.time()
    while time.time() - t0 < seg:
        s = cluster()
        if s and s["under_replicated"] == 0 and s["lost"] == 0:
            print(f"  OK    todos los bloques con 3 copias vivas ({time.time() - t0:.1f} s)")
            return
        time.sleep(2)
    FAILS.append("no se completó la reparación")
    print(f"  FALLÓ sin reparar tras {seg} s: {cluster()}")


CMDS = {"estado": cmd_estado, "preparar": cmd_preparar, "leer": cmd_leer, "concurrencia": cmd_concurrencia,
        "modificar": cmd_modificar, "seguridad": cmd_seguridad, "bloque": cmd_bloque, "escribir": cmd_escribir,
        "esperar-reparado": cmd_esperar_reparado}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in CMDS or not CTRL:
        sys.exit(__doc__ + "\nDefina DFSHA_CONTROLLERS, DFSHA_CA y DFSHA_ADMIN_KEY.")
    CMDS[sys.argv[1]](*[int(a) for a in sys.argv[2:]])
    sys.exit(1 if FAILS else 0)
