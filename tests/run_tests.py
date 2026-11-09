#!/usr/bin/env python3
"""Pruebas end-to-end de DFSha (cubren las 7 pruebas mínimas del enunciado + extras).

Local (procesos separados + TLS, sockets reales):   python tests/run_tests.py
Contra un clúster ya desplegado (p. ej. AWS):       python tests/run_tests.py --remote cluster-remote.json
   (el JSON de --remote define controllers, ca, y 'data_dirs'/'ssh' si se quiere inspeccionar discos; las pruebas
    que matan procesos solo corren en modo local o con un hook de fallos propio -> ver README)

Salida: results/report.md, results/perf.csv  y código de salida != 0 si algo falla.
"""
import argparse
import hashlib
import json
import math
import os
import ssl
import socket
import statistics
import sys
import threading
import time
import traceback

os.environ.setdefault("DFSHA_CLUSTER_KEY", "test-cluster-key")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import requests  # noqa: E402

from dfsha.client import DFSClient  # noqa: E402
from dfsha.common import DFSError, sign  # noqa: E402
from local_cluster import LocalCluster  # noqa: E402

MiB = 1024 * 1024
RESULTS = []


class Ctx:
    pass


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def rand_file(path, size):
    with open(path, "wb") as f:
        left = size
        while left:
            n = min(left, 8 * MiB)
            f.write(os.urandom(n))
            left -= n
    return path


def sha_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for ch in iter(lambda: f.read(1 << 20), b""):
            h.update(ch)
    return h.hexdigest()


def client(ctx, user="ana", login=True):
    c = DFSClient(ctx.cl.controller_urls, ca=ctx.cl.ca)
    if login:
        c.login(user, "clave-" + user)
    return c


def raises(code, fn, *a, **kw):
    try:
        fn(*a, **kw)
    except DFSError as e:
        check(e.code == code, f"se esperaba {code}, llegó {e.code}")
        return e
    raise AssertionError(f"se esperaba {code} pero la operación tuvo éxito")


def block_files(ctx):
    out = {}
    for i in range(ctx.cl.n):
        d = os.path.join(ctx.cl.base, f"data{i+1}", "blocks")
        out[f"n{i+1}"] = [os.path.join(r, f) for r, _, fs in os.walk(d) for f in fs]
    return out


def n_block_files(ctx):
    return sum(len(v) for v in block_files(ctx).values())


def holders(mf):
    """nodo -> nº de bloques del manifiesto que tiene"""
    h = {}
    for b in mf["blocks"]:
        for l in b["locations"]:
            h[l["node"]] = h.get(l["node"], 0) + 1
    return h


# =============================================================================================
def t_setup(ctx):
    for u in ("ana", "bob"):
        c = DFSClient(ctx.cl.controller_urls, ca=ctx.cl.ca)
        c.register(u, "clave-" + u)
    ctx.ana, ctx.bob = client(ctx, "ana"), client(ctx, "bob")
    st = ctx.cl.status()
    return {"controladores": 3, "nodos_de_datos": len(st["nodes"]), "lider": st["leader_api"]}


def t_rf1(ctx):
    a, b = ctx.ana, ctx.bob
    a.mkdir("/proyectos"); a.mkdir("/proyectos/2026")
    raises("EXISTS", a.mkdir, "/proyectos")
    raises("NO_PARENT", a.mkdir, "/no/existe/x")
    a.cd("/proyectos")
    a.write_bytes("informe.txt", b"hola mundo")           # ruta relativa al cwd
    names = sorted(e["name"] for e in a.ls("."))
    check(names == ["2026", "informe.txt"], f"ls devolvió {names}")
    raises("NOT_EMPTY", a.rmdir, "/proyectos")
    a.rm("informe.txt"); a.rmdir("2026")
    raises("NOT_FOUND", a.stat, "informe.txt")
    a.cd("/")
    # aislamiento: cada usuario ve solo su raíz; '..' no escapa de ella
    a.write_bytes("/secreto_ana.txt", b"solo de ana")
    check(all(e["name"] != "secreto_ana.txt" for e in b.ls("/")), "bob ve archivos de ana")
    raises("NOT_FOUND", b.stat, "/../../secreto_ana.txt")
    check(b.cd("../..") == "/", "'..' escapó de la raíz")
    return {"operaciones": "mkdir, cd, ls, rmdir, rm, stat", "aislamiento_entre_usuarios": "ok"}


def t_carga_recuperacion(ctx):
    a = ctx.ana
    src, dst = "/tmp/dfsha_t_big.bin", "/tmp/dfsha_t_big.out"
    rand_file(src, 64 * MiB)
    r = a.put(src, "/big.bin", block_size=4 * MiB)
    mf = a.manifest("/big.bin")
    h = holders(mf)
    reps = [len(b["locations"]) for b in mf["blocks"]]
    g = a.get("/big.bin", dst)
    check(sha_file(src) == sha_file(dst), "el archivo recuperado NO es idéntico")
    check(len(mf["blocks"]) == 16, f"se esperaban 16 bloques, hay {len(mf['blocks'])}")
    check(len(h) >= 3, f"bloques en solo {len(h)} nodos")
    check(min(reps) >= 2, "algún bloque con <2 copias")
    # distintos nodos por bloque
    for b in mf["blocks"]:
        ns = [l["node"] for l in b["locations"]]
        check(len(ns) == len(set(ns)), "dos copias del mismo bloque en el mismo nodo")
    return {"tamano_MiB": 64, "bloques": len(mf["blocks"]), "nodos_con_bloques": len(h),
            "bloques_por_nodo": h, "copias_por_bloque_min_max": [min(reps), max(reps)],
            "sha256_origen=destino": sha_file(src)[:16] + "…", "put_s": round(r["seconds"], 2),
            "get_s": round(g["seconds"], 2)}


def t_rf3(ctx):
    a = ctx.ana
    f = a.open("/rf3.txt", "x"); f.write(b"0123456789"); r1 = f.close()
    raises("EXISTS", a.open, "/rf3.txt", "x")
    check(r1["version"] == 1, "primera versión != 1")
    f = a.open("/rf3.txt", "a"); f.write(b"ABCDEF"); r2 = f.close()
    check(a.read_bytes("/rf3.txt") == b"0123456789ABCDEF", "append incorrecto")
    check(a.read_bytes("/rf3.txt", 8, 4) == b"89AB", "read(offset,len) incorrecto")
    rd = a.open("/rf3.txt", "r"); rd.seek(16)
    check(rd.read(10) == b"", "al llegar al final debe devolver 0 bytes")
    # snapshot: un lector abierto sigue viendo su versión aunque se publique otra
    rd = a.open("/rf3.txt", "r"); v_old = rd.version
    f = a.open("/rf3.txt", "w"); f.write(b"REEMPLAZO TOTAL"); r3 = f.close()
    check(rd.read() == b"0123456789ABCDEF" and rd.version == v_old, "el lector abierto vio una mezcla/otra versión")
    check(a.read_bytes("/rf3.txt") == b"REEMPLAZO TOTAL", "lectura nueva no ve la última versión")
    # escritura abortada: no publica nada, la versión anterior sigue vigente
    f = a.open("/rf3.txt", "w"); f.write(b"basura a medio escribir"); f.abort()
    check(a.read_bytes("/rf3.txt") == b"REEMPLAZO TOTAL" and a.stat("/rf3.txt")["version"] == r3["version"],
          "una escritura abortada alteró el archivo")
    # control optimista: expected_version
    raises("VERSION_CONFLICT", a.open, "/rf3.txt", "w", expected_version=1)
    # lock explícito
    l = a.lock("/rf3.txt")
    other = client(ctx, "ana")  # otro cliente (otra sesión) del mismo usuario
    raises("LOCKED", other.lock, "/rf3.txt")
    raises("LOCKED", other.open, "/rf3.txt", "w")
    a.unlock("/rf3.txt")
    other.lock("/rf3.txt"); other.unlock("/rf3.txt")
    return {"write": "crear(x)/reemplazar(w)/anexar(a); sin escritura parcial", "versiones": [1, r2["version"], r3["version"]],
            "fencing_token": l["fence"]}


def t_concurrencia(ctx):
    a = ctx.ana
    a.write_bytes("/conc.bin", b"v0")
    # (1) dos clientes abren para escribir a la vez: uno obtiene el lock, el otro 423
    c1, c2 = client(ctx, "ana"), client(ctx, "ana")
    w1 = c1.open("/conc.bin", "w")
    e = raises("LOCKED", c2.open, "/conc.bin", "w")
    w1.write(b"escritor-1"); w1.close()
    w2 = c2.open("/conc.bin", "w"); w2.write(b"escritor-2"); w2.close()
    check(a.read_bytes("/conc.bin") == b"escritor-2", "contenido final inesperado")
    # (2) carrera de 6 hilos sobre el mismo archivo: nunca se mezclan bytes; cada commit crea una versión
    wins, losses, lk = [], [], threading.Lock()

    def racer(i):
        c = client(ctx, "ana")
        payload = (f"hilo-{i}-" * 400_000).encode()[:3 * MiB]
        for _ in range(40):
            try:
                f = c.open("/conc.bin", "w", block_size=1 * MiB)
            except DFSError as ex:
                if ex.code == "LOCKED":
                    with lk:
                        losses.append(i)
                    time.sleep(0.05)
                    continue
                raise
            f.write(payload); f.close()
            with lk:
                wins.append((i, payload))
            return

    ts = [threading.Thread(target=racer, args=(i,)) for i in range(6)]
    [t.start() for t in ts]; [t.join() for t in ts]
    check(len(wins) == 6, f"solo {len(wins)}/6 escritores lograron publicar")
    final = a.read_bytes("/conc.bin")
    check(any(final == p for _, p in wins), "el archivo final es una MEZCLA de escritores")
    # (3) cliente "zombi": pierde el lease, otro escribe, y el zombi no puede publicar (fencing)
    z = client(ctx, "ana"); zf = z.open("/conc.bin", "w"); zf.write(b"zombi"); zf.done = True  # deja de renovar
    time.sleep(ctx.cl.cfg["lease"] + 2)
    n = client(ctx, "ana"); nf = n.open("/conc.bin", "w"); nf.write(b"nuevo-duenio"); nf.close()
    err = raises("LEASE_EXPIRED", zf.close)
    check(a.read_bytes("/conc.bin") == b"nuevo-duenio", "el escritor zombi pisó al nuevo dueño")
    return {"lock_segundo_escritor": e.code, "carrera_6_hilos": {"publicaron": len(wins), "reintentos_por_423": len(losses)},
            "archivo_final_integro": True, "escritor_zombi": err.code}


def t_fallos_nodo(ctx):
    a = ctx.ana
    src, dst = "/tmp/dfsha_f.bin", "/tmp/dfsha_f.out"
    rand_file(src, 48 * MiB)
    a.put(src, "/fallos.bin", block_size=2 * MiB)
    mf = a.manifest("/fallos.bin")
    victim = max(holders(mf).items(), key=lambda kv: kv[1])[0]
    t0 = time.time(); ctx.cl.kill(victim)
    g = a.get("/fallos.bin", dst)                                  # lectura inmediata con el nodo caído
    check(sha_file(src) == sha_file(dst), "lectura con nodo caído devolvió datos distintos")
    src2 = rand_file("/tmp/dfsha_f2.bin", 16 * MiB)
    p = a.put(src2, "/fallos2.bin", block_size=2 * MiB)             # escritura con nodo caído
    a.get("/fallos2.bin", "/tmp/dfsha_f2.out"); check(sha_file(src2) == sha_file("/tmp/dfsha_f2.out"), "escritura con nodo caído")
    ctx.cl.wait_for(lambda s: s["nodes"][victim]["state"] in ("SUSPECT", "DEAD"), 40)
    t_susp = time.time() - t0                                       # desde el instante de la caída
    ctx.cl.wait_for(lambda s: s["nodes"][victim]["state"] == "DEAD", 60)
    t_dead = time.time() - t0
    ctx.cl.wait_for(lambda s: s["under_replicated"] == 0 and s["lost"] == 0, 120)
    t_rep = time.time() - t0 - t_dead                               # desde que se declaró caído
    mf2 = a.manifest("/fallos.bin")
    check(all(len(b["locations"]) >= 3 for b in mf2["blocks"]), "no se restauró el factor de replicación 3")
    check(victim not in holders(mf2), "el nodo caído sigue figurando como ubicación")
    # el nodo vuelve: reconcilia su inventario y todo sigue consistente
    ctx.cl.restart(victim)
    ctx.cl.wait_for(lambda s: s["nodes"].get(victim, {}).get("state") == "ALIVE", 60)
    time.sleep(6)
    a.get("/fallos.bin", dst); check(sha_file(src) == sha_file(dst), "tras el regreso del nodo")
    st = ctx.cl.status(); check(st["lost"] == 0, "bloques perdidos")
    return {"nodo_caido": victim, "lectura_con_nodo_caido_s": round(g["seconds"], 2),
            "escritura_con_nodo_caido_s": round(p["seconds"], 2),
            "deteccion_sospechoso_desde_la_caida_s": round(t_susp, 1),
            "deteccion_caido_desde_la_caida_s": round(t_dead, 1), "reparacion_a_3_copias_tras_declararlo_caido_s": round(t_rep, 1),
            "bloques_perdidos": 0}


def t_nodo_cae_durante_escritura(ctx):
    a = ctx.ana
    src = rand_file("/tmp/dfsha_mid.bin", 48 * MiB)
    box = {}

    def up():
        try:
            box["r"] = a.put(src, "/mid.bin", block_size=1 * MiB)
        except Exception as e:
            box["e"] = e

    t = threading.Thread(target=up); t.start()
    time.sleep(0.8)
    victim = "n3" if ctx.cl.procs["n3"].poll() is None else "n2"
    ctx.cl.kill(victim)
    t.join(120)
    check("e" not in box, f"la escritura falló al caer un nodo: {box.get('e')}")
    a.get("/mid.bin", "/tmp/dfsha_mid.out"); check(sha_file(src) == sha_file("/tmp/dfsha_mid.out"), "hash tras caída durante escritura")
    mf = a.manifest("/mid.bin")
    ctx.cl.wait_for(lambda s: s["under_replicated"] == 0 and s["lost"] == 0, 120)
    ctx.cl.restart(victim)
    ctx.cl.wait_for(lambda s: s["nodes"].get(victim, {}).get("state") == "ALIVE", 60)
    return {"nodo_caido_a_mitad_de_escritura": victim, "bloques": len(mf["blocks"]),
            "copias_min_al_publicar": min(len(b["locations"]) for b in mf["blocks"]), "hash_ok": True}


def t_replica_retrasada(ctx):
    a = ctx.ana
    a.write_bytes("/lag.bin", os.urandom(6 * MiB), block_size=1 * MiB)
    v1 = a.read_bytes("/lag.bin")
    pausado = "n2"
    ctx.cl.pause(pausado)                                           # réplica "desconectada" (proceso congelado)
    new = os.urandom(6 * MiB)
    t0 = time.time(); r = a.write_bytes("/lag.bin", new, block_size=1 * MiB); dt = time.time() - t0
    mf = a.manifest("/lag.bin")
    sin_pausado = [b for b in mf["blocks"] if pausado not in [l["node"] for l in b["locations"]]]
    lectura = a.read_bytes("/lag.bin")
    check(lectura == new, "los lectores no ven la versión confirmada")
    check(lectura != v1, "los lectores ven la versión vieja")
    reps_al_publicar = min(len(b["locations"]) for b in mf["blocks"])
    check(reps_al_publicar >= 2, "se publicó con <2 copias")
    ctx.cl.resume(pausado)
    ctx.cl.wait_for(lambda s: s["under_replicated"] == 0 and s["lost"] == 0 and
                    s["nodes"][pausado]["state"] == "ALIVE", 90)
    mf2 = a.manifest("/lag.bin")
    check(a.read_bytes("/lag.bin") == new, "tras recuperar la réplica")
    return {"replica_pausada": pausado, "escritura_con_replica_caida_s": round(dt, 2), "version_publicada": r["version"],
            "copias_minimas_al_publicar": reps_al_publicar,
            "bloques_publicados_sin_la_replica_pausada": len(sin_pausado),
            "copias_minimas_tras_recuperacion": min(len(b["locations"]) for b in mf2["blocks"]),
            "lectores_vieron": "version nueva completa, nunca una mezcla"}


def t_escritura_interrumpida(ctx):
    a = ctx.ana
    # (1) un bloque que llega incompleto a un nodo es rechazado (hash/longitud no coinciden)
    f = a.open("/parcial.bin", "w", block_size=1 * MiB)
    tgt = f._targets(0)[0]
    data = os.urandom(1 * MiB)
    full_sha = hashlib.sha256(data).hexdigest()
    r = requests.put(f"{tgt['url']}/blocks/{f.id}-0", data=data[: 300 * 1024], verify=ctx.cl.ca,
                     headers={"Authorization": "Bearer " + f.token, "X-Sha256": full_sha})
    check(r.status_code == 422, f"el nodo aceptó un bloque incompleto ({r.status_code})")
    f.abort()
    # (2) el cliente "muere" tras enviar bloques sin hacer commit: el archivo NO aparece y los bloques se limpian
    f = a.open("/crash.bin", "w", block_size=1 * MiB)
    f.write(os.urandom(3 * MiB + 100)); [x.result() for x in f.futs]; f.done = True     # sin commit ni abort
    up_id = f.id
    antes = sum(1 for v in block_files(ctx).values() for p in v if os.path.basename(p).startswith(up_id))
    raises("NOT_FOUND", a.stat, "/crash.bin")
    check(antes >= 3, "no se encontraron bloques temporales de la escritura interrumpida")
    t, _ = ctx.cl.wait_for(lambda s: all(not os.path.basename(p).startswith(up_id)
                                         for v in block_files(ctx).values() for p in v), 120)
    return {"bloque_parcial_rechazado": "422 INVALID_HASH", "archivo_visible_tras_crash": False,
            "bloques_huerfanos_creados": antes, "limpiados_tras_s": round(t, 1)}


def t_gc_versiones(ctx):
    a = ctx.ana
    for i in range(3):
        a.write_bytes("/gc.bin", os.urandom(4 * MiB), block_size=1 * MiB)
    antes = n_block_files(ctx)
    time.sleep(1)
    e = None
    t, _ = ctx.cl.wait_for(lambda s: n_block_files(ctx) < antes, 90)
    try:
        a.manifest("/gc.bin", version=1)
    except DFSError as ex:
        e = ex.code
    check(e == "VERSION_EXPIRED", "la versión 1 debería estar liberada")
    # rm = borrado lógico: desaparece ya; los bloques se liberan tras la retención
    a.rm("/gc.bin")
    raises("NOT_FOUND", a.stat, "/gc.bin")
    mid = n_block_files(ctx)
    t2, _ = ctx.cl.wait_for(lambda s: n_block_files(ctx) < mid, 90)
    return {"bloques_antes": antes, "versiones_viejas_liberadas_s": round(t, 1), "version_1": e,
            "rm_logico_bloques_liberados_s": round(t2, 1)}


def t_modificacion_bloque(ctx):
    """Modificación de un rango por bloque (copy-on-write): solo se reenvían los bloques que toca el rango."""
    a, b = ctx.ana, ctx.bob
    ev = {}
    size = 4 * MiB + 512 * 1024  # 5 bloques de 1 MiB, el último de 512 KiB
    data = os.urandom(size)
    a.write_bytes("/mod.bin", data, block_size=1 * MiB)
    m1 = a.manifest("/mod.bin")
    ids = lambda m: [x["id"] for x in m["blocks"]]
    check(len(m1["blocks"]) == 5, "se esperaban 5 bloques")
    snap = a.open("/mod.bin", "r")  # lector con instantánea de la versión 1
    exp = bytearray(data)

    def cambiados(m_ant, m_nuevo):
        return [i for i, (x, y) in enumerate(zip(ids(m_ant), ids(m_nuevo))) if x != y]

    # 1) rango dentro de un solo bloque
    off, patch = 2 * MiB + 1000, os.urandom(100)
    r = a.write_at("/mod.bin", off, patch)
    exp[off:off + len(patch)] = patch
    m2 = a.manifest("/mod.bin")
    check(r["version"] == m1["version"] + 1 and m2["size"] == size, "versión/tamaño incorrectos")
    check(cambiados(m1, m2) == [2], f"solo debía cambiar el bloque 2: {cambiados(m1, m2)}")
    check(a.read_bytes("/mod.bin") == bytes(exp), "contenido distinto tras modificar un bloque")
    ev["rango_en_un_bloque"] = {"bloques_nuevos": cambiados(m1, m2), "bloques_compartidos": 4}
    # el lector que abrió antes sigue viendo la versión anterior completa (snapshot)
    check(snap.read() == data, "el snapshot dejó de ver la versión anterior")
    ev["snapshot_anterior_intacto"] = True
    # 2) rango que cruza dos bloques
    off, patch = 1 * MiB - 50, os.urandom(100)
    a.write_at("/mod.bin", off, patch)
    exp[off:off + len(patch)] = patch
    m3 = a.manifest("/mod.bin")
    check(cambiados(m2, m3) == [0, 1], f"debían cambiar los bloques 0 y 1: {cambiados(m2, m3)}")
    # 3) último bloque (más corto)
    off, patch = 4 * MiB + 10, os.urandom(20)
    a.write_at("/mod.bin", off, patch)
    exp[off:off + len(patch)] = patch
    m4 = a.manifest("/mod.bin")
    check(cambiados(m3, m4) == [4], "debía cambiar solo el último bloque")
    check(a.read_bytes("/mod.bin") == bytes(exp) and m4["size"] == size, "contenido final incorrecto")
    ev["rango_que_cruza_bloques"] = cambiados(m2, m3)
    ev["version_final"] = m4["version"]
    # 4) errores: no extiende el archivo, archivo inexistente, versión esperada obsoleta, archivo ajeno
    e = raises("RANGE_OUT_OF_FILE", a.write_at, "/mod.bin", size - 5, b"x" * 10)
    ev["fuera_de_rango"] = e.status
    check(a.manifest("/mod.bin")["version"] == m4["version"], "una modificación fallida creó una versión")
    raises("NOT_FOUND", a.write_at, "/no-existe.bin", 0, b"x")
    e = raises("VERSION_CONFLICT", a.write_at, "/mod.bin", 0, b"z", expected_version=m1["version"])
    ev["version_esperada_obsoleta"] = e.status
    raises("NOT_FOUND", b.write_at, "/mod.bin", 0, b"x")  # bob no ve el namespace de ana
    # 5) concurrencia: con una modificación abierta, otro escritor recibe LOCKED; al abortar, el lock se libera
    f = a.open("/mod.bin", "m")
    c2 = client(ctx, "ana")
    e = raises("LOCKED", c2.write_at, "/mod.bin", 0, b"y")
    ev["segundo_escritor"] = e.status
    f.abort()
    c2.write_at("/mod.bin", 0, b"Q")
    exp[0:1] = b"Q"
    m5 = a.manifest("/mod.bin")
    # 6) GC: los bloques reemplazados se liberan tras la retención; los compartidos NO se borran
    viejos = set(ids(m1)) - set(ids(m5))
    compartidos = set(ids(m5)) & set(ids(m1))
    en_disco = lambda bid: [n for n, fs in block_files(ctx).items() if any(os.path.basename(f) == bid for f in fs)]
    t0 = time.time()
    ctx.cl.wait_for(lambda s: not any(en_disco(bid) for bid in viejos), 90)
    ev["bloques_reemplazados_liberados_s"] = round(time.time() - t0, 1)
    check(all(len(en_disco(bid)) >= 2 for bid in ids(m5)), "falta una copia de un bloque vigente tras el GC")
    check(a.read_bytes("/mod.bin") == bytes(exp), "el GC dañó el contenido")
    ev["bloques_reemplazados"] = len(viejos)
    ev["bloques_compartidos_conservados"] = len(compartidos)
    a.rm("/mod.bin")
    return ev


def t_seguridad(ctx):
    a, b, cl = ctx.ana, ctx.bob, ctx.cl
    url = cl.controller_urls[0]
    ev = {}
    leader = cl.status()["leader_api"]
    R = lambda m, p, **k: requests.request(m, leader + p, verify=cl.ca, timeout=10, **k)
    ev["sin_token"] = R("GET", "/v1/ls").status_code
    tok = a.token
    ev["token_alterado"] = R("GET", "/v1/ls", headers={"Authorization": "Bearer " + tok[:-3] + "AAA"}).status_code
    ev["token_vencido"] = R("GET", "/v1/ls", headers={"Authorization": "Bearer " + sign({"typ": "user", "sub": "ana"}, ttl=-5)}).status_code
    ev["password_incorrecta"] = R("POST", "/v1/login", json={"user": "ana", "password": "mala"}).status_code
    check(ev["sin_token"] == 401 and ev["token_alterado"] == 401 and ev["token_vencido"] == 401 and ev["password_incorrecta"] == 401, str(ev))
    # autorización entre usuarios
    fid = a.stat("/big.bin")["file_id"]
    e = raises("FORBIDDEN", b.manifest_by_id, fid)
    ev["bob_lee_archivo_de_ana"] = e.status
    # bob no puede reportar como corrupto (ni retirar réplicas de) un bloque de ana; ana, como dueña, sí puede
    ablk0 = a.manifest("/big.bin")["blocks"][0]
    ev["bob_reporta_bloque_de_ana"] = requests.post(
        leader + "/v1/blocks/corrupt", json={"block_id": ablk0["id"], "node": ablk0["locations"][0]["node"]},
        verify=cl.ca, timeout=10, headers={"Authorization": "Bearer " + b.token}).status_code
    check(ev["bob_reporta_bloque_de_ana"] == 403, "bob pudo reportar un bloque ajeno: " + str(ev["bob_reporta_bloque_de_ana"]))
    check(len(a.manifest("/big.bin")["blocks"][0]["locations"]) == len(ablk0["locations"]), "se retiró una réplica ajena")
    # tokens de datos acotados
    b.write_bytes("/bob.txt", b"de bob")
    mfa, mfb = a.manifest("/big.bin"), b.manifest("/bob.txt")
    ablk = mfa["blocks"][0]; loc = ablk["locations"][0]["url"]
    ev["nodo_sin_token"] = requests.get(f"{loc}/blocks/{ablk['id']}", verify=cl.ca).status_code
    ev["token_de_bob_en_bloque_de_ana"] = requests.get(f"{loc}/blocks/{ablk['id']}", verify=cl.ca,
                                                      headers={"Authorization": "Bearer " + mfb["read_token"]}).status_code
    ev["token_de_lectura_para_escribir"] = requests.put(f"{loc}/blocks/zzz-0", data=b"x", verify=cl.ca,
                                                       headers={"Authorization": "Bearer " + mfa["read_token"]}).status_code
    ev["token_propio_ok"] = requests.get(f"{loc}/blocks/{ablk['id']}", verify=cl.ca,
                                         headers={"Authorization": "Bearer " + mfa["read_token"]}).status_code
    ev["borrar_bloque_sin_token_servicio"] = requests.delete(f"{loc}/blocks/{ablk['id']}", verify=cl.ca,
                                                             headers={"Authorization": "Bearer " + mfa["read_token"]}).status_code
    check(ev["nodo_sin_token"] == 401 and ev["token_de_bob_en_bloque_de_ana"] == 403 and
          ev["token_de_lectura_para_escribir"] == 403 and ev["token_propio_ok"] == 200 and
          ev["borrar_bloque_sin_token_servicio"] == 403, str(ev))
    # cifrado en tránsito
    host, port = "127.0.0.1", int(leader.rsplit(":", 1)[1])
    ctxs = ssl.create_default_context(cafile=cl.ca)
    with socket.create_connection((host, port), timeout=5) as s, ctxs.wrap_socket(s, server_hostname=host) as ss:
        ev["tls"] = {"version": ss.version(), "cifrado": ss.cipher()[0]}
    try:
        with socket.create_connection((host, port), timeout=5) as s, ssl.create_default_context().wrap_socket(s, server_hostname=host):
            raise AssertionError("aceptó un certificado de una CA desconocida")
    except ssl.SSLError:
        ev["ca_desconocida_rechazada"] = True
    try:
        r = requests.get(leader.replace("https", "http") + "/health", timeout=3)
        raise AssertionError(f"respondió en claro: {r.status_code}")
    except requests.RequestException:
        ev["http_en_claro_rechazado"] = True
    # cifrado en reposo: el contenido no aparece en el disco de los nodos
    marker = b"MARCADOR-SECRETO-DFSHA-" * 4000
    a.write_bytes("/marker.txt", marker)
    hits = 0
    for paths in block_files(ctx).values():
        for p in paths:
            if b"MARCADOR-SECRETO-DFSHA" in open(p, "rb").read():
                hits += 1
    check(hits == 0, "el texto plano aparece en disco")
    mfm = a.manifest("/marker.txt")["blocks"][0]
    nodo = mfm["locations"][0]["node"]
    p = [x for x in block_files(ctx)[nodo] if os.path.basename(x) == mfm["id"]][0]
    ev["en_reposo"] = {"coincidencias_texto_plano_en_discos": hits, "primeros_bytes_del_bloque": open(p, "rb").read(36).hex()}
    # integridad: un bloque alterado en disco se detecta, se descarta y se repara
    mf = a.manifest("/big.bin"); blk = mf["blocks"][0]; n0 = blk["locations"][0]["node"]
    bp = [x for x in block_files(ctx)[n0] if os.path.basename(x) == blk["id"]][0]
    raw = bytearray(open(bp, "rb").read()); raw[100] ^= 0xFF; open(bp, "wb").write(bytes(raw))
    for _ in range(6):
        a.get("/big.bin", "/tmp/dfsha_sec.out"); check(sha_file("/tmp/dfsha_t_big.bin") == sha_file("/tmp/dfsha_sec.out"), "lectura con bloque alterado")
    t, _ = ctx.cl.wait_for(lambda s: s["under_replicated"] == 0, 90)
    ev["bloque_alterado"] = {"nodo": n0, "lecturas_correctas": 6, "reparado_en_s": round(t, 1)}
    return ev


def t_failover_controlador(ctx):
    a = ctx.ana
    st = ctx.cl.status()
    lider = f"c{ctx.cl.controller_urls.index(st['leader_api']) + 1}"
    t0 = time.time(); ctx.cl.kill(lider)
    a.mkdir("/tras_failover")                                      # el cliente reintenta contra otro controlador
    dt = time.time() - t0
    a.write_bytes("/tras_failover/x.txt", b"sigue funcionando")
    check(a.read_bytes("/big.bin", 0, 10) is not None, "lectura tras failover")
    nuevo = ctx.cl.status()["leader_api"]
    check(nuevo != st["leader_api"], "no cambió el líder")
    # perder la mayoría (2 de 3): se detienen los commits (consistencia sobre disponibilidad)
    otro = f"c{ctx.cl.controller_urls.index(nuevo) + 1}"
    ctx.cl.kill(otro)
    e = None
    try:
        a._ctl("POST", "/v1/mkdir", {"path": "/sin_quorum"}, retry=4.0)
    except DFSError as ex:
        e = ex
    check(e is not None and e.status == 503, "se aceptó un commit sin mayoría de controladores")
    ctx.cl.restart(lider); ctx.cl.restart(otro)
    t1 = time.time()
    a.mkdir("/recuperado")
    rec = time.time() - t1
    names = {x["name"] for x in a.ls("/")}
    check({"tras_failover", "recuperado", "big.bin"} <= names, "se perdieron metadatos tras reiniciar controladores")
    return {"lider_caido": lider, "failover_hasta_primera_escritura_ok_s": round(dt, 1),
            "sin_mayoria": f"{e.status} {e.code}", "recuperacion_con_2_de_3_s": round(rec, 1),
            "metadatos_conservados": True}


def pct(vals, p):
    s = sorted(vals)
    return s[min(len(s) - 1, max(0, math.ceil(p / 100 * len(s)) - 1))]


def t_rendimiento(ctx, reps=3, size=24 * MiB):
    rows = []
    cases = [("bloque=1MiB, 1 cliente", 1, 1), ("bloque=4MiB, 1 cliente", 1, 4), ("bloque=8MiB, 1 cliente", 1, 8),
             ("bloque=4MiB, 2 clientes", 2, 4), ("bloque=4MiB, 4 clientes", 4, 4)]
    src = rand_file("/tmp/dfsha_perf.bin", size)
    for label, nclients, bs in cases:
        put_t, get_t, agg = [], [], []
        for rep in range(reps):
            cs = [client(ctx, "ana") for _ in range(nclients)]
            res = [None] * nclients

            def work(i):
                t0 = time.time(); cs[i].put(src, f"/perf_{label[:9]}_{nclients}_{rep}_{i}.bin", block_size=bs * MiB)
                t1 = time.time(); cs[i].get(f"/perf_{label[:9]}_{nclients}_{rep}_{i}.bin", f"/tmp/dfsha_perf_{i}.out")
                res[i] = (t1 - t0, time.time() - t1)

            T0 = time.time()
            ts = [threading.Thread(target=work, args=(i,)) for i in range(nclients)]
            [t.start() for t in ts]; [t.join() for t in ts]
            wall = time.time() - T0
            for pt, gt in res:
                put_t.append(pt); get_t.append(gt)
            agg.append(2 * nclients * size / MiB / wall)
        rows.append({"condicion": label, "clientes": nclients, "bloque_MiB": bs, "archivo_MiB": size // MiB, "repeticiones": reps,
                     "put_mediana_s": round(statistics.median(put_t), 2), "put_p95_s": round(pct(put_t, 95), 2),
                     "get_mediana_s": round(statistics.median(get_t), 2), "get_p95_s": round(pct(get_t, 95), 2),
                     "throughput_put_MiBs": round(size / MiB / statistics.median(put_t), 1),
                     "throughput_get_MiBs": round(size / MiB / statistics.median(get_t), 1)})
    # latencia de operaciones de metadatos
    a = ctx.ana; lat = []
    for i in range(60):
        t0 = time.time(); a.stat("/big.bin"); lat.append((time.time() - t0) * 1000)
    meta = {"stat_mediana_ms": round(statistics.median(lat), 1), "stat_p95_ms": round(pct(lat, 95), 1)}
    ctx.perf = rows
    return {"nota": "máquina local de 1 vCPU, todo en localhost: NO es representativo de AWS", "metadatos": meta,
            "mejor_put_MiBs": max(r["throughput_put_MiBs"] for r in rows)}


# =============================================================================================
TESTS = [
    ("Preparación del clúster", t_setup),
    ("RF1  Sistema de archivos jerárquico y aislamiento", t_rf1),
    ("P1   Carga y recuperación (64 MiB, particionado, hash idéntico)", t_carga_recuperacion),
    ("P2   Acceso RF3: open/read/write/close/lock", t_rf3),
    ("P3   Concurrencia: lock, carrera de escritores, fencing", t_concurrencia),
    ("P4   Replicación y fallos: caída de un nodo", t_fallos_nodo),
    ("P4b  Nodo cae a mitad de una escritura", t_nodo_cae_durante_escritura),
    ("P5   Consistencia: réplica desconectada durante escritura", t_replica_retrasada),
    ("E3   Escritura interrumpida / bloque parcial", t_escritura_interrumpida),
    ("GC   Versiones, tombstones y liberación de bloques", t_gc_versiones),
    ("P8   Modificación de un rango por bloque (copy-on-write)", t_modificacion_bloque),
    ("P6   Seguridad: authn/authz, TLS, cifrado en reposo, integridad", t_seguridad),
    ("P7   Rendimiento (≥2 condiciones)", t_rendimiento),
    ("HA   Failover de controladores y pérdida de mayoría", t_failover_controlador),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="subcadena del nombre de la prueba")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--dir", default="/tmp/dfsha-tests")
    a = ap.parse_args()
    import shutil
    shutil.rmtree(a.dir, ignore_errors=True)
    ctx = Ctx()
    ctx.cl = LocalCluster(a.dir, 4, overrides={"block_size": 4 * MiB, "dead_after": 12.0, "retention": 8.0,
                                               "orphan_grace": 6.0, "blockreport_interval": 4.0, "lease": 8.0}).start()
    ctx.cl.cfg.update({"lease": 8.0})
    ctx.perf = []
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    fails = 0
    try:
        for name, fn in TESTS:
            if a.only and a.only.lower() not in name.lower() and fn not in (t_setup, t_carga_recuperacion):  # P1 crea /big.bin que otras pruebas usan
                continue
            t0 = time.time()
            try:
                ev = fn(ctx)
                RESULTS.append((name, "PASS", time.time() - t0, ev))
                print(f"[PASS] {name}  ({time.time()-t0:.1f}s)\n       {json.dumps(ev, ensure_ascii=False)[:600]}", flush=True)
            except Exception as e:
                fails += 1
                RESULTS.append((name, "FAIL", time.time() - t0, {"error": repr(e), "trace": traceback.format_exc()[-800:]}))
                print(f"[FAIL] {name}: {e!r}\n{traceback.format_exc()[-900:]}", flush=True)
    finally:
        if not a.keep:
            ctx.cl.stop()
    # informe
    with open(os.path.join(ROOT, "results", "report.md"), "w") as f:
        f.write("# Resultados de pruebas DFSha\n\nEntorno: clúster local (3 controladores + 4 nodos de datos, procesos "
                "separados, TLS) generado con `tests/run_tests.py`.\n\n| Prueba | Resultado | Tiempo |\n|---|---|---|\n")
        for n, s, t, ev in RESULTS:
            f.write(f"| {n} | {s} | {t:.1f}s |\n")
        f.write("\n## Evidencias\n")
        for n, s, t, ev in RESULTS:
            f.write(f"\n### {n} — {s}\n```json\n{json.dumps(ev, indent=2, ensure_ascii=False)}\n```\n")
    if ctx.perf:
        import csv
        with open(os.path.join(ROOT, "results", "perf.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(ctx.perf[0])); w.writeheader(); w.writerows(ctx.perf)
    print(f"\n{len(RESULTS) - fails}/{len(RESULTS)} pruebas OK")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
