#!/usr/bin/env python3
"""Medición de rendimiento contra un clúster DFSha ya desplegado (se ejecuta en la VM cliente).

Mide put y get (mediana y p95) para varios tamaños de bloque y números de clientes simultáneos, con N repeticiones,
verifica el hash de cada archivo recuperado y escribe un CSV. Es la versión "contra un clúster real" de t_rendimiento.

  export DFSHA_CONTROLLERS=https://10.0.1.11:8001,https://10.0.1.12:8001,https://10.0.1.13:8001
  export DFSHA_CA=/opt/dfsha/certs/ca.pem
  python3 scripts/aws_bench.py --user ana --password clave123 --size-mib 64 --reps 5 --out results/aws_perf.csv
"""
import argparse
import csv
import hashlib
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from dfsha.client import DFSClient  # noqa: E402
from dfsha.common import DFSError  # noqa: E402

MiB = 1 << 20


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p / 100 * len(xs) + 0.5)) - 1))]


def sha_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--size-mib", type=int, default=64)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--blocks", default="1,4,8", help="tamaños de bloque en MiB, separados por coma (1 cliente)")
    ap.add_argument("--clients", default="1,2,4", help="nº de clientes simultáneos (con bloque de --clients-block MiB)")
    ap.add_argument("--clients-block", type=int, default=8)
    ap.add_argument("--out", default="aws_perf.csv")
    ap.add_argument("--dir", default="/tmp/dfsha_bench")
    a = ap.parse_args()

    controllers = [c for c in os.environ.get("DFSHA_CONTROLLERS", "").split(",") if c]
    if not controllers:
        sys.exit("Defina DFSHA_CONTROLLERS (URLs de los controladores separadas por coma)")
    ca = os.environ.get("DFSHA_CA")
    os.makedirs(a.dir, exist_ok=True)

    def new_client():
        c = DFSClient(controllers, ca=ca)
        try:
            c.login(a.user, a.password)
        except DFSError:
            c.register(a.user, a.password)
            c.login(a.user, a.password)
        return c

    src = os.path.join(a.dir, "origen.bin")
    with open(src, "wb") as f:
        for _ in range(a.size_mib):
            f.write(os.urandom(MiB))
    want = sha_file(src)
    size = a.size_mib * MiB

    cases = [(f"bloque={b}MiB, 1 cliente", 1, int(b)) for b in a.blocks.split(",")]
    cases += [(f"bloque={a.clients_block}MiB, {n} clientes", int(n), a.clients_block) for n in a.clients.split(",") if int(n) > 1]
    rows = []
    for label, ncl, bs in cases:
        puts, gets, wall_agg = [], [], []
        for rep in range(a.reps):
            cs = [new_client() for _ in range(ncl)]
            res, errs = [None] * ncl, []

            def work(i):
                name = f"/bench_{bs}_{ncl}_{rep}_{i}.bin"
                out = os.path.join(a.dir, f"salida_{i}.bin")
                try:
                    t0 = time.time()
                    cs[i].put(src, name, block_size=bs * MiB)
                    t1 = time.time()
                    cs[i].get(name, out)
                    t2 = time.time()
                    if sha_file(out) != want:
                        errs.append(f"hash distinto en {name}")
                    res[i] = (t1 - t0, t2 - t1)
                    cs[i].rm(name)
                except Exception as e:  # noqa: BLE001
                    errs.append(repr(e))

            T0 = time.time()
            ts = [threading.Thread(target=work, args=(i,)) for i in range(ncl)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            wall = time.time() - T0
            if errs:
                sys.exit(f"FALLO en '{label}' repetición {rep}: {errs[0]}")
            for p, g in res:
                puts.append(p)
                gets.append(g)
            wall_agg.append(2 * ncl * size / MiB / wall)
        row = {"condicion": label, "clientes": ncl, "bloque_MiB": bs, "archivo_MiB": a.size_mib, "repeticiones": a.reps,
               "put_mediana_s": round(statistics.median(puts), 2), "put_p95_s": round(pct(puts, 95), 2),
               "get_mediana_s": round(statistics.median(gets), 2), "get_p95_s": round(pct(gets, 95), 2),
               "throughput_put_MiBs": round(a.size_mib / statistics.median(puts), 1),
               "throughput_get_MiBs": round(a.size_mib / statistics.median(gets), 1),
               "throughput_agregado_MiBs": round(statistics.median(wall_agg), 1)}
        rows.append(row)
        print(row, flush=True)

    c = new_client()
    c.put(src, "/bench_meta.bin", block_size=1 * MiB)
    lat = []
    for _ in range(60):
        t0 = time.time()
        c.stat("/bench_meta.bin")
        lat.append((time.time() - t0) * 1000)
    c.rm("/bench_meta.bin")
    print(f"stat: mediana {statistics.median(lat):.1f} ms, p95 {pct(lat, 95):.1f} ms")

    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("CSV en", a.out)


if __name__ == "__main__":
    main()
