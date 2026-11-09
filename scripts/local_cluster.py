#!/usr/bin/env python3
"""Clúster local para desarrollo y pruebas: 3 controladores + N nodos de datos, procesos separados,
TLS activado y comunicación por sockets TCP reales (el despliegue en AWS usa los mismos binarios).

  python scripts/local_cluster.py up --dir /tmp/dfsha --nodes 4
"""
import json
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class LocalCluster:
    def __init__(self, base="/tmp/dfsha-local", n_nodes=4, tls=True, overrides=None, env=None):
        self.base, self.n, self.tls = base, n_nodes, tls
        self.scheme = "https" if tls else "http"
        self.procs, self.logs = {}, {}
        self.env = {**os.environ, "PYTHONPATH": ROOT, "DFSHA_CLUSTER_KEY": "test-cluster-key",
                    "DFSHA_ADMIN_KEY": "admin-dev", "DFSHA_NODE_KEY": "test-node-key", **(env or {})}
        self.cfg = {"controllers": [{"id": f"c{i+1}", "raft": f"127.0.0.1:{4321+i}",
                                     "api": f"{self.scheme}://127.0.0.1:{8001+i}"} for i in range(3)],
                    **(overrides or {})}
        self.controller_urls = [c["api"] for c in self.cfg["controllers"]]
        self.ca = os.path.join(base, "certs", "ca.pem")

    def _spawn(self, name, args, extra_env=None):
        log = open(os.path.join(self.base, "logs", name + ".log"), "ab")
        env = {**self.env, **(extra_env or {})}
        if self.tls:
            env.update(DFSHA_CA=self.ca, DFSHA_TLS_CERT=os.path.join(self.base, "certs", "server.pem"),
                       DFSHA_TLS_KEY=os.path.join(self.base, "certs", "server.key"))
        self.procs[name] = subprocess.Popen([sys.executable, "-m"] + args, env=env, stdout=log, stderr=log,
                                            cwd=ROOT, start_new_session=True)

    def start(self):
        os.makedirs(os.path.join(self.base, "logs"), exist_ok=True)
        if self.tls and not os.path.exists(self.ca):
            subprocess.check_call([sys.executable, os.path.join(ROOT, "scripts", "gen_certs.py"),
                                   os.path.join(self.base, "certs"), "127.0.0.1", "localhost"], stdout=subprocess.DEVNULL)
        cfgp = os.path.join(self.base, "cluster.json")
        json.dump(self.cfg, open(cfgp, "w"), indent=1)
        self.cfgp = cfgp
        for i in range(3):
            self.start_controller(i)
        for i in range(self.n):
            self.start_node(i)
        self.wait_ready()
        return self

    def start_controller(self, i):
        self._spawn(f"c{i+1}", ["dfsha.controller", "--config", self.cfgp, "--id", f"c{i+1}",
                                "--listen", f"127.0.0.1:{8001+i}"],
                    {"DFSHA_STATE_DIR": os.path.join(self.base, "state")})

    def start_node(self, i):
        d = os.path.join(self.base, f"data{i+1}")
        os.makedirs(d, exist_ok=True)
        self._spawn(f"n{i+1}", ["dfsha.datanode", "--config", self.cfgp, "--id", f"n{i+1}",
                                "--listen", f"127.0.0.1:{9001+i}", "--advertise",
                                f"{self.scheme}://127.0.0.1:{9001+i}", "--data", d])

    # ------------------------------------------------------------ control de procesos (inyección de fallos)
    def kill(self, name):  # SIGKILL: caída abrupta
        p = self.procs.get(name)
        if p and p.poll() is None:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()

    def pause(self, name):  # SIGSTOP: nodo "colgado"/desconectado (en AWS: docker pause o desconectar la ENI)
        os.killpg(self.procs[name].pid, signal.SIGSTOP)

    def resume(self, name):
        os.killpg(self.procs[name].pid, signal.SIGCONT)

    def restart(self, name):
        self.kill(name)
        kind, i = name[0], int(name[1:]) - 1
        (self.start_controller if kind == "c" else self.start_node)(i)

    def stop(self):
        for n, p in self.procs.items():
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGCONT)
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    # ------------------------------------------------------------ estado
    def status(self):
        import requests
        for base in self.controller_urls:
            try:
                r = requests.get(base + "/v1/cluster", headers={"X-Admin-Key": "admin-dev"},
                                 verify=self.ca if self.tls else True, timeout=3)
                if r.status_code == 200:
                    return r.json()
            except Exception:
                pass
        return None

    def wait_ready(self, timeout=60, nodes=None, wait_replicated=False):
        want = nodes if nodes is not None else self.n
        t0 = time.time()
        while time.time() - t0 < timeout:
            st = self.status()
            if st and sum(1 for n in st["nodes"].values() if n["state"] == "ALIVE") >= want:
                return st
            time.sleep(0.5)
        raise RuntimeError("el clúster no quedó listo: " + json.dumps(self.status()))

    def wait_for(self, pred, timeout=60, every=0.5):
        t0 = time.time()
        while time.time() - t0 < timeout:
            st = self.status()
            if st and pred(st):
                return time.time() - t0, st
            time.sleep(every)
        raise TimeoutError("condición no alcanzada: " + json.dumps(self.status()))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["up"])
    ap.add_argument("--dir", default="/tmp/dfsha-local")
    ap.add_argument("--nodes", type=int, default=4)
    a = ap.parse_args()
    c = LocalCluster(a.dir, a.nodes).start()
    print("Clúster listo. Exporte:")
    print(f"  export DFSHA_CONTROLLERS={','.join(c.controller_urls)}\n  export DFSHA_CA={c.ca}")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        c.stop()
