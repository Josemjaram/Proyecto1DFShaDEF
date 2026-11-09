"""CLI de DFSha:  python -m dfsha.cli <comando>   (o el script ./dfsha)

Configuración: DFSHA_CONTROLLERS=https://ctl1:8001,https://ctl2:8001,https://ctl3:8001  y  DFSHA_CA=ca.pem
Sesión (token, directorio actual, locks) en ~/.dfsha/session.json  (DFSHA_HOME para cambiarlo).
"""
import argparse
import getpass
import json
import os
import sys

from .client import DFSClient
from .common import DFSError, http, norm

HOME = os.environ.get("DFSHA_HOME", os.path.expanduser("~/.dfsha"))
SESSION = os.path.join(HOME, "session.json")


def load():
    st = {}
    if os.path.exists(SESSION):
        st = json.load(open(SESSION))
    ctl = os.environ.get("DFSHA_CONTROLLERS")
    controllers = [c for c in ctl.split(",") if c] if ctl else st.get("controllers", [])
    if not controllers:
        sys.exit("Defina DFSHA_CONTROLLERS (URLs de los controladores separadas por coma)")
    c = DFSClient(controllers, token=st.get("token"), user=st.get("user"), ca=os.environ.get("DFSHA_CA"),
                  session=st.get("session"), cwd=st.get("cwd", "/"))
    c.locks = st.get("locks", {})
    return c


def save(c):
    os.makedirs(HOME, exist_ok=True)
    with open(SESSION, "w") as f:
        json.dump({"controllers": c.controllers, "token": c.token, "user": c.user, "cwd": c.cwd,
                   "session": c.session, "locks": c.locks}, f)
    os.chmod(SESSION, 0o600)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="dfsha", description="Cliente del sistema de archivos distribuido DFSha")
    sp = ap.add_subparsers(dest="cmd", required=True)

    def add(name, *args, **flags):
        p = sp.add_parser(name)
        for a in args:
            p.add_argument(a)
        for f, kw in flags.items():
            p.add_argument("--" + f.replace("_", "-"), **kw)
        return p

    add("register", user={"required": True}, password={})
    add("login", user={"required": True}, password={})
    add("whoami"); add("pwd")
    add("cd", "path")
    p = sp.add_parser("ls")
    p.add_argument("-l", "--long", action="store_true")
    p.add_argument("path", nargs="?", default=".")
    add("stat", "path"); add("mkdir", "path"); add("rmdir", "path"); add("rm", "path")
    add("put", "local", "remote", append={"action": "store_true"}, create={"action": "store_true"},
        block_size={"type": int})
    add("get", "remote", "local")
    add("read", "path", offset={"type": int, "default": 0}, length={"type": int, "default": -1})
    add("write", "path", append={"action": "store_true"}, offset={"type": int, "default": None}, data={})
    add("lock", "path"); add("unlock", "path")
    add("cluster", admin_key={"default": os.environ.get("DFSHA_ADMIN_KEY", "admin-dev")})
    a = ap.parse_args(argv)

    c = load()
    try:
        if a.cmd in ("register", "login"):
            pw = a.password or getpass.getpass("Contraseña: ")
            if a.cmd == "register":
                c.register(a.user, pw)
            c.login(a.user, pw)
            c.cwd = "/"
            print(f"sesión iniciada como {a.user}")
        elif a.cmd == "whoami":
            print(c.user or "(sin sesión)")
        elif a.cmd == "pwd":
            print(c.cwd)
        elif a.cmd == "cd":
            print(c.cd(a.path))
        elif a.cmd == "ls":
            for e in c.ls(a.path):
                print(f"{e['type'][0]} {e.get('size', '-'):>12} v{e.get('version', '-'):<3} {e['name']}" if a.long
                      else e["name"] + ("/" if e["type"] == "dir" else ""))
        elif a.cmd == "stat":
            print(json.dumps(c.stat(a.path), indent=2))
        elif a.cmd in ("mkdir", "rmdir", "rm"):
            getattr(c, a.cmd)(a.path)
        elif a.cmd == "put":
            r = c.put(a.local, a.remote, mode="append" if a.append else "create" if a.create else "replace",
                      block_size=a.block_size)
            print(f"ok versión {r['version']}  {r['bytes']} bytes  {r['seconds']:.2f}s  "
                  f"{r['bytes'] / 1048576 / max(r['seconds'], 1e-9):.1f} MiB/s")
        elif a.cmd == "get":
            r = c.get(a.remote, a.local)
            print(f"ok versión {r['version']}  {r['bytes']} bytes  {r['seconds']:.2f}s")
        elif a.cmd == "read":
            sys.stdout.buffer.write(c.read_bytes(a.path, a.offset, a.length))
        elif a.cmd == "write":
            data = a.data.encode() if a.data is not None else sys.stdin.buffer.read()
            if a.offset is not None:
                if a.append:
                    sys.exit("--offset y --append son excluyentes")
                print(json.dumps(c.write_at(a.path, a.offset, data)))  # modifica solo los bloques que toca el rango
            else:
                print(json.dumps(c.write_bytes(a.path, data, "a" if a.append else "w")))
        elif a.cmd == "lock":
            print(json.dumps(c.lock(a.path)))
        elif a.cmd == "unlock":
            c.unlock(a.path)
        elif a.cmd == "cluster":
            for base in c.controllers:  # consulta al líder (los seguidores responden 503 NOT_LEADER)
                r = http().get(base + "/v1/cluster", headers={"X-Admin-Key": a.admin_key}, timeout=5)
                if r.status_code == 200:
                    print(json.dumps(r.json(), indent=2))
                    break
    except DFSError as e:
        print(f"error {e.status} {e.code}: {e.msg}", file=sys.stderr)
        save(c)
        return 1
    save(c)
    return 0


if __name__ == "__main__":
    sys.exit(main())
