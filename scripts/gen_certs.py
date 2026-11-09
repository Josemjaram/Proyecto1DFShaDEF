#!/usr/bin/env python3
"""Genera una CA propia y certificados TLS con SAN (IPs/DNS) para el clúster.

Dos modos:

1) Desarrollo local (un solo certificado compartido, como antes):
     python scripts/gen_certs.py certs 127.0.0.1 localhost
   Produce certs/ca.pem, certs/ca.key, certs/server.pem, certs/server.key.

2) Despliegue en AWS (un certificado y una llave privada POR NODO):
     python scripts/gen_certs.py certs --node c1=10.0.1.11 --node c2=10.0.1.12 --node c3=10.0.1.13 \\
                                       --node n1=10.0.1.21 --node n2=10.0.1.22
   Produce certs/ca.pem, certs/ca.key y certs/<nodo>/server.pem + server.key.
   A cada VM se copian SOLO certs/ca.pem y certs/<su nodo>/server.{pem,key} (nunca ca.key ni las llaves de otros).

Agregar un nodo después (la CA se reutiliza; no hay que tocar ni reiniciar los demás nodos):
     python scripts/gen_certs.py certs --node n5=10.0.1.25 [--node n5=nombre.interno]

ca.key es la llave que firma todos los certificados: guárdela fuera de las VMs y fuera del repositorio.
"""
import argparse
import datetime
import ipaddress
import os

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

DAYS = 365


def pem_key(k):
    return k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())


def write_private(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(data)


def load_or_create_ca(out):
    ca_pem, ca_key_path = f"{out}/ca.pem", f"{out}/ca.key"
    if os.path.exists(ca_pem) and os.path.exists(ca_key_path):
        ca = x509.load_pem_x509_certificate(open(ca_pem, "rb").read())
        key = serialization.load_pem_private_key(open(ca_key_path, "rb").read(), password=None)
        return ca, key
    now = datetime.datetime.now(datetime.timezone.utc)
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "DFSha CA")])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(days=1))
          .not_valid_after(now + datetime.timedelta(days=DAYS))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                       content_commitment=False, key_encipherment=False, data_encipherment=False,
                                       key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
          .sign(key, hashes.SHA256()))
    open(ca_pem, "wb").write(ca.public_bytes(serialization.Encoding.PEM))
    write_private(ca_key_path, pem_key(key))
    return ca, key


def issue(ca, ca_key, cn, sans, dest):
    """Emite un certificado de servidor con los SAN dados y lo escribe en dest/server.{pem,key}."""
    now = datetime.datetime.now(datetime.timezone.utc)
    alt = []
    for s in sans:
        try:
            alt.append(x509.IPAddress(ipaddress.ip_address(s)))
        except ValueError:
            alt.append(x509.DNSName(s))
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
            .issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=DAYS))
            .add_extension(x509.SubjectAlternativeName(alt), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .sign(ca_key, hashes.SHA256()))
    os.makedirs(dest, exist_ok=True)
    open(f"{dest}/server.pem", "wb").write(cert.public_bytes(serialization.Encoding.PEM))
    write_private(f"{dest}/server.key", pem_key(key))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out", help="directorio de salida (se crea)")
    ap.add_argument("sans", nargs="*", help="modo local: IPs/DNS del certificado único compartido")
    ap.add_argument("--node", action="append", default=[], metavar="NOMBRE=IP[,IP|DNS...]",
                    help="modo AWS: un certificado propio para este nodo (repetible)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    ca, ca_key = load_or_create_ca(a.out)
    if a.node:
        for spec in a.node:
            name, _, sans = spec.partition("=")
            if not name or not sans:
                ap.error(f"--node debe ser NOMBRE=IP[,IP|DNS...], recibido: {spec!r}")
            issue(ca, ca_key, f"dfsha-{name}", sans.split(","), f"{a.out}/{name}")
            print(f"certificado de {name} en {a.out}/{name}/ SAN: {sans}")
    else:
        sans = a.sans or ["127.0.0.1", "localhost"]
        issue(ca, ca_key, "dfsha-node", sans, a.out)
        print("certificados en", a.out, "SAN:", sans)


if __name__ == "__main__":
    main()
