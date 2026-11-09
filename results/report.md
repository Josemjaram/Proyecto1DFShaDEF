# Resultados de pruebas DFSha

Entorno: clúster local (3 controladores + 4 nodos de datos, procesos separados, TLS) generado con `tests/run_tests.py`.

| Prueba | Resultado | Tiempo |
|---|---|---|
| Preparación del clúster | PASS | 2.4s |
| RF1  Sistema de archivos jerárquico y aislamiento | PASS | 2.0s |
| P1   Carga y recuperación (64 MiB, particionado, hash idéntico) | PASS | 2.5s |
| P2   Acceso RF3: open/read/write/close/lock | PASS | 3.5s |
| P3   Concurrencia: lock, carrera de escritores, fencing | PASS | 19.0s |
| P4   Replicación y fallos: caída de un nodo | PASS | 25.8s |
| P4b  Nodo cae a mitad de una escritura | PASS | 9.1s |
| P5   Consistencia: réplica desconectada durante escritura | PASS | 6.4s |
| E3   Escritura interrumpida / bloque parcial | PASS | 17.8s |
| GC   Versiones, tombstones y liberación de bloques | PASS | 23.5s |
| P6   Seguridad: authn/authz, TLS, cifrado en reposo, integridad | PASS | 5.7s |
| P7   Rendimiento (≥2 condiciones) | PASS | 38.4s |
| HA   Failover de controladores y pérdida de mayoría | PASS | 10.2s |

## Evidencias

### Preparación del clúster — PASS
```json
{
  "controladores": 3,
  "nodos_de_datos": 4,
  "lider": "https://127.0.0.1:8002"
}
```

### RF1  Sistema de archivos jerárquico y aislamiento — PASS
```json
{
  "operaciones": "mkdir, cd, ls, rmdir, rm, stat",
  "aislamiento_entre_usuarios": "ok"
}
```

### P1   Carga y recuperación (64 MiB, particionado, hash idéntico) — PASS
```json
{
  "tamano_MiB": 64,
  "bloques": 16,
  "nodos_con_bloques": 4,
  "bloques_por_nodo": {
    "n1": 12,
    "n2": 12,
    "n3": 12,
    "n4": 12
  },
  "copias_por_bloque_min_max": [
    3,
    3
  ],
  "sha256_origen=destino": "358aace1a6f3e3d4…",
  "put_s": 1.5,
  "get_s": 0.58
}
```

### P2   Acceso RF3: open/read/write/close/lock — PASS
```json
{
  "write": "crear(x)/reemplazar(w)/anexar(a); sin escritura parcial",
  "versiones": [
    1,
    2,
    3
  ],
  "fencing_token": 8
}
```

### P3   Concurrencia: lock, carrera de escritores, fencing — PASS
```json
{
  "lock_segundo_escritor": "LOCKED",
  "carrera_6_hilos": {
    "publicaron": 6,
    "reintentos_por_423": 45
  },
  "archivo_final_integro": true,
  "escritor_zombi": "LEASE_EXPIRED"
}
```

### P4   Replicación y fallos: caída de un nodo — PASS
```json
{
  "nodo_caido": "n1",
  "lectura_con_nodo_caido_s": 0.38,
  "escritura_con_nodo_caido_s": 0.56,
  "deteccion_sospechoso_desde_la_caida_s": 5.3,
  "deteccion_caido_desde_la_caida_s": 11.5,
  "reparacion_a_3_copias_tras_declararlo_caido_s": 5.7,
  "bloques_perdidos": 0
}
```

### P4b  Nodo cae a mitad de una escritura — PASS
```json
{
  "nodo_caido_a_mitad_de_escritura": "n3",
  "bloques": 48,
  "copias_min_al_publicar": 2,
  "hash_ok": true
}
```

### P5   Consistencia: réplica desconectada durante escritura — PASS
```json
{
  "replica_pausada": "n2",
  "escritura_con_replica_caida_s": 2.51,
  "version_publicada": 2,
  "copias_minimas_al_publicar": 2,
  "bloques_publicados_sin_la_replica_pausada": 6,
  "copias_minimas_tras_recuperacion": 3,
  "lectores_vieron": "version nueva completa, nunca una mezcla"
}
```

### E3   Escritura interrumpida / bloque parcial — PASS
```json
{
  "bloque_parcial_rechazado": "422 INVALID_HASH",
  "archivo_visible_tras_crash": false,
  "bloques_huerfanos_creados": 9,
  "limpiados_tras_s": 16.9
}
```

### GC   Versiones, tombstones y liberación de bloques — PASS
```json
{
  "bloques_antes": 390,
  "versiones_viejas_liberadas_s": 9.7,
  "version_1": "VERSION_EXPIRED",
  "rm_logico_bloques_liberados_s": 10.8
}
```

### P6   Seguridad: authn/authz, TLS, cifrado en reposo, integridad — PASS
```json
{
  "sin_token": 401,
  "token_alterado": 401,
  "token_vencido": 401,
  "password_incorrecta": 401,
  "bob_lee_archivo_de_ana": 403,
  "bob_reporta_bloque_de_ana": 403,
  "nodo_sin_token": 401,
  "token_de_bob_en_bloque_de_ana": 403,
  "token_de_lectura_para_escribir": 403,
  "token_propio_ok": 200,
  "borrar_bloque_sin_token_servicio": 403,
  "tls": {
    "version": "TLSv1.3",
    "cifrado": "TLS_AES_256_GCM_SHA384"
  },
  "ca_desconocida_rechazada": true,
  "http_en_claro_rechazado": true,
  "en_reposo": {
    "coincidencias_texto_plano_en_discos": 0,
    "primeros_bytes_del_bloque": "444642315ef5975979cb93fbb5b6fc82297c823b7cce16990d7fdb03f807a4b97a242cca"
  },
  "bloque_alterado": {
    "nodo": "n1",
    "lecturas_correctas": 6,
    "reparado_en_s": 0.0
  }
}
```

### P7   Rendimiento (≥2 condiciones) — PASS
```json
{
  "nota": "máquina local de 1 vCPU, todo en localhost: NO es representativo de AWS",
  "metadatos": {
    "stat_mediana_ms": 2.0,
    "stat_p95_ms": 2.3
  },
  "mejor_put_MiBs": 33.8
}
```

### HA   Failover de controladores y pérdida de mayoría — PASS
```json
{
  "lider_caido": "c2",
  "failover_hasta_primera_escritura_ok_s": 3.4,
  "sin_mayoria": "503 NOT_LEADER",
  "recuperacion_con_2_de_3_s": 2.3,
  "metadatos_conservados": true
}
```
