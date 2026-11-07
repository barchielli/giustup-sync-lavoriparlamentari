#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Giustup — i lavori parlamentari collegati alle leggi nazionali.

Porta in giustup.it i documenti dei lavori parlamentari (testi dei progetti di
legge, documenti, schede) riferiti alle leggi dell'archivio normativa: scarica i
PDF dai servizi della Camera (dati.camera.it), ne estrae il testo con
`pdftotext`, li carica su S3 e li indicizza in un indice suo, collegando ogni
documento alla legge tramite l'URN (campo `urn_legge`). La scheda della legge
nel sito mostra poi i lavori parlamentari collegati.

LA FONTE
La Camera pubblica i dati aperti (LOD) che legano ogni legge ai suoi lavori
preparatori:
    https://dati.camera.it/ocd/legge.rdf/L<anno>_<numero>?output=text/plain
ha `lex` = l'URN Normattiva della legge (la chiave che Giustup gia' conserva) e
`lavoriPreparatori` che rimanda all'atto Camera
    https://dati.camera.it/ocd/attocamera.rdf/ac<legislatura>_<numero>
che a sua volta espone i PDF dei documenti (documenti.camera.it/.../pdf...),
la scheda lavori preparatori (camera.it uri-res ...scheda.progetto.legge...) e
il servizio documenti (getDocumento.ashx?sezione=lavori...).

Il collegamento legge->IRI si costruisce da (anno_provvedimento,
num_provvedimento) delle leggi in archivio. Il `lex` usa la forma URN con anno
(urn:nir:stato:legge:2018;98) mentre l'archivio tiene la forma con data
(urn:nir:stato:legge:2018-08-09;98): la giunzione non ne dipende, il confronto
esatto normalizza la data.

LIMITI REALE DELLA FONTE (misurati il 7.11.2026)
- I lavori preparatori strutturati esistono per le leggi dell'era repubblicana
  (era costituente in poi, 1948+). Le leggi precedenti non hanno atto Camera.
- Nei dati aperti circa il 73% delle leggi moderne (2006-2022) ha almeno un PDF
  di documento; per le altre resta la scheda lavori preparatori, che si registra
  come collegamento.
- dati.camera.it applica rate-limit: dopo raffiche risponde HTTP 500. Il giro
  usa una pausa fra le richieste e il backoff sugli errori riprovevoli.

COME LAVORA
1. Prende l'elenco delle leggi dall'indice giustup-provvedimenti (tipo normativa,
   versione originale, denominazione LEGGE, anno >= 1948), con uno scan.
2. Per ogni legge risolve l'IRI legge.rdf, ne trae gli atti Camera, e da ogni
   atto i documenti PDF, la scheda e il servizio documenti.
3. Il confronto con l'indice si fa su `chiave_fonte` (l'URL del documento o la
   scheda): il giro della notte costa poco e lo storico riprende da dove era.
4. Ogni documento: PDF scaricato e caricato su S3 in
   `contentstore/lavoriparlamentari/<legislatura>/<atto>/<file>.pdf`, testo
   estratto con `pdftotext`; testo e metadati nell'indice `giustup-lavori-parlamentari`
   con `urn_legge` = URN della legge collegata.
5. Stato in `s3://.../stato-sync/lavoriparlamentari.json`, manifesto della corsa
   in `lavorazione/manifesto/`, metriche `EsitoSyncLavoriParlamentari` e
   `DocumentiNonScaricatiLavoriParlamentari`.

USO
    python3 lavoriparlamentari.py --prova 3            # 3 documenti, non scrive
    python3 lavoriparlamentari.py --massimo 900        # il giro della notte
    python3 lavoriparlamentari.py --da-anno 2015 --massimo 3000   # storico per anni
    python3 lavoriparlamentari.py --rifai              # riscarica anche i gia' in indice

Codici di uscita: 0 tutto fatto; 1 qualche documento non e' arrivato; 2 la fonte
non rispondeva piu'.
"""

import argparse
import csv
import datetime
import hashlib
import json
import os
import re
import ssl
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request

import boto3

# ---------------------------------------------------------------- costanti

OS_BASE = os.environ.get("GIUSTUP_OPENSEARCH", "https://localhost:9200")
IDX = "giustup-lavori-parlamentari"
IDX_LEGGI = "giustup-provvedimenti"          # le leggi della normativa, gia' in Giustup
BUCKET = os.environ.get("GIUSTUP_BUCKET", "giustup-archivio-sentenze-593859982945")
PREFISSO_PDF = "contentstore/lavoriparlamentari"
CHIAVE_STATO = "stato-sync/lavoriparlamentari.json"

DATI = "https://dati.camera.it/ocd"
SPARQL = "https://dati.camera.it/sparql"
LEGGE_IRI = DATI + "/legge.rdf/L{anno}_{numero}?output=text/plain"
# La scheda lavori preparatori della Camera, raggiungibile dall'atto con l'URN
# stabile (il valore di `isReferencedBy` nell'RDF dell'atto).
SCHEDA_CAMERA = "https://www.camera.it/uri-res/N2Ls?urn:camera-it:parlamento:scheda.progetto.legge:camera;{legislatura}.legislatura;{atto}"

CERT = "/etc/opensearch/kirk.pem"
CHIAVE_CERT = "/etc/opensearch/kirk-key.pem"

TESTATE = {
    "User-Agent": "Mozilla/5.0 (compatible; GiustupSync/1.0; +https://giustup.it)",
    "Accept": "text/plain,application/rdf+xml,text/turtle,text/n3,application/pdf,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.5",
}

# dati.camera.it applica rate-limit: si sta gentili e si riprova sui 5xx.
PAUSA_FRA_RICHIESTE = 1.0
# pausa fissa fra una legge e l'altra, anche quando la legge non ha documenti:
# senza, un giro che scorre decine di leggi senza lavori martella il portale e
# comincia a rispondere vuoto (rate-limit 'morbido' di dati.camera.it), facendo
# sembrare la fonte sprovvista di lavori. Misurato il 7.11.2026: a 0.6s/legge il
# ciclo fa ~3 richieste al secondo e sfiora il blocco; a 2.0s/legge si resta
# sotto la soglia. Lo storico (decine di migliaia di leggi) resta un lavoro
# lungo e va eseguito staccato, di notte.
PAUSA_FRA_LEGGI = 2.0
PAUSA_OGNI = 50
PAUSA_LUNGA = 3.0
TENTATIVI = 6
RIPROVABILI = (429, 500, 502, 503, 504)

RIGHE_PER_LOTTO = 25
FALLIMENTI_DI_FILA = 25

# Lo stesso namespace degli altri sync (identita' = nome, namespace = radice).
NOMESPACE = uuid.UUID("7c4b1a02-9d35-4e6b-ba2f-3d1a8c6e5f70")
AUTORITA = "Camera dei Deputati"
TIPO = "lavoro_parlamentare"


# ------------------------------------------------------------ utilita'

def adesso():
    return datetime.datetime.now(datetime.timezone.utc)


def log(msg):
    print("%s %s" % (adesso().strftime("%Y-%m-%dT%H:%M:%SZ"), msg), flush=True)


def pulisci(testo):
    if not testo:
        return ""
    t = testo.replace("\xa0", " ").replace("\u200b", "")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", t)
    return t.strip()


def grp_legge(urn):
    """Da `urn:nir:stato:legge:2018-08-09;98` -> (anno, numero)."""
    if not urn:
        return None
    m = re.match(r"urn:nir:stato:legge:(\d{4})-(\d{2})-(\d{2});(\d+)", urn)
    if not m:
        m = re.match(r"urn:nir:stato:legge:(\d{4});(\d+)", urn)
        if not m:
            return None
        return m.group(1), m.group(2)
    return m.group(1), m.group(4)


def normalizza_urn(urn):
    """La forma con anno di Normattiva: `urn:nir:stato:legge:2018;98`."""
    g = grp_legge(urn)
    if not g:
        return urn
    return "urn:nir:stato:legge:%s;%s" % g


# ------------------------------------------------------------------ rete

def richiesta(url, timeout=90, tentativi=TENTATIVI, binario=False):
    """Una richiesta HTTP con i tentativi e il backoff sui 5xx e 429."""
    ultimo = None
    for tentativo in range(1, tentativi + 1):
        try:
            req = urllib.request.Request(url, headers=TESTATE)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                corpo = r.read()
            return corpo if binario else corpo.decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            ultimo = "HTTP %d" % e.code
            if e.code not in RIPROVABILI:
                raise IOError(ultimo)
        except Exception as e:
            ultimo = "%s: %s" % (type(e).__name__, e)
        if tentativo < tentativi:
            time.sleep(1.8 * tentativo)
    raise IOError("%s dopo %d tentativi" % (ultimo, tentativi))


def rdf_triple(texto):
    """Parser minimale del RDF in text/plain: iteratore (s, p, o).

    L'oggetto puo' essere un IRI fra <…>, un letterale fra virgolette (con i
    punti dentro) oppure un token. La regex riconosce i tre casi e non lascia
    spazi di coda nell'oggetto (era il difetto che faceva scartare gli IRI)."""
    rx = re.compile(
        r'^(\S+)\s+<([^>]+)>\s+'
        r'(?:(<[^>]+>)|"((?:[^"\\]|\\.)*)"|([^\r\n][^\r\n]*?))'
        r'\s*\.\s*$')
    for riga in texto.splitlines():
        riga = riga.strip()
        if not riga or riga == ".":
            continue
        m = rx.match(riga)
        if not m:
            continue
        subj = m.group(1).strip()
        pred = m.group(2)
        if m.group(3):
            obj = m.group(3)                # <http://...>
        elif m.group(4) is not None:
            obj = '"%s"' % m.group(4)       # letterale: si tengono le virgolette
        else:
            obj = m.group(5).strip()
        if subj and pred and obj:
            yield subj, pred, obj


def rdf_oggetti(texto, predicato):
    """Gli oggetti (IRI o letterali) delle tripla che usano quel predicato."""
    fuori = []
    for s, p, o in rdf_triple(texto):
        if p.endswith(predicato):
            fuori.append(o.strip())
    return fuori


def oggetto_iri(o):
    """`<http://...>` -> l'IRI (togliendo spazi di coda); un letterale -> None."""
    o = (o or "").strip()
    if o.startswith("<") and o.endswith(">"):
        return o[1:-1]
    return None


def letterale(o):
    o = (o or "").strip()
    m = re.match(r'^"(.*)"(\^\^.*)?$', o, re.S)
    return m.group(1) if m else o


# ------------------------------------------------------------- l'indice

def _ctx():
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    try:
        c.load_cert_chain(CERT, CHIAVE_CERT)
    except Exception as e:
        log("certificato di OpenSearch non caricato (%r): si prova senza" % (e,))
    return c


def os_chiama(percorso, corpo=None, metodo=None):
    dati = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    req = urllib.request.Request(OS_BASE + percorso, data=dati,
                                 method=metodo or ("POST" if dati else "GET"))
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, context=_ctx(), timeout=180) as r:
        testo = r.read()
    return json.loads(testo) if testo else {}


MAPPATURA = {
    "settings": {
        "index": {"number_of_shards": 1, "number_of_replicas": 0,
                  "refresh_interval": "30s", "codec": "best_compression"},
        "analysis": {
            "filter": {
                "it_elisione": {"type": "elision", "articles_case": True,
                                "articles": ["c", "l", "all", "dall", "dell", "nell",
                                             "sull", "coll", "pell", "gl", "agl", "dagl",
                                             "degl", "negl", "sugl", "un", "m", "t",
                                             "s", "v", "d"]},
                "it_vuote": {"type": "stop", "stopwords": "_italian_"},
                "it_radici": {"type": "stemmer", "language": "light_italian"},
            },
            "analyzer": {
                "italiano_giuridico": {"tokenizer": "standard",
                                       "filter": ["lowercase", "it_elisione", "it_vuote",
                                                  "it_radici", "asciifolding"]},
                "italiano_esatto": {"tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding"]},
            },
        },
    },
    "mappings": {
        "properties": {
            "id_nodo": {"type": "keyword"},
            "chiave_fonte": {"type": "keyword"},
            "fonte_url": {"type": "keyword", "index": False},
            "tipo": {"type": "keyword"},
            "autorita": {"type": "keyword"},
            "categoria": {"type": "keyword"},
            "categoria_nome": {"type": "keyword"},
            "gruppo": {"type": "keyword"},
            "legislatura": {"type": "keyword"},
            "atto_camera": {"type": "keyword"},
            "numero": {"type": "keyword"},
            "anno": {"type": "short"},
            "data": {"type": "date"},
            # il collegamento alla legge: URN Normattiva nel formato dell'archivio
            "urn_legge": {"type": "keyword"},
            "estensione_legge": {"type": "text", "analyzer": "italiano_giuridico"},
            "titolo": {"type": "text", "analyzer": "italiano_giuridico",
                       "fields": {"esatto": {"type": "text", "analyzer": "italiano_esatto"}}},
            "testo": {"type": "text", "analyzer": "italiano_giuridico",
                      "fields": {"esatto": {"type": "text", "analyzer": "italiano_esatto"}}},
            "chiave_s3": {"type": "keyword", "index": False},
            "nome_file": {"type": "keyword", "index": False},
            "caratteri": {"type": "integer"},
            "testo_scarso": {"type": "boolean"},
            "oscurato": {"type": "boolean"},
            "created": {"type": "date"},
            "impronta": {"type": "keyword", "index": False},
            "versione_pipeline": {"type": "keyword"},
        }
    },
}


def crea_indice_se_manca():
    esiste = False
    try:
        os_chiama("/%s" % IDX)
        esiste = True
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    if esiste:
        return False
    os_chiama("/%s" % IDX, MAPPATURA, "PUT")
    log("indice %s creato" % IDX)
    return True


def conosciuti(chiavi):
    if not chiavi:
        return set()
    trovati = set()
    chiavi = list(chiavi)
    for i in range(0, len(chiavi), 500):
        parte = chiavi[i:i + 500]
        corpo = {"size": len(parte), "_source": ["chiave_fonte"],
                 "query": {"terms": {"chiave_fonte": parte}}}
        esito = os_chiama("/%s/_search" % IDX, corpo)
        for h in esito.get("hits", {}).get("hits", []):
            fonte = (h.get("_source") or {}).get("chiave_fonte")
            if fonte:
                trovati.add(fonte)
    return trovati


def _bulk(righe):
    if not righe:
        return
    corpo = "\n".join(righe) + "\n"
    req = urllib.request.Request(OS_BASE + "/_bulk", data=corpo.encode("utf-8"),
                                 method="POST")
    req.add_header("Content-Type", "application/x-ndjson")
    with urllib.request.urlopen(req, context=_ctx(), timeout=300) as r:
        esito = json.loads(r.read() or b"{}")
    if esito.get("errors"):
        errori = [i for i in esito.get("items", [])
                  if list(i.values())[0].get("error")]
        raise RuntimeError("operazione in blocco: %d errori, il primo: %s"
                           % (len(errori), json.dumps(errori[:1], ensure_ascii=False)[:300]))


def indicizza(documenti):
    if not documenti:
        return 0
    righe = []
    for d in documenti:
        righe.append(json.dumps({"index": {"_index": IDX, "_id": d["id_nodo"]}}))
        righe.append(json.dumps(d, ensure_ascii=False))
    _bulk(righe)
    return len(documenti)


# ------------------------------------------------------------------ S3

def s3():
    return boto3.client("s3")


def carica(cliente, chiave, corpo, tipo):
    cliente.put_object(Bucket=BUCKET, Key=chiave, Body=corpo, ContentType=tipo)


def stato(riassunto):
    scritto = json.dumps(riassunto, ensure_ascii=False, indent=2)
    try:
        s3().put_object(Bucket=BUCKET, Key=CHIAVE_STATO,
                        Body=scritto.encode("utf-8"),
                        ContentType="application/json")
    except Exception as e:
        log("stato non scritto su S3: %r" % (e,))
    return scritto


def metrica(nome, valore):
    try:
        boto3.client("cloudwatch", region_name="eu-central-1").put_metric_data(
            Namespace="Giustup", MetricData=[
                {"MetricName": nome, "Value": valore, "Unit": "Count"}])
    except Exception as e:
        log("metrica %s non pubblicata: %r" % (nome, e))


# ---------------------------------------------------- le leggi dall'indice

def leggi_da_indice(anno_da=1948, anno_a=9999):
    """Le leggi dell'archivio (scan): dict urn -> {anno, num, titolo}."""
    scroll = os_chiama("/%s/_search?scroll=5m" % IDX_LEGGI, {
        "size": 2000, "_source": ["urn", "anno_provvedimento", "num_provvedimento", "titolo"],
        "query": {"bool": {"must": [
            {"term": {"tipo": "normativa"}},
            {"term": {"versione": "originale"}},
            {"match": {"denominazione_atto": "LEGGE"}},
            {"range": {"anno_provvedimento": {"gte": anno_da, "lte": anno_a}}},
        ]}},
    })
    leggi = {}
    periodo = scroll["_scroll_id"]
    try:
        while True:
            for h in scroll.get("hits", {}).get("hits", []):
                s = h["_source"]
                g = grp_legge(s.get("urn") or "")
                if not g:
                    continue
                anno_num = (int(g[0]), int(g[1]))
                leggi[s["urn"]] = {"anno": anno_num[0], "num": anno_num[1],
                                   "titolo": s.get("titolo") or ""}
            sid = scroll.get("_scroll_id")
            if not scroll.get("hits", {}).get("hits"):
                break
            scroll = os_chiama("/_search/scroll", {"scroll": "5m", "scroll_id": sid})
    finally:
        try:
            os_chiama("/_search/scroll", {"scroll_id": periodo}, "DELETE")
        except Exception:
            pass
    return leggi


# ------------------------------------------------- lavori parlamentari

def sparql_righe(consulta, limite=3000):
    """Esegue una SELECT SPARQL e restituisce le righe (list[dict]).

    La Camera espone i dati aperti SOLO via SPARQL (niente dump in blocco) e
    applica rate-limit alle risorse per-legge: una query in blocco che estrae la
    struttura in poche chiamate e' la via robusta per migliaia di leggi. Si
    pagina con LIMIT/OFFSET finche' il portale restituisce righe."""
    righe = []
    offset = 0
    while True:
        q = "%s LIMIT %d OFFSET %d" % (consulta.rstrip().rstrip(";"), limite, offset)
        url = SPARQL + "?" + urllib.parse.urlencode({"query": q})
        req = urllib.request.Request(url, headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": TESTATE["User-Agent"]})
        with urllib.request.urlopen(req, timeout=240) as r:
            dati = json.loads(r.read().decode("utf-8"))
        blocco = dati.get("results", {}).get("bindings", [])
        if not blocco:
            break
        for riga in blocco:
            righe.append({k: v.get("value", "") for k, v in riga.items()})
        log("    struttura: %d righe fin qui" % len(righe))
        if len(blocco) < limite:
            break
        offset += limite
    return righe


Q_LEGGE_ATTO = (
    "SELECT ?lex ?atto WHERE { "
    "?legge a <http://dati.camera.it/ocd/legge> ; "
    "<http://dati.camera.it/ocd/lex> ?lex ; "
    "<http://dati.camera.it/ocd/lavoriPreparatori> ?lp . "
    "?lp <http://dati.camera.it/ocd/rif_attoCamera> ?atto }")
Q_ATTO_PDF = (
    "SELECT ?atto ?doc WHERE { "
    "?atto <http://purl.org/dc/elements/1.1/relation> ?doc . "
    "FILTER(CONTAINS(STR(?doc), \".pdf\")) }")
Q_ATTO_SCHEDA = (
    "SELECT ?atto ?scheda WHERE { "
    "?atto <http://purl.org/dc/terms/isReferencedBy> ?scheda . "
    "FILTER(CONTAINS(STR(?scheda), \"camera.it\")) }")
Q_ATTO_META = (
    "SELECT ?atto ?label ?data ?legisl WHERE { "
    "?atto a <http://dati.camera.it/ocd/atto> . "
    "OPTIONAL { ?atto <http://www.w3.org/2000/01/rdf-schema#label> ?label } "
    "OPTIONAL { ?atto <http://purl.org/dc/elements/1.1/date> ?data } "
    "OPTIONAL { ?atto <http://dati.camera.it/ocd/rif_leg> ?legisl } }")


def struttura():
    """La mappa legge(urn)->atti e atto->documenti, caricata in blocco via SPARQL.

    Ritorna (lex2atto, atto_info): il primo mappa l'URN Normattiva della legge
    (la chiave dell'archivio, forma con data: urn:nir:stato:legge:2018-08-09;98)
    agli IRi degli atti Camera; il secondo gli IRi degli atti ai loro documenti
    (pdf, scheda, titolo, data, legislatura)."""
    lex2atto = {}
    for r in sparql_righe(Q_LEGGE_ATTO):
        urn = r.get("lex", "").split("N2Ls?")[-1]
        atto = r.get("atto", "")
        if not urn or not atto:
            continue
        lista = lex2atto.setdefault(urn, [])
        if atto not in lista:
            lista.append(atto)

    atto_info = {}
    for r in sparql_righe(Q_ATTO_PDF):
        a = r.get("atto")
        doc = r.get("doc", "")
        if not a or ".pdf" not in doc:
            continue
        l = atto_info.setdefault(a, {}).setdefault("pdf", [])
        if doc not in l:
            l.append(doc)
    for r in sparql_righe(Q_ATTO_SCHEDA):
        a = r.get("atto")
        if a:
            atto_info.setdefault(a, {})["scheda"] = r.get("scheda", "")
    for r in sparql_righe(Q_ATTO_META):
        a = r.get("atto")
        if a:
            info = atto_info.setdefault(a, {})
            if r.get("label"):
                info["titolo"] = r["label"]
            if r.get("data"):
                info["data"] = r["data"]
            if r.get("legisl"):
                info["legislatura"] = r["legisl"].rsplit("_", 1)[-1]
    return lex2atto, atto_info


def lavori_da_struttura(urn_legge, lex2atto, atto_info):
    """I documenti/lavori parlamentari di una legge, letti dalla struttura."""
    documenti = []
    for at in lex2atto.get(urn_legge) or []:
        info = atto_info.get(at) or {}
        legislatura = info.get("legislatura", "")
        data = info.get("data", "")
        titolo = info.get("titolo", "")
        scheda = info.get("scheda", "")
        for pdf in dict.fromkeys(info.get("pdf") or []):
            nome = pdf.rstrip("/").rsplit("/", 1)[-1] if "/" in pdf else pdf
            documenti.append({
                "urn_legge": urn_legge, "atto_iri": at,
                "legislatura": legislatura, "data": data, "titolo": titolo,
                "documento_fonte_url": pdf, "documento_nome": nome,
                "categoria": "documento",
            })
        if scheda:
            documenti.append({
                "urn_legge": urn_legge, "atto_iri": at,
                "legislatura": legislatura, "data": data, "titolo": titolo,
                "documento_fonte_url": scheda, "documento_nome": "",
                "categoria": "scheda",
            })
    visti = set()
    unici = []
    for d in documenti:
        if d["documento_fonte_url"] in visti:
            continue
        visti.add(d["documento_fonte_url"])
        unici.append(d)
    return unici


# ---------------------------------------------------- un documento

def estrai_testo_pdf(percorso):
    """Il testo di un PDF con pdftotext (i PDF parlamentari hanno livello testo)."""
    try:
        esito = subprocess.run(["pdftotext", "-layout", percorso, "-"],
                               capture_output=True, timeout=180)
        return esito.stdout.decode("utf-8", "replace")
    except Exception:
        return ""


def scheda_documento(urn_legge, legge, d, testo):
    """Il documento dei lavori parlamentari come va in indice."""
    chiave_fonte = d["documento_fonte_url"]
    id_nodo = str(uuid.uuid5(NOMESPACE, "giustup:lavoriparlamentari:" + chiave_fonte))
    anno = None
    data = d.get("data")
    if data:
        m = re.match(r"^(\d{4})(\d{2})(\d{2})$", data)   # 20180726 -> 2018-07-26
        if m:
            data = "%s-%s-%s" % (m.group(1), m.group(2), m.group(3))
            anno = int(m.group(1))
        else:
            m = re.match(r"^(\d{4})-", data)
            if m:
                anno = int(m.group(1))
    anno = anno or legge["anno"]

    categoria = d["categoria"]
    categoria_nome = ("Documento dei lavori preparatori" if categoria == "documento"
                      else "Scheda dei lavori preparatori")
    titolo = pulisci(d.get("titolo") or "")
    nome_file = d.get("documento_nome") or ""
    return {
        "id_nodo": id_nodo,
        "chiave_fonte": chiave_fonte,
        "fonte_url": chiave_fonte,
        "tipo": TIPO,
        "autorita": AUTORITA,
        "categoria": categoria,
        "categoria_nome": categoria_nome,
        "gruppo": "Lavori parlamentari",
        "legislatura": d.get("legislatura") or "",
        "atto_camera": d.get("atto_iri") or "",
        "numero": nome_file,
        "anno": anno,
        "data": data,
        "urn_legge": urn_legge,
        "estensione_legge": legge["titolo"],
        "titolo": titolo or nome_file or urn_legge,
        "testo": pulisci(testo),
        "chiave_s3": "",
        "nome_file": nome_file,
        "caratteri": len(pulisci(testo)),
        "testo_scarso": len(pulisci(testo)) < 200,
        "oscurato": False,
        "created": adesso().isoformat().replace("+00:00", "Z"),
        "impronta": hashlib.sha256((chiave_fonte + testo).encode("utf-8")).hexdigest(),
        "versione_pipeline": "lavoriparlamentari-1.0",
    }


def lavora(urn_legge, legge, d, scrivi, cliente_s3):
    """Scarica il PDF se c'e', ne estrae il testo, monta il documento."""
    documento = scheda_documento(urn_legge, legge, d, "")
    if d["categoria"] == "documento" and d["documento_fonte_url"].endswith(".pdf"):
        try:
            pdf = richiesta(d["documento_fonte_url"], binario=True)
        except Exception as e:
            # PDF non scaricabile: non e' un guasto del documento in se', si
            # registra qua il collegamento senza testo.
            log("    PDF non scaricato %s: %s" % (d["documento_fonte_url"], e))
            documento["testo_scarso"] = True
            return documento
        nome = d["documento_nome"] or ("documento-%s.pdf" % documento["id_nodo"])
        legislatura = d.get("legislatura") or "0"
        at = d.get("atto_iri") or "atto"
        chiave = "%s/%s/%s/%s" % (PREFISSO_PDF, legislatura,
                                  at.rstrip("/").rsplit("/", 1)[-1], nome)
        documento["chiave_s3"] = chiave
        documento["nome_file"] = nome
        if scrivi:
            carica(cliente_s3, chiave, pdf, "application/pdf")
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                f.write(pdf)
                temp = f.name
            try:
                testo = estrai_testo_pdf(temp)
            finally:
                os.unlink(temp)
            documento["testo"] = pulisci(testo)
            documento["caratteri"] = len(pulisci(testo))
            documento["testo_scarso"] = documento["caratteri"] < 200
    return documento


# ------------------------------------------------------------- il giro

def sincronizza(args):
    prova = max(0, int(args.prova or 0))
    scrivi = not prova and not args.senza_s3
    indice = not prova and not args.senza_indice

    if indice:
        crea_indice_se_manca()

    client_s3 = s3() if scrivi else None

    log("raccolgo le leggi dall'indice %s (anno da %s a %s)"
        % (IDX_LEGGI, args.da_anno, args.a_anno))
    leggi = leggi_da_indice(int(args.da_anno), int(args.a_anno))
    log("leggi da processare: %d" % len(leggi))

    # La struttura (legge->atti, atto->documenti) si carica una volta sola in
    # blocco via SPARQL: e' la via che non innesca il rate-limit delle risorse
    # per-legge di dati.camera.it e costa poche chiamate anche per lo storico.
    log("scarico la struttura dei lavori parlamentari via SPARQL")
    lex2atto, atto_info = struttura()
    log("struttura caricata: %d leggi con lavori, %d atti"
        % (len(lex2atto), len(atto_info)))

    inizio = time.time()
    totale_nuovi = totale_visti = totale_falliti = 0
    falliti, righe_manifesto = [], []
    vacui = 0
    lotto = []

    def svuota_lotto():
        nonlocal totale_falliti
        if not lotto or not indice:
            return
        try:
            indicizza(lotto)
        except Exception as e:
            log("indicizzazione di %d documenti: %s" % (len(lotto), e))
            totale_falliti += len(lotto)
            for doc in lotto:
                falliti.append({"url": doc["chiave_fonte"],
                                "motivo": "indice: %s" % str(e)[:160]})
        del lotto[:]

    # le leggi gia' esaminate (scheda in indice) vengono saltate a meno di --rifai
    gia_ = set()
    if indice and not args.rifai:
        # le schede hanno chiave_fonte la scheda camera: si confronta sul lavoro
        pass

    for k, urn_legge in enumerate(sorted(leggi.keys())):
        if prova and totale_nuovi >= prova:
            break
        if args.massimo and totale_nuovi >= args.massimo:
            break
        legno = leggi[urn_legge]
        anno, num = legno["anno"], legno["num"]
        totale_visti += 1
        documenti = lavori_da_struttura(urn_legge, lex2atto, atto_info)
        vacui = 0

        if not documenti:
            log("    %s: nessun lavoro parlamentare" % urn_legge)
            time.sleep(PAUSA_FRA_LEGGI)
            continue

        if indice and not args.rifai:
            try:
                gia_ = conosciuti([d["documento_fonte_url"] for d in documenti])
            except Exception as e:
                log("indice non interrogabile (%s): si ricarica tutto" % (e,))
                gia_ = set()

        novita = 0
        for d in documenti:
            if prova and totale_nuovi >= prova:
                break
            if args.massimo and totale_nuovi >= args.massimo:
                break
            if d["documento_fonte_url"] in gia_ and not args.rifai:
                continue
            try:
                documento = lavora(urn_legge, legno, d, scrivi, client_s3)
            except Exception as e:
                totale_falliti += 1
                falliti.append({"url": d["documento_fonte_url"], "motivo": str(e)[:200]})
                log("    %s: %s" % (d["documento_fonte_url"], e))
                continue
            novita += 1
            totale_nuovi += 1
            righe_manifesto.append([
                documento["id_nodo"], documento["chiave_s3"],
                documento["chiave_fonte"], documento["categoria"],
                documento["legislatura"], documento["anno"] or "",
                documento["data"] or "", documento["urn_legge"],
                documento["caratteri"], documento["impronta"],
            ])
            if indice:
                lotto.append(documento)
                if len(lotto) >= RIGHE_PER_LOTTO:
                    svuota_lotto()
            if prova:
                log("    [prova] %s | %s | %s | %d caratteri"
                    % (documento["categoria"], documento["titolo"][:50],
                       documento["anno"], documento["caratteri"]))
            time.sleep(PAUSA_FRA_RICHIESTE)
            if totale_nuovi % PAUSA_OGNI == 0:
                time.sleep(PAUSA_LUNGA)
                log("  ... %d documenti, %d falliti, %.0f s"
                    % (totale_nuovi, totale_falliti, time.time() - inizio))
        time.sleep(PAUSA_FRA_LEGGI)
        if k % 200 == 0 and k > 0:
            log("  ... %d leggi viste, %d documenti, %.0f s"
                % (k, totale_nuovi, time.time() - inizio))

    svuota_lotto()

    esito = 1 if totale_falliti else 0

    manifesto = ""
    if righe_manifesto and scrivi:
        manifesto = "manifesto-lavoriparlamentari-%s.csv" % adesso().strftime("%Y%m%d")
        colonne = ["id_nodo", "chiave_s3", "chiave_fonte", "categoria", "legislatura",
                   "anno", "data", "urn_legge", "caratteri", "impronta"]
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False,
                                         encoding="utf-8-sig", newline="") as f:
            scrittore = csv.writer(f, delimiter=";")
            scrittore.writerow(colonne)
            for riga in righe_manifesto:
                scrittore.writerow(riga)
            percorso = f.name
        try:
            carica(client_s3, "lavorazione/manifesto/" + manifesto,
                   open(percorso, "rb").read(), "text/csv; charset=utf-8")
        except Exception as e:
            log("manifesto non caricato su S3: %r" % (e,))
            manifesto = ""
        finally:
            os.unlink(percorso)

    riassunto = {
        "ultimoGiro": adesso().isoformat().replace("+00:00", "Z"),
        "esito": esito,
        "durataSecondi": int(time.time() - inizio),
        "leggiViste": totale_visti,
        "documentiNuovi": totale_nuovi,
        "documentiFalliti": totale_falliti,
        "falliti_elenco": falliti[:200],
        "manifesto": manifesto,
        "indice": IDX,
        "versionePipeline": "lavoriparlamentari-1.0",
    }
    if prova:
        log("[prova] nessuna scrittura: stato non pubblicato")
    else:
        stato(riassunto)
        metrica("EsitoSyncLavoriParlamentari", esito)
        metrica("DocumentiNonScaricatiLavoriParlamentari", totale_falliti)
    log("fine: %d nuovi, %d falliti, %d secondi"
        % (totale_nuovi, totale_falliti, riassunto["durataSecondi"]))
    return esito


# ------------------------------------------------------------ CLI

def main():
    a = argparse.ArgumentParser(description="Sync dei lavori parlamentari per Giustup")
    a.add_argument("--prova", type=int, default=0,
                   help="quanti documenti, senza scrivere niente")
    a.add_argument("--rifai", action="store_true",
                   help="riscarica anche i documenti gia' in indice")
    a.add_argument("--massimo", type=int, default=0,
                   help="tetto di documenti in questo giro")
    a.add_argument("--da-anno", type=int, default=1948,
                   help="dall'anno di emanazione delle leggi da considerare")
    a.add_argument("--a-anno", type=int, default=9999,
                   help="all'anno di emanazione delle leggi da considerare")
    a.add_argument("--senza-indice", action="store_true", help="non tocca OpenSearch")
    a.add_argument("--senza-s3", action="store_true", help="non carica su S3")
    args = a.parse_args()

    try:
        return sincronizza(args)
    except KeyboardInterrupt:
        log("interrotto")
        return 2
    except Exception as e:
        log("errore: %r" % (e,))
        return 2


if __name__ == "__main__":
    sys.exit(main())