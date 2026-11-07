# giustup-sync-lavoriparlamentari

Raccoglitore che porta in giustup.it i **lavori parlamentari** collegati alle
leggi nazionali dell'archivio normativa: scarica i PDF dei documenti dei lavori
preparatori dalla Camera (`dati.camera.it`), ne estrae il testo con `pdftotext`,
li carica su S3 e li indicizza nell'indice `giustup-lavori-parlamentari`,
collegando ogni documento alla legge tramite `urn_legge` (l'URN Normattiva, la
stessa chiave che Giustup conserva).

La scheda della legge nel sito mostra i lavori parlamentari collegati.

## La fonte

`https://dati.camera.it/ocd/legge.rdf/L<anno>_<numero>?output=text/plain`
- `lex` = URN Normattiva della legge (giunzione);
- `lavoriPreparatori` → `attocamera.rdf/ac<legislatura>_<numero>`.

`attocamera.rdf/ac<legislatura>_<numero>` → PDF dei documenti
(`documenti.camera.it/_dati/.../pdf`), scheda lavori preparatori, servizio
documenti.

## Uso

```
python3 lavoriparlamentari.py --prova 3            # 3 documenti, non scrive
python3 lavoriparlamentari.py --massimo 900        # il giro della notte
python3 lavoriparlamentari.py --da-anno 2015 --massimo 3000  # storico per anni
python3 lavoriparlamentari.py --rifai              # riscarica anche i gia' in indice
```

Dipendenze: `boto3`; `pdftotext` (poppler) per il testo dei PDF.

## Limiti della fonte (misurati il 7.11.2026)
- I lavori preparatori strutturati esistono per le leggi dell'era repubblicana
  (1948+). Le leggi precedenti non hanno atto Camera.
- Circa il 73% delle leggi moderne ha almeno un PDF; per le altre si registra la
  scheda lavori preparatori come collegamento.
- `dati.camera.it` applica rate-limit (HTTP 500 a raffiche): il giro usa pausa
  e backoff sui 5xx/429.

## Indice

`giustup-lavori-parlamentari` — ogni documento con `urn_legge` per il
collegamento alla legge. `chiave_fonte` = URL del documento o della scheda.