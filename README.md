# Seismic Entropy

True random numbers extracted from live seismic waveform data. A wall of
raw integers, each traceable to a real ground-motion sensor somewhere on
Earth right now.

**Live**: [jontoews.com/seismic](https://jontoews.com/seismic/) (static
frontend) — backend at
[seismic-entropy.fly.dev](https://seismic-entropy.fly.dev) (Fly.io).

## How it works

1. **Source**: live streaming from IRIS/EarthScope SeedLink
   (`rtserve.iris.washington.edu:18000`), nine stations at once, deliberately
   spread across the world (only one US station, College AK): Borgarfjörður
   Iceland, Afiamalu Samoa, Matsushiro Japan, College Alaska, Palmer Station
   Antarctica, South Karori New Zealand, Galápagos Ecuador, Kongsberg Norway,
   Tsumeb Namibia. See `backend/seedlink_source.py` to add or change stations
   — also confirmed live there but not currently used: Australia, a second
   Antarctic station, Cook Islands, Thailand, Greenland.
2. **Entropy**: each raw sample is a signed integer sensor count. The high
   bits are real ground motion (structured, not random); only the
   least-significant bit flickers unpredictably. We take *only* that bit
   per sample, accumulate 256 of them, and run them through SHA-256 once
   full. The 256-bit digest is sliced into eight 32-bit integers — the
   whole digest used, nothing discarded. See `backend/entropy.py`.
3. **Serving**: a FastAPI server runs the SeedLink client on a background
   thread and fans every extracted integer out to (a) a WebSocket for the
   live visual wall and (b) a FIFO queue that `/api/random` consumes from
   — numbers served over the API are never repeated.

## Running it locally

```bash
cd ~/Creative-Projects/seismic-entropy
source venv/bin/activate          # already set up; venv/ has obspy+fastapi+uvicorn
cd backend
uvicorn server:app --reload --port 8420
```

Then open `http://127.0.0.1:8420/`.

- `GET /api/random?n=10` — pull up to 256 fresh integers (ANU QRNG style),
  waits up to 5s (default) for enough live entropy if the feed hasn't
  produced enough yet.
- `GET /api/status` — feed health: connected?, packets received, per-station
  labels, how many numbers are queued for the API right now.

## Deployment

Two pieces, deployed separately:

1. **Backend** (Fly.io, app name `seismic-entropy`, region `ord`) — the
   actual persistent process holding the SeedLink connection open. Deploy
   changes with `flyctl deploy` from this directory. `fly.toml` pins
   `min_machines_running = 1` / `auto_stop_machines = false` deliberately —
   this can't be allowed to scale to zero on idle like a normal web app,
   since the whole point is an always-open connection to IRIS.
2. **Frontend** (`frontend/index.html`, no build step) — mirrored as a
   static file to `~/swoonjet.github.io/seismic/index.html` and pushed
   there to go live at jontoews.com/seismic (same pattern as Cadastre/
   Lampo/etc., see `reference_jontoews_personal_site_deploy` memory). The
   page detects whether it's self-hosted (localhost, or the Fly.io domain
   itself) vs. mirrored elsewhere, and points its WebSocket/API links at
   `seismic-entropy.fly.dev` explicitly when it's not. After editing
   `frontend/index.html`, re-copy it to the pages repo and push there too
   — the two copies are not symlinked, just kept in sync by hand.

## Known gaps / next steps

- **Click-to-copy** is standard `navigator.clipboard.writeText`, wired up
  and should work in any normal browser tab — but I could not get
  automated proof of it (the Clipboard API hangs without genuine window
  focus, which the browser-automation harness I tested with can't
  provide). Worth a quick manual click-test.
- **Reykjanes substitution**: no currently-live SeedLink station sits on
  the Reykjanes peninsula specifically — the temporary deployments from
  the 2021 eruption swarm (networks `8F`, `3A`, `XH`) all ended in
  2021–2023. Used `II.BORG` (Borgarfjörður, still genuinely Iceland,
  confirmed live) instead.
- The "further directions" list (cellular automaton rules, reaction-
  diffusion, flow fields, mutating typography, granular synthesis,
  plotter output) is intentionally not built yet — this is the wall +
  API MVP, hashing-based, per today's ask.
