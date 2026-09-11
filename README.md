# Seismic Entropy

True random numbers extracted from live seismic waveform data. A wall of
raw integers, each traceable to a real ground-motion sensor somewhere on
Earth right now.

## How it works

1. **Source**: live streaming from IRIS/EarthScope SeedLink
   (`rtserve.iris.washington.edu:18000`), six stations at once:
   New Madrid MO, Borgarfjörður Iceland, Albuquerque NM, Afiamalu Samoa,
   Matsushiro Japan, College AK. See `backend/seedlink_source.py` to add
   or change stations.
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

## Running it

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

## Known gaps / next steps

- **Not yet deployed anywhere.** This needs a persistent server process
  (the SeedLink connection is a long-lived TCP stream), so it can't be a
  static GitHub Pages site like the other generative pieces. Needs a real
  host (a small VPS, Fly.io, Railway, etc.) before it has a public URL.
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
