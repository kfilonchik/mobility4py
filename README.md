# mobility4py

**mobility4py** is a Python pipeline that transforms **Call Detail Records (CDRs)** into structured **mobility trajectories**.  
It covers antenna-sector spreading, staypoint detection (InfoStop), and trajectory segmentation with trackintel (positionfixes → staypoints → triplegs → trips).

---

## Features
- **Antenna-based spreading**: deterministic placement inside tower sectors (azimuth/radius), optional water masking.
- **Staypoints (InfoStop)**: robust to sparse/irregular CDRs; configurable dwell/distance/time parameters.
- **Trajectories (trackintel)**: build `positionfixes`, `staypoints`, `triplegs`, `trips`; assign `staypoint_id` to fixes.
- **Analytics**: home/work labeling (`osna_method`), tripleg length, duration, and speed; positionfix speeds.
- **Optional**: simple, illustrative transport mode tagging (not a validated classifier).

---

## Installation

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -U pip setuptools wheel
pip install pandas numpy geopandas shapely pyproj trackintel infostop
```
## Licence
MIT License © Khristina Filonchik, 2025

