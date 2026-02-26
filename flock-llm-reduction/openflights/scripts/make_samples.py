import argparse
import csv
from collections import Counter
from pathlib import Path

def read_rows(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            yield row

def write_rows(path: Path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def is_int(s: str) -> bool:
    return (s or "").strip().isdigit()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", default="data/original_data")
    ap.add_argument("--out-dir", default="data/samples")
    ap.add_argument("--max-airports", type=int, default=300)
    ap.add_argument("--max-routes", type=int, default=8000)
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    airports_csv = in_dir / "airports.csv"
    airlines_csv = in_dir / "airlines.csv"
    routes_csv = in_dir / "routes.csv"
    planes_csv = in_dir / "planes.csv"
    countries_csv = in_dir / "countries.csv"

    # OpenFlights routes contain src/dst airport IDs and airline IDs, but can be NULL (shown as \N in original .dat files).
    # We'll build a join-friendly sample by using routes that have numeric IDs. (OpenFlights documents \N NULL behavior.)
    airport_freq = Counter()
    valid_routes = []

    for r in read_rows(routes_csv):
        src_id = (r.get("src_airport_id") or "").strip()
        dst_id = (r.get("dst_airport_id") or "").strip()
        if is_int(src_id) and is_int(dst_id):
            airport_freq[src_id] += 1
            airport_freq[dst_id] += 1
            valid_routes.append(r)

    # pick a dense set of airports (appear a lot in routes)
    top_airports = set(aid for aid, _ in airport_freq.most_common(args.max_airports))

    # keep routes fully inside that airport set
    sample_routes = []
    used_airport_ids = set()
    used_airline_ids = set()
    used_equipment_codes = set()

    for r in valid_routes:
        src_id = (r.get("src_airport_id") or "").strip()
        dst_id = (r.get("dst_airport_id") or "").strip()
        if src_id in top_airports and dst_id in top_airports:
            sample_routes.append(r)
            used_airport_ids.add(src_id)
            used_airport_ids.add(dst_id)

            alid = (r.get("airline_id") or "").strip()
            if is_int(alid):
                used_airline_ids.add(alid)

            eq = (r.get("equipment") or "").strip()
            if eq:
                for code in eq.split():
                    used_equipment_codes.add(code)

        if len(sample_routes) >= args.max_routes:
            break

    # airports subset
    airports_all = list(read_rows(airports_csv))
    airports_fields = list(airports_all[0].keys()) if airports_all else []
    airports_sample = [a for a in airports_all if (a.get("airport_id") or "").strip() in used_airport_ids]
    write_rows(out_dir / "airports.csv", airports_fields, airports_sample)

    # airlines subset
    airlines_all = list(read_rows(airlines_csv))
    airlines_fields = list(airlines_all[0].keys()) if airlines_all else []
    airlines_sample = [a for a in airlines_all if (a.get("airline_id") or "").strip() in used_airline_ids]
    write_rows(out_dir / "airlines.csv", airlines_fields, airlines_sample)

    # planes subset: match equipment codes to planes iata/icao
    planes_all = list(read_rows(planes_csv))
    planes_fields = list(planes_all[0].keys()) if planes_all else []
    planes_sample = []
    for p in planes_all:
        iata = (p.get("iata") or "").strip()
        icao = (p.get("icao") or "").strip()
        if (iata and iata in used_equipment_codes) or (icao and icao in used_equipment_codes):
            planes_sample.append(p)
    write_rows(out_dir / "planes.csv", planes_fields, planes_sample)

    # countries subset: OpenFlights airports/airlines store country name (string)
    used_country_names = set()
    for a in airports_sample:
        c = (a.get("country") or "").strip()
        if c:
            used_country_names.add(c)
    for a in airlines_sample:
        c = (a.get("country") or "").strip()
        if c:
            used_country_names.add(c)

    countries_all = list(read_rows(countries_csv))
    countries_fields = list(countries_all[0].keys()) if countries_all else []
    countries_sample = [c for c in countries_all if (c.get("name") or "").strip() in used_country_names]
    write_rows(out_dir / "countries.csv", countries_fields, countries_sample)

    # routes sample last
    route_fields = list(sample_routes[0].keys()) if sample_routes else []
    write_rows(out_dir / "routes.csv", route_fields, sample_routes)

    (out_dir / "README.md").write_text(
        f"""# OpenFlights sample data (airports-only)

Generated from: {in_dir}

Rules:
- Use routes that have numeric src_airport_id and dst_airport_id (join-friendly)
- Pick top {args.max_airports} airports by route frequency
- Keep routes where both endpoints are in that set (cap {args.max_routes})
- Keep referenced airlines (numeric airline_id)
- Keep planes referenced by route equipment codes
- Keep countries referenced by sampled airports/airlines (name match)
""",
        encoding="utf-8"
    )

    print(f"Wrote sample CSVs to: {out_dir}")

if __name__ == "__main__":
    main()