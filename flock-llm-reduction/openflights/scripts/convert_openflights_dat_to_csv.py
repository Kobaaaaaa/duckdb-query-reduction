import csv
from pathlib import Path
import argparse

# OpenFlights .dat files are comma-delimited text with quotes, and use \N to mean NULL. (See OpenFlights docs.)
# We will write real CSVs with headers.
SCHEMAS = {
    "airports.dat": [
        "airport_id","name","city","country","iata","icao",
        "latitude","longitude","altitude_ft","timezone_utc_offset",
        "dst","tz_db_timezone","type","source"
    ],
    "airlines.dat": [
        "airline_id","name","alias","iata","icao","callsign","country","active"
    ],
    "routes.dat": [
        "airline_code","airline_id","src_airport_code","src_airport_id",
        "dst_airport_code","dst_airport_id","codeshare","stops","equipment"
    ],
    "planes.dat": ["name","iata","icao"],
    "countries.dat": ["name","iso_code","dafif_code"],
}

def convert_file(in_path: Path, out_path: Path) -> tuple[int, int]:
    headers = SCHEMAS[in_path.name]
    expected_cols = len(headers)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    bad_rows = 0

    with in_path.open("r", encoding="utf-8", newline="") as f_in, out_path.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        reader = csv.reader(f_in, delimiter=",", quotechar='"', escapechar="\\")
        writer = csv.writer(f_out)

        writer.writerow(headers)

        for row in reader:
            # Normalize OpenFlights NULL marker
            row = [("" if v == r"\N" else v) for v in row]

            if len(row) != expected_cols:
                bad_rows += 1
                # keep going; you can inspect the log file later
                continue

            writer.writerow(row)
            rows_written += 1

    return rows_written, bad_rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Folder with .dat files (e.g. data/original_data)")
    ap.add_argument("--out-dir", required=True, help="Folder for .csv outputs (e.g. data/processed_csv)")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)

    # Write a simple conversion report
    report_lines = []

    for fname in SCHEMAS.keys():
        src = in_dir / fname
        if not src.exists():
            report_lines.append(f"SKIP (missing): {fname}")
            continue

        dst = out_dir / fname.replace(".dat", ".csv")
        rows_written, bad_rows = convert_file(src, dst)
        report_lines.append(f"OK: {fname} -> {dst.name} | rows={rows_written} | bad_rows={bad_rows}")

    report_path = out_dir / "conversion_report.txt"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Wrote CSVs to: {out_dir}")
    print(f"Report: {report_path}")

if __name__ == "__main__":
    main()