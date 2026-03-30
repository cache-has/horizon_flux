import polars as pl
import json


def transform(inputs, params):
    df = inputs["earthquakes"]

    # The REST connector serializes nested JSON objects as strings.
    # Parse properties and geometry from JSON strings.
    rows = df.to_dicts()
    parsed = []
    for row in rows:
        props = json.loads(row["properties"]) if isinstance(row["properties"], str) else row["properties"]
        geom = json.loads(row["geometry"]) if isinstance(row["geometry"], str) else row["geometry"]
        coords = geom.get("coordinates", [0, 0, 0])
        parsed.append({
            "eq_id": str(row.get("id", "")),
            "magnitude": float(props.get("mag", 0)),
            "place": str(props.get("place", "unknown")),
            "event_type": str(props.get("type", "earthquake")),
            "tsunami": int(props.get("tsunami", 0)),
            "longitude": float(coords[0]) if len(coords) > 0 else 0.0,
            "latitude": float(coords[1]) if len(coords) > 1 else 0.0,
            "depth_km": float(coords[2]) if len(coords) > 2 else 0.0,
        })

    result = pl.DataFrame(parsed)

    # Filter by minimum magnitude from pipeline variables
    min_mag = float(params.get("min_magnitude", 2.5))
    result = result.filter(pl.col("magnitude") >= min_mag)

    return result
