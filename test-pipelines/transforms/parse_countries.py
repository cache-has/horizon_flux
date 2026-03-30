import polars as pl
import json


def transform(inputs, params):
    df = inputs["rest_countries"]

    # The REST connector serializes nested JSON objects as strings.
    # Parse each row's nested fields.
    rows = df.to_dicts()
    parsed = []
    for row in rows:
        name = row.get("name", "")
        if isinstance(name, str):
            try:
                name = json.loads(name)
            except (json.JSONDecodeError, TypeError):
                pass
        country_name = name.get("common", str(name)) if isinstance(name, dict) else str(name)

        latlng = row.get("latlng", "[]")
        if isinstance(latlng, str):
            try:
                latlng = json.loads(latlng)
            except (json.JSONDecodeError, TypeError):
                latlng = [0, 0]
        if not isinstance(latlng, list):
            latlng = [0, 0]

        area = float(row.get("area", 0) or 0)
        pop = int(row.get("population", 0) or 0)

        parsed.append({
            "country_name": country_name,
            "region": str(row.get("region", "Unknown")),
            "subregion": str(row.get("subregion", "Unknown")),
            "population": pop,
            "area": area,
            "lat": float(latlng[0]) if len(latlng) > 0 else 0.0,
            "lng": float(latlng[1]) if len(latlng) > 1 else 0.0,
        })

    result = pl.DataFrame(parsed)

    # Population density
    result = result.with_columns(
        (pl.col("population") / pl.col("area")).round(2).alias("pop_density_per_km2")
    ).filter(pl.col("area") > 0)

    return result
