import requests
import pandas as pd
from datetime import date
from pathlib import Path

def getWaterTempData(startDate = "2022-01-01", endDate = date.today().strftime("%Y-%m-%d"), saveToCsv=True):
    url = f"https://api.sodertalje.se/GETALLwatertemp?start={startDate}&end={endDate}"
    resp = requests.get(url)
    data = resp.json()
    
    
    df = pd.DataFrame(data)
    df = df[(df["type"] == "Watertemp")]
    df = df.dropna()

    df["formatted_time"] = pd.to_datetime(df["formatted_time"], format="%b %d %Y %H:%M:%S").dt.floor("min")
    df["date"] = df["formatted_time"].dt.date
    df = df.sort_values(["alias", "formatted_time"]).reset_index(drop=True)
    #midday = df["formatted_time"].dt.hour.between(10, 15)  # 10:00–15:59
    #df = df[midday]

    counts = (
    df.groupby(["alias", "date"])
      .size()
      .reset_index(name="n_rows")
    )

    # We only want the cases where a date repeats (more than 1 row that day):
    repeats = counts[counts["n_rows"] > 1].sort_values(["alias", "date"])
    #print(repeats)
    dup_rows = df.merge(repeats[["alias", "date"]], on=["alias", "date"], how="inner")

    noon = dup_rows["formatted_time"].dt.normalize() + pd.Timedelta(hours=12)
    #print(noon)
    dup_rows["delta_to_noon"] = (dup_rows["formatted_time"] - noon).abs()
    closest_dup = (
    dup_rows.sort_values(["alias", "date", "delta_to_noon", "formatted_time"])
            .drop_duplicates(subset=["alias", "date"], keep="first")
    )

    # all non-duplicate (alias,date) rows:
    non_dup = df.merge(repeats[["alias", "date"]], on=["alias", "date"], how="left", indicator=True)
    non_dup = non_dup[non_dup["_merge"] == "left_only"].drop(columns=["_merge"])

    # final dataset
    final_df = (
        pd.concat([non_dup, closest_dup], ignore_index=True)
        .sort_values(["alias", "formatted_time"])
        .reset_index(drop=True)
    )

    root_dir = Path().absolute()
    if root_dir.parts[-2:] == ('notebooks', 'algae_bloom'):
        root_dir = Path(*root_dir.parts[:-2])
    root_dir = str(root_dir)

    print(f"Root dir: {root_dir}")

    data_dir = Path(root_dir) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if saveToCsv:
        final_df[["temp_water","formatted_time","alias","latitude","longitude"]].to_csv(
            data_dir / "watertemp_midday_deduped.csv", index=False, encoding="utf-8"
        )

    out = df[["temp_water","formatted_time","alias","latitude","longitude"]]
    out.to_csv(data_dir / "watertemp_midday.csv", index=False, encoding="utf-8")
    print("Saved:", len(out), "rows to", data_dir / "watertemp_midday.csv")

    return final_df[["temp_water","formatted_time","alias","latitude","longitude"]]





getWaterTempData()