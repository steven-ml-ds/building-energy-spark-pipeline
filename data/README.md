# Data

CSV files are excluded from version control (see `.gitignore`).  
Place the following files in this directory before running the notebooks.

---

## Files

### `meters.csv`

Hourly energy consumption readings from building meters.

| Column | Type | Description |
|--------|------|-------------|
| `building_id` | int | Unique building identifier |
| `meter_type` | string | Meter category (`c` = chilled water, `e` = electricity, `h` = hot water, `s` = steam) |
| `ts` | timestamp | Hourly reading timestamp |
| `value` | double | Energy consumption reading |
| `row_id` | int | Row identifier |

---

### `building_information.csv`

Static metadata for each building in the dataset.

| Column | Type | Description |
|--------|------|-------------|
| `site_id` | int | Campus/site identifier |
| `building_id` | int | Unique building identifier |
| `primary_use` | string | Primary building function (Education, Office, Residential, etc.) |
| `square_feet` | int | Total floor area in square feet |
| `floor_count` | int | Number of floors |
| `year_built` | int | Year of construction |
| `row_id` | int | Row identifier |
| `latent_y` | double | Latent feature (anonymised) |
| `latent_s` | double | Latent feature (anonymised) |
| `latent_r` | double | Latent feature (anonymised) |

---

### `new_building_information.csv`

Updated building metadata used by the streaming prediction pipeline (notebook 03).  
Same schema as `building_information.csv`.

---

### `weather.csv`

Hourly weather observations per site.

| Column | Type | Description |
|--------|------|-------------|
| `site_id` | int | Campus/site identifier |
| `timestamp` | timestamp | Hourly observation timestamp |
| `air_temperature` | double | Dry-bulb temperature (°C) |
| `cloud_coverage` | double | Cloud coverage (oktas, 0–9) |
| `dew_temperature` | double | Dew point temperature (°C) |
| `sea_level_pressure` | double | Sea level pressure (hPa) |
| `wind_direction` | double | Wind direction (degrees, 0–360) |
| `wind_speed` | double | Wind speed (m/s) |
