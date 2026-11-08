"""City grid: maps positions (km) to regions and to latitude/longitude for maps."""
from __future__ import annotations

import math

from common.config import Config


def region_id(row: int, col: int) -> str:
    return f"r-{row}-{col}"


def region_of(x_km: float, y_km: float, cfg: Config) -> str:
    cell_w = cfg.city_w_km / cfg.grid_cols
    cell_h = cfg.city_h_km / cfg.grid_rows
    col = min(max(int(x_km // cell_w), 0), cfg.grid_cols - 1)
    row = min(max(int(y_km // cell_h), 0), cfg.grid_rows - 1)
    return region_id(row, col)


def all_regions(cfg: Config) -> list[str]:
    return [region_id(r, c) for r in range(cfg.grid_rows) for c in range(cfg.grid_cols)]


def region_bounds(region: str, cfg: Config) -> tuple[float, float, float, float]:
    """(x0, y0, x1, y1) in km."""
    _, row, col = region.split("-")
    cell_w = cfg.city_w_km / cfg.grid_cols
    cell_h = cfg.city_h_km / cfg.grid_rows
    r, c = int(row), int(col)
    return c * cell_w, r * cell_h, (c + 1) * cell_w, (r + 1) * cell_h


def to_latlon(x_km: float, y_km: float, cfg: Config) -> tuple[float, float]:
    lat = cfg.origin_lat + y_km / 111.32
    lon = cfg.origin_lon + x_km / (111.32 * math.cos(math.radians(cfg.origin_lat)))
    return round(lat, 6), round(lon, 6)
