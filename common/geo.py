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


# ---------------- cells and borders (Stage 4) ----------------
# The city is also divided into square cells of `cell_km` (1 km by default, equal to the trust
# radius). Region boundaries lie on cell boundaries, so the 1 km strip next to a region's edge is
# exactly one row of cells. Readings are published per cell, so an aggregator can subscribe to
# just the strip of foreign cells along its border.

def cell_of(x_km: float, y_km: float, cfg: Config) -> str:
    cols = max(1, round(cfg.city_w_km / cfg.cell_km))
    rows = max(1, round(cfg.city_h_km / cfg.cell_km))
    col = min(max(int(x_km // cfg.cell_km), 0), cols - 1)
    row = min(max(int(y_km // cfg.cell_km), 0), rows - 1)
    return f"c-{row}-{col}"


def _rect_distance(a: tuple, b: tuple) -> float:
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return math.hypot(dx, dy)


def distance_to_region(x_km: float, y_km: float, region: str, cfg: Config) -> float:
    """0 inside the region, otherwise the distance to its nearest edge (km)."""
    return _rect_distance((x_km, y_km, x_km, y_km), region_bounds(region, cfg))


def distance_to_own_edge(x_km: float, y_km: float, cfg: Config) -> float:
    """How far a point is from the boundary of the region it belongs to (km)."""
    x0, y0, x1, y1 = region_bounds(region_of(x_km, y_km, cfg), cfg)
    return min(x_km - x0, x1 - x_km, y_km - y0, y1 - y_km)


def border_cells(region: str, cfg: Config, radius_km: float) -> list[tuple[str, str]]:
    """(foreign_region, cell) pairs for the cells outside `region` that touch its border strip."""
    cols = max(1, round(cfg.city_w_km / cfg.cell_km))
    rows = max(1, round(cfg.city_h_km / cfg.cell_km))
    bounds = region_bounds(region, cfg)
    out = []
    for row in range(rows):
        for col in range(cols):
            cell = (col * cfg.cell_km, row * cfg.cell_km, (col + 1) * cfg.cell_km, (row + 1) * cfg.cell_km)
            cx, cy = (cell[0] + cell[2]) / 2, (cell[1] + cell[3]) / 2
            owner = region_of(cx, cy, cfg)
            if owner != region and _rect_distance(cell, bounds) < radius_km - 1e-9:
                out.append((owner, f"c-{row}-{col}"))
    return out


def regions_within(x_km: float, y_km: float, cfg: Config, radius_km: float) -> list[str]:
    """Regions (other than the point's own) whose territory lies within `radius_km` of the point."""
    own = region_of(x_km, y_km, cfg)
    return [r for r in all_regions(cfg) if r != own and distance_to_region(x_km, y_km, r, cfg) <= radius_km]
