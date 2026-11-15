"""On-demand geo-velocity between the last stored point and the request."""

import math

_EARTH_RADIUS_KM = 6371.0
_MS_PER_HOUR = 3_600_000


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres on a 6371 km sphere."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    chord = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    arc = 2 * math.atan2(math.sqrt(chord), math.sqrt(1 - chord))
    return _EARTH_RADIUS_KM * arc


def velocity_kmh(
    origin_lat: float,
    origin_lon: float,
    origin_ms: int,
    dest_lat: float,
    dest_lon: float,
    dest_ms: int,
) -> float | None:
    """Kilometres per hour from the origin point to the destination.

    The span is absolute. A zero time span has no velocity and returns ``None``.
    """
    elapsed_ms = abs(dest_ms - origin_ms)
    if elapsed_ms == 0:
        return None
    return haversine_km(origin_lat, origin_lon, dest_lat, dest_lon) / (elapsed_ms / _MS_PER_HOUR)
