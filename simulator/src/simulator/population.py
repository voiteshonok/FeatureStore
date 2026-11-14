"""Card identities and the merchant population.

Cards are not stored. A PAN and a home are pure functions of the card index.
"""

import math
import random
from dataclasses import dataclass

from simulator.config import SimulatorConfig

_KM_PER_DEG = 111.32


def pan_for_index(index: int, *, bin: str = "411111", length: int = 16) -> str:
    """Build a Luhn-valid PAN for ``index`` without storing the card."""
    if index < 0:
        msg = "card index must be non-negative"
        raise ValueError(msg)
    body_width = length - len(bin) - 1
    if body_width < 1 or index >= 10**body_width:
        msg = f"card index {index} does not fit in a {length}-digit PAN"
        raise ValueError(msg)
    body = f"{bin}{index:0{body_width}d}"
    return body + _luhn_check_digit(body)


def card_home(index: int, config: SimulatorConfig) -> tuple[float, float]:
    """Return a stable lat/lon within ``card_home_sigma_km`` of the card's city."""
    city = config.cities[index % len(config.cities)]
    lat_off = (_signed_unit(index, 1) * config.card_home_sigma_km) / _KM_PER_DEG
    lon_scale = _KM_PER_DEG * max(math.cos(math.radians(city.lat)), 0.2)
    lon_off = (_signed_unit(index, 2) * config.card_home_sigma_km) / lon_scale
    return _clamp_lat(city.lat + lat_off), _clamp_lon(city.lon + lon_off)


def sample_zipf(rng: random.Random, alpha: float, n: int) -> int:
    """Return an integer in ``1..n`` with probability proportional to ``k ** -alpha``.

    Rejection sampling (Devroye) so the weight table is never materialised.
    """
    exponent = -1.0 / (alpha - 1.0)
    bound = 2.0 ** (alpha - 1.0)
    while True:
        unit = rng.random()
        if unit <= 0.0:
            continue
        proposed = unit**exponent
        if proposed > n:
            continue
        draw = int(proposed)
        if draw < 1:
            continue
        lifted = (1.0 + 1.0 / draw) ** (alpha - 1.0)
        accept = rng.random() * draw * (lifted - 1.0) / (bound - 1.0)
        if accept <= lifted / bound:
            return draw


@dataclass(frozen=True)
class Merchant:
    """One synthetic merchant."""

    merchant_id: str
    city_index: int
    lat: float
    lon: float
    decline_rate: float


def build_merchants(config: SimulatorConfig, rng: random.Random) -> tuple[Merchant, ...]:
    """Draw the merchant list from ``rng`` before the payment stream uses it."""
    spread = (config.decline_rate_max - config.decline_rate_min) / 6
    merchants: list[Merchant] = []
    for index in range(config.n_merchants):
        city = config.cities[index % len(config.cities)]
        lat_scale = config.merchant_sigma_km / _KM_PER_DEG
        lon_scale = lat_scale / max(math.cos(math.radians(city.lat)), 0.2)
        lat = _clamp_lat(city.lat + _bounded_gauss(rng, lat_scale))
        lon = _clamp_lon(city.lon + _bounded_gauss(rng, lon_scale))
        rate = rng.gauss(config.decline_rate_mean, spread)
        rate = min(config.decline_rate_max, max(config.decline_rate_min, rate))
        merchants.append(
            Merchant(
                merchant_id=f"m{index}",
                city_index=index % len(config.cities),
                lat=lat,
                lon=lon,
                decline_rate=rate,
            )
        )
    return tuple(merchants)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    radius = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    chord = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lon / 2) ** 2
    return 2 * radius * math.asin(min(1.0, math.sqrt(chord)))


def _luhn_check_digit(body: str) -> str:
    total = 0
    for position, char in enumerate(reversed(body), start=1):
        digit = ord(char) - 48
        if position % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return str((10 - (total % 10)) % 10)


def _signed_unit(index: int, salt: int) -> float:
    """Deterministic value in ``[-1, 1)`` from the card index."""
    mixed = (index * 0x9E3779B1 + salt * 0x85EBCA6B) & 0xFFFFFFFF
    return (mixed / 0x100000000) * 2 - 1


def _bounded_gauss(rng: random.Random, sigma: float) -> float:
    """Gaussian offset kept inside four standard deviations."""
    return min(4 * sigma, max(-4 * sigma, rng.gauss(0, sigma)))


def _clamp_lat(lat: float) -> float:
    return max(-90.0, min(90.0, lat))


def _clamp_lon(lon: float) -> float:
    return max(-180.0, min(180.0, lon))
