"""Riegel-based race time prediction."""

CANONICAL = [5000, 10000, 21100, 42195]
CANONICAL_LABELS = {5000: "5 km", 10000: "10 km", 21100: "21.1 km", 42195: "42.2 km"}
EXPONENT = 1.06


def predict_time(known_dist: float, known_time: float, target_dist: float) -> float:
    """Predict time at target_dist from a known result using Riegel's formula."""
    return known_time * (target_dist / known_dist) ** EXPONENT


def format_time(seconds: float) -> str:
    s = int(round(seconds))
    if s >= 3600:
        return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"
    return f"{s // 60}:{s % 60:02d}"
