import numpy as np


def naive(history: np.ndarray) -> np.ndarray:
    return history[:, -1].astype(float)


def moving_average(history: np.ndarray, window: int = 12) -> np.ndarray:
    return history[:, -window:].mean(axis=1)


def ses(history: np.ndarray, alpha: float = 0.1) -> np.ndarray:
    level = history[:, 0].astype(float)
    for t in range(1, history.shape[1]):
        level = alpha * history[:, t] + (1 - alpha) * level
    return level


def croston(history: np.ndarray, alpha: float = 0.1, variant: str = "croston") -> np.ndarray:
    n_parts, n_months = history.shape
    size = np.zeros(n_parts)
    interval = np.zeros(n_parts)
    since = np.zeros(n_parts)
    started = np.zeros(n_parts, dtype=bool)

    for t in range(n_months):
        since += 1
        demand = history[:, t]
        hit = demand > 0

        first = hit & ~started
        size[first] = demand[first]
        interval[first] = since[first]

        later = hit & started
        size[later] = alpha * demand[later] + (1 - alpha) * size[later]
        interval[later] = alpha * since[later] + (1 - alpha) * interval[later]

        started |= hit
        since[hit] = 0

    forecast = np.where(started, size / np.maximum(interval, 1e-9), 0.0)
    if variant == "sba":
        forecast *= 1 - alpha / 2
    return forecast


def tsb(history: np.ndarray, alpha: float = 0.1, beta: float = 0.1) -> np.ndarray:
    n_parts, n_months = history.shape
    probability = np.zeros(n_parts)
    size = np.zeros(n_parts)
    started = np.zeros(n_parts, dtype=bool)

    for t in range(n_months):
        demand = history[:, t]
        hit = demand > 0

        probability[started] = beta * hit[started] + (1 - beta) * probability[started]
        update = hit & started
        size[update] = alpha * demand[update] + (1 - alpha) * size[update]

        first = hit & ~started
        probability[first] = 1 / (t + 1)
        size[first] = demand[first]
        started |= first

    return probability * size
