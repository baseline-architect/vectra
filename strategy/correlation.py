from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

import numpy as np
import pandas as pd

from .indicators import pct_change


@dataclass
class ClusterResult:
    correlation_matrix: pd.DataFrame
    clusters: List[Set[str]]
    leaders: Dict[str, str]  # symbol -> leader symbol of its cluster
    allowed_symbols: Set[str]


def compute_correlation_matrix(price_df: pd.DataFrame, window: int) -> pd.DataFrame:
    # price_df: index time, columns symbols, values close prices
    # Use return correlations to be robust to levels
    returns = price_df.pct_change().dropna()
    if len(returns) >= window:
        returns = returns.iloc[-window:]
    return returns.corr()


def _build_clusters(corr: pd.DataFrame, threshold: float) -> List[Set[str]]:
    symbols = list(corr.columns)
    visited: Set[str] = set()
    clusters: List[Set[str]] = []

    for sym in symbols:
        if sym in visited:
            continue
        # BFS/DFS to group correlated symbols
        stack = [sym]
        cluster: Set[str] = set()
        while stack:
            s = stack.pop()
            if s in visited:
                continue
            visited.add(s)
            cluster.add(s)
            # neighbors with corr > threshold (excluding self)
            neighbors = [n for n in symbols if n != s and not np.isnan(corr.loc[s, n]) and corr.loc[s, n] > threshold]
            for n in neighbors:
                if n not in visited:
                    stack.append(n)
        clusters.append(cluster)
    return clusters


def _relative_momentum(prices: pd.Series, lookback: int) -> float:
    if len(prices) < lookback + 1:
        if len(prices) < 2:
            return 0.0
        return float((prices.iloc[-1] / prices.iloc[0]) - 1.0)
    p0 = float(prices.iloc[-(lookback + 1)])
    p1 = float(prices.iloc[-1])
    return (p1 / p0) - 1.0 if p0 > 0 else 0.0


def select_cluster_leaders(price_df: pd.DataFrame, clusters: List[Set[str]], lookback: int) -> Dict[str, str]:
    leaders: Dict[str, str] = {}
    for cluster in clusters:
        best_sym = None
        best_mom = -np.inf
        for sym in cluster:
            mom = _relative_momentum(price_df[sym].dropna(), lookback)
            if mom > best_mom:
                best_mom = mom
                best_sym = sym
        if best_sym is not None:
            for sym in cluster:
                leaders[sym] = best_sym
    return leaders


def correlation_protector(price_df: pd.DataFrame, threshold: float, lookback: int) -> ClusterResult:
    corr = compute_correlation_matrix(price_df, window=lookback)
    clusters = _build_clusters(corr, threshold)
    leaders = select_cluster_leaders(price_df, clusters, lookback)
    allowed = {leader for leader in set(leaders.values())}
    return ClusterResult(correlation_matrix=corr, clusters=clusters, leaders=leaders, allowed_symbols=allowed)
