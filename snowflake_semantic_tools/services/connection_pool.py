"""
Thread-safe Connection Pool for Parallel Generation.

Manages a pool of Snowflake ConnectionManager instances for concurrent
semantic view generation. Supports warmup (fail-fast auth validation),
checkout/checkin semantics, and guaranteed cleanup.
"""

import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("connection_pool")


class ConnectionPool:
    """
    Thread-safe pool of Snowflake ConnectionManagers.

    Each ConnectionManager maintains one persistent connection.
    Workers checkout a manager, use it for one or more views,
    then return it for reuse by another worker.
    """

    def __init__(self, config: SnowflakeConfig, size: int):
        self._config = config
        self._size = size
        self._pool: queue.Queue[ConnectionManager] = queue.Queue(maxsize=size)
        self._all_managers: List[ConnectionManager] = []
        self._closed = False

    @property
    def size(self) -> int:
        return self._size

    def warmup(self) -> None:
        """
        Open all connections in parallel and validate with SELECT 1.

        Raises:
            RuntimeError: If any connection fails to authenticate.
        """
        logger.info(f"Warming up {self._size} connection(s)...")
        managers: List[ConnectionManager] = []
        errors: List[str] = []

        def _open_one(idx: int) -> ConnectionManager:
            cm = ConnectionManager(config=self._config)
            with cm.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
            return cm

        with ThreadPoolExecutor(max_workers=self._size) as executor:
            futures = {executor.submit(_open_one, i): i for i in range(self._size)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    cm = future.result(timeout=30)
                    managers.append(cm)
                except Exception as e:
                    errors.append(f"Connection {idx + 1}: {e}")

        if errors:
            for cm in managers:
                try:
                    cm.close()
                except Exception:
                    pass
            raise RuntimeError(
                f"Failed to establish {len(errors)} of {self._size} connection(s):\n"
                + "\n".join(f"  {err}" for err in errors)
            )

        self._all_managers = managers
        for cm in managers:
            self._pool.put(cm)
        logger.info(f"Connection pool ready ({self._size} connection(s))")

    def checkout(self, timeout: float = 60.0) -> ConnectionManager:
        """
        Get a ConnectionManager from the pool (blocking).

        Args:
            timeout: Max seconds to wait for an available connection.

        Returns:
            A ConnectionManager ready for use.

        Raises:
            queue.Empty: If no connection available within timeout.
        """
        return self._pool.get(timeout=timeout)

    def checkin(self, cm: ConnectionManager) -> None:
        """Return a ConnectionManager to the pool."""
        if not self._closed:
            self._pool.put(cm)

    def close_all(self) -> None:
        """Close all connections and drain the pool."""
        self._closed = True
        for cm in self._all_managers:
            try:
                cm.close()
            except Exception:
                pass
        self._all_managers.clear()
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except queue.Empty:
                break
        logger.debug("Connection pool closed")
