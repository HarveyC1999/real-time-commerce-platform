from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable
from pathlib import Path

import psycopg

from app.config import (
    Settings,
    get_settings,
    postgres_connection_string,
)

logger = logging.getLogger(__name__)

MIGRATIONS_DIRECTORY = Path("infrastructure/postgres")

CREATE_MIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


def order_migration_paths(paths: Iterable[Path]) -> list[Path]:
    return sorted(
        paths,
        key=lambda path: (
            path.name != "init.sql",
            path.name,
        ),
    )


def ordered_migration_paths(directory: Path) -> list[Path]:
    """Return bootstrap SQL first, followed by versioned migrations."""

    return order_migration_paths(directory.glob("*.sql"))


def migration_checksum(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def apply_migrations(
    *,
    settings: Settings | None = None,
    directory: Path = MIGRATIONS_DIRECTORY,
) -> None:
    """Apply immutable SQL migrations transactionally."""

    migration_paths = ordered_migration_paths(directory)
    if not migration_paths:
        raise RuntimeError(f"No SQL migrations found in {directory}.")

    with psycopg.connect(
        postgres_connection_string(
            settings or get_settings()
        )
    ) as connection:
        connection.execute(CREATE_MIGRATION_TABLE_SQL)

        for path in migration_paths:
            sql = path.read_text(encoding="utf-8")
            checksum = migration_checksum(sql)
            row = connection.execute(
                """
                SELECT checksum
                FROM schema_migrations
                WHERE version = %s
                """,
                (path.name,),
            ).fetchone()

            if row is not None:
                if row[0] != checksum:
                    raise RuntimeError(
                        "Applied migration checksum changed: "
                        f"{path.name}"
                    )

                logger.info("Migration already applied: %s", path.name)
                continue

            connection.execute(sql)
            connection.execute(
                """
                INSERT INTO schema_migrations (version, checksum)
                VALUES (%s, %s)
                """,
                (path.name, checksum),
            )
            logger.info("Migration applied: %s", path.name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    apply_migrations()


if __name__ == "__main__":
    main()
