from pathlib import Path

from scripts.apply_migrations import (
    migration_checksum,
    order_migration_paths,
)


def test_order_migration_paths_puts_init_first() -> None:
    analytics = Path("002_analytics.sql")
    bootstrap = Path("init.sql")
    later = Path("010_later.sql")

    assert order_migration_paths([later, analytics, bootstrap]) == [
        bootstrap,
        analytics,
        later,
    ]


def test_migration_checksum_is_content_sensitive() -> None:
    assert migration_checksum("SELECT 1;") == migration_checksum("SELECT 1;")
    assert migration_checksum("SELECT 1;") != migration_checksum("SELECT 2;")
