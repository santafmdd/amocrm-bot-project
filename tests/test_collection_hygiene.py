from pathlib import Path


def test_tests_subpackages_have_init_files_for_import_hygiene():
    root = Path(__file__).resolve().parent
    required = [
        root / "amocrm_auth" / "__init__.py",
        root / "amocrm_collector" / "__init__.py",
        root / "amocrm_discovery" / "__init__.py",
        root / "deal_analyzer" / "__init__.py",
        root / "ops_storage" / "__init__.py",
    ]
    for path in required:
        assert path.exists(), f"missing package marker: {path}"
