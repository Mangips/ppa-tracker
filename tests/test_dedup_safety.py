import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("NEWSAPI_KEY", "TEST_NO_API_CALLS")
os.environ.setdefault("ENVIRONMENT", "TEST")
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import pipeline as p  # noqa: E402


class DuplicateSafetyTests(unittest.TestCase):
    def test_close_capacity_but_different_named_projects_do_not_match(self):
        valkyria = {
            "id": 1978,
            "event_type": "U",
            "canonical_id": 1949,
            "buyer": "EDP",
            "seller": "Zelestra",
            "country": "Spain",
            "capacity_mw": 150.0,
            "date_agreement": "2026-10",
            "tenure_years": None,
            "notes": (
                "Valkyria (Castilla-La Mancha) solar plant with 90.45 MW BESS "
                "integration (362 MWh storage). Hybridization of existing operational plant."
            ),
        }
        trujillo = {
            "buyer": "EDP",
            "seller": "Zelestra",
            "country": "Spain",
            "capacity_mw": 170.0,
            "date_agreement": "2026-10",
            "tenure_years": None,
            "notes": (
                "Trujillo (Extremadura) solar plant with 400 MWh BESS integration. "
                "First hybrid PPA in Spain combining solar and battery storage."
            ),
        }
        self.assertTrue(p._capacity_close(150.0, 170.0))
        self.assertFalse(
            p.extract_project_tokens(valkyria["notes"], "EDP", "Zelestra")
            & p.extract_project_tokens(trujillo["notes"], "EDP", "Zelestra")
        )
        self.assertIsNone(p.find_similar_duplicate([valkyria], trujillo))

    def test_thousand_fold_capacity_discrepancy_is_not_auto_matched(self):
        small = {
            "id": 1939,
            "buyer": "Nova Coop",
            "seller": "Nova Aeg",
            "country": "Italy",
            "capacity_mw": 4.071,
            "date_agreement": "2025",
            "tenure_years": None,
            "notes": "PPA includes renewable energy guarantees of origin in Vercelli.",
        }
        large = {
            "buyer": "Nova Coop",
            "seller": "Nova Aeg",
            "country": "Italy",
            "capacity_mw": 4071.0,
            "date_agreement": "2025",
            "tenure_years": None,
            "notes": "Vercelli Larizzate photovoltaic plant.",
        }
        self.assertFalse(p._capacity_close(4.071, 4071.0))
        self.assertIsNone(p.find_similar_duplicate([small], large))

    def test_empty_tenure_values_do_not_crash_missing_capacity_match(self):
        existing = {
            "id": 555,
            "event_type": "N",
            "canonical_id": None,
            "buyer": "Example Buyer",
            "seller": "Example Seller",
            "country": "Spain",
            "capacity_mw": None,
            "date_agreement": "2020",
            "tenure_years": "",
            "notes": "",
        }
        new = {
            "buyer": "Example Buyer",
            "seller": "Example Seller",
            "country": "Spain",
            "capacity_mw": None,
            "date_agreement": "2026",
            "tenure_years": "",
            "notes": "",
        }
        self.assertIsNone(p.find_similar_duplicate([existing], new))

    def test_resolver_returns_original_new_record_not_intermediate_update(self):
        rows = {
            100: {"id": 100, "event_type": "N", "canonical_id": None},
            101: {"id": 101, "event_type": "U", "canonical_id": 100},
            102: {"id": 102, "event_type": "U", "canonical_id": 101},
            103: {"id": 103, "event_type": "U", "canonical_id": None},
        }
        self.assertEqual(p.resolve_canonical_deal_id(rows[102], rows), 100)
        self.assertIsNone(p.resolve_canonical_deal_id(rows[103], rows))

    def test_aggregate_row_only_dropped_when_explicit_and_components_match(self):
        deals = [
            {
                "buyer": "EDP", "seller": "Zelestra", "country": "Spain",
                "capacity_mw": 150.0, "notes": "Valkyria solar PPA project.",
            },
            {
                "buyer": "EDP", "seller": "Zelestra", "country": "Spain",
                "capacity_mw": 170.0, "notes": "Trujillo solar PPA project.",
            },
            {
                "buyer": "EDP", "seller": "Zelestra", "country": "Spain",
                "capacity_mw": 320.0,
                "notes": "Combined capacity across two projects: 320 MW.",
            },
        ]
        result = p._drop_aggregate_rows(deals)
        self.assertEqual(len(result), 2)
        self.assertEqual({d["capacity_mw"] for d in result}, {150.0, 170.0})

    def test_similar_capacity_records_are_kept_when_project_specific(self):
        deals = [
            {
                "buyer": "EDP", "seller": "Zelestra", "country": "Spain",
                "capacity_mw": 150.0, "notes": "Valkyria solar PPA project.",
            },
            {
                "buyer": "EDP", "seller": "Zelestra", "country": "Spain",
                "capacity_mw": 170.0, "notes": "Trujillo solar PPA project.",
            },
        ]
        self.assertEqual(len(p._drop_aggregate_rows(deals)), 2)

    def test_unlabelled_sum_match_is_retained_for_review(self):
        deals = [
            {"buyer": "EDP", "seller": "Zelestra", "country": "Spain", "capacity_mw": 150.0, "notes": "Valkyria"},
            {"buyer": "EDP", "seller": "Zelestra", "country": "Spain", "capacity_mw": 170.0, "notes": "Trujillo"},
            {"buyer": "EDP", "seller": "Zelestra", "country": "Spain", "capacity_mw": 320.0, "notes": "Project with 320 MW capacity"},
        ]
        self.assertEqual(len(p._drop_aggregate_rows(deals)), 3)


if __name__ == "__main__":
    unittest.main()
