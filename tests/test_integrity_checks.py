"""
지표 정합성 검증기 단위 테스트
============================================================
pytest tests/test_integrity_checks.py -v
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

# 프로젝트 루트 → scripts 경로 추가
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from run_integrity_checks import (
    CheckStatus,
    IntegrityCheckResult,
    MetricsIntegrityChecker,
    Severity,
    calc_iqr_bounds,
    calc_z_score,
    detect_trend_break,
    generate_demo_data,
    load_config,
)


# ══════════════════════════════════════════════════════════
# Fixture
# ══════════════════════════════════════════════════════════


@pytest.fixture
def checker() -> MetricsIntegrityChecker:
    return MetricsIntegrityChecker()


@pytest.fixture
def demo_data():
    return generate_demo_data()


@pytest.fixture
def perfect_share_data():
    """점유율 합계가 정확히 100%인 데이터"""
    return [
        {"year_month": "2025-12-01", "card_company": "A", "market_share_pct": 40.0, "share_change_pp": 0.5},
        {"year_month": "2025-12-01", "card_company": "B", "market_share_pct": 35.0, "share_change_pp": -0.3},
        {"year_month": "2025-12-01", "card_company": "C", "market_share_pct": 25.0, "share_change_pp": -0.2},
    ]


@pytest.fixture
def broken_share_data():
    """점유율 합계가 100%가 아닌 데이터"""
    return [
        {"year_month": "2025-12-01", "card_company": "A", "market_share_pct": 40.0},
        {"year_month": "2025-12-01", "card_company": "B", "market_share_pct": 35.0},
        {"year_month": "2025-12-01", "card_company": "C", "market_share_pct": 20.0},  # 합계 95%
    ]


# ══════════════════════════════════════════════════════════
# 유틸리티 함수 테스트
# ══════════════════════════════════════════════════════════


class TestStatisticsUtils:
    def test_z_score_normal(self):
        values = [10, 20, 30, 40, 50]
        z = calc_z_score(30, values)
        assert z == 0.0  # 평균값은 Z=0

    def test_z_score_positive(self):
        values = [10, 20, 30, 40, 50]
        z = calc_z_score(50, values)
        assert z is not None
        assert z > 0

    def test_z_score_insufficient_data(self):
        z = calc_z_score(10, [10])
        assert z is None

    def test_z_score_zero_stdev(self):
        z = calc_z_score(5, [5, 5, 5, 5])
        assert z == 0.0

    def test_iqr_bounds(self):
        values = list(range(1, 101))
        lower, upper = calc_iqr_bounds(values)
        assert lower < 1  # Q1 - 1.5*IQR
        assert upper > 100  # Q3 + 1.5*IQR

    def test_trend_break_detection(self):
        # 안정적인 시계열 + 급변
        values = [100, 102, 101, 103, 100, 300, 101, 99]
        breaks = detect_trend_break(values, window=3, threshold_sigma=2.0)
        assert 5 in breaks  # 300이 급변 지점

    def test_trend_break_stable(self):
        # 충분히 완만한 시계열 — 노이즈 없이 일정한 값
        values = [100, 100, 100, 100, 100, 100, 100, 100]
        breaks = detect_trend_break(values, window=3)
        assert len(breaks) == 0


# ══════════════════════════════════════════════════════════
# 설정 로더 테스트
# ══════════════════════════════════════════════════════════


class TestConfig:
    def test_default_config(self):
        config = load_config(None)
        assert "thresholds" in config
        assert "sum_integrity" in config["thresholds"]

    def test_yaml_config_load(self):
        config_path = Path(__file__).resolve().parent.parent / "config" / "thresholds.yaml"
        if config_path.exists():
            config = load_config(str(config_path))
            assert config["thresholds"]["sum_integrity"]["tolerance"] == 1


# ══════════════════════════════════════════════════════════
# 합계 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestSumIntegrity:
    def test_pass_when_equal(self, checker):
        data = [
            {"year_month": "2025-12", "card_company": "A", "usage_amount": 1000},
            {"year_month": "2025-12", "card_company": "B", "usage_amount": 2000},
        ]
        results = checker.check_sum_integrity(data)
        assert all(r.is_passed for r in results)

    def test_consistent_with_demo_data(self, checker, demo_data):
        results = checker.check_sum_integrity(demo_data["usage"])
        assert len(results) > 0
        assert all(r.is_passed for r in results)


# ══════════════════════════════════════════════════════════
# 비율 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestRatioIntegrity:
    def test_market_share_100(self, checker, perfect_share_data):
        results = checker.check_market_share_integrity(perfect_share_data)
        assert len(results) == 1
        assert results[0].is_passed

    def test_market_share_not_100(self, checker, broken_share_data):
        results = checker.check_market_share_integrity(broken_share_data)
        assert len(results) == 1
        assert not results[0].is_passed

    def test_category_ratio(self, checker):
        data = [
            {"year_month": "2025-12", "card_company": "A", "business_category": "음식점", "category_share_pct": 50.0},
            {"year_month": "2025-12", "card_company": "A", "business_category": "교통", "category_share_pct": 50.0},
        ]
        results = checker.check_category_ratio_integrity(data)
        assert len(results) == 1
        assert results[0].is_passed


# ══════════════════════════════════════════════════════════
# 산출식 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestFormulaIntegrity:
    def test_mom_reverse_calc(self, checker):
        """MoM = 10%, 당월 110, 전월 100 → 역산 = 110/1.1 = 100"""
        data = [{
            "year_month": "2025-12", "card_company": "A",
            "current_amount": 110, "prev_month_amount": 100,
            "mom_growth_rate": 10.0,
        }]
        results = checker.check_formula_mom(data)
        assert len(results) == 1
        assert results[0].is_passed

    def test_yoy_reverse_calc(self, checker):
        data = [{
            "year_month": "2025-12", "card_company": "A",
            "current_amount": 120, "prev_year_amount": 100,
            "yoy_growth_rate": 20.0,
        }]
        results = checker.check_formula_yoy(data)
        assert len(results) == 1
        assert results[0].is_passed

    def test_formula_skips_null(self, checker):
        data = [{
            "year_month": "2025-01", "card_company": "A",
            "current_amount": 100, "prev_month_amount": None,
            "mom_growth_rate": None,
        }]
        results = checker.check_formula_mom(data)
        assert len(results) == 0


# ══════════════════════════════════════════════════════════
# 범위 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestRangeIntegrity:
    def test_activation_in_range(self, checker):
        data = [
            {"year_month": "2025-12", "card_company": "A", "activation_rate": 72.5},
        ]
        results = checker.check_range_activation(data)
        assert results[0].is_passed

    def test_activation_out_of_range(self, checker):
        data = [
            {"year_month": "2025-12", "card_company": "A", "activation_rate": 105.0},
        ]
        results = checker.check_range_activation(data)
        assert not results[0].is_passed

    def test_activation_negative(self, checker):
        data = [
            {"year_month": "2025-12", "card_company": "A", "activation_rate": -5.0},
        ]
        results = checker.check_range_activation(data)
        assert not results[0].is_passed


# ══════════════════════════════════════════════════════════
# 연속성 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestContinuityIntegrity:
    def test_continuous_months(self, checker):
        data = [
            {"year_month": "2025-01-01", "card_company": "A", "total_usage_amount": 100},
            {"year_month": "2025-02-01", "card_company": "A", "total_usage_amount": 110},
            {"year_month": "2025-03-01", "card_company": "A", "total_usage_amount": 120},
        ]
        results = checker.check_continuity(data)
        assert all(r.is_passed for r in results)

    def test_missing_month(self, checker):
        data = [
            {"year_month": "2025-01-01", "card_company": "A", "total_usage_amount": 100},
            {"year_month": "2025-03-01", "card_company": "A", "total_usage_amount": 120},
            # 2025-02 누락
        ]
        results = checker.check_continuity(data)
        assert any(not r.is_passed for r in results)


# ══════════════════════════════════════════════════════════
# 통계 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestStatisticalIntegrity:
    def test_no_anomaly(self, checker):
        data = [
            {"year_month": f"2025-{m:02d}-01", "card_company": "A", "total_usage_amount": 100 + m}
            for m in range(1, 13)
        ]
        results = checker.check_statistical_anomaly(data)
        # Z-score 이상치 비율이 5% 미만이어야 함
        summary_result = [r for r in results if "비율" in r.check_name]
        assert len(summary_result) == 1
        assert summary_result[0].is_passed

    def test_demo_data_anomaly(self, checker, demo_data):
        results = checker.check_statistical_anomaly(demo_data["monthly_usage"])
        assert len(results) > 0


# ══════════════════════════════════════════════════════════
# 교차 정합성 테스트
# ══════════════════════════════════════════════════════════


class TestCrossKPIIntegrity:
    def test_consistent_direction(self, checker):
        share = [{"year_month": "2025-12", "card_company": "A", "share_change_pp": 1.0}]
        growth = [{"year_month": "2025-12", "card_company": "A", "mom_growth_rate": 5.0}]
        results = checker.check_cross_kpi_consistency(share, growth)
        assert all(r.is_passed for r in results)

    def test_inconsistent_direction(self, checker):
        share = [{"year_month": "2025-12", "card_company": "A", "share_change_pp": 2.0}]
        growth = [{"year_month": "2025-12", "card_company": "A", "mom_growth_rate": -5.0}]
        results = checker.check_cross_kpi_consistency(share, growth)
        assert any(not r.is_passed for r in results)


# ══════════════════════════════════════════════════════════
# 리포트 내보내기 테스트
# ══════════════════════════════════════════════════════════


class TestExport:
    def test_csv_export(self, checker, demo_data):
        checker.check_sum_integrity(demo_data["usage"])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = checker.export_to_csv(tmpdir)
            assert os.path.exists(path)
            assert path.endswith(".csv")

    def test_json_export(self, checker, demo_data):
        checker.check_sum_integrity(demo_data["usage"])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = checker.export_to_json(tmpdir)
            assert os.path.exists(path)
            with open(path, "r") as f:
                data = json.load(f)
            assert "total_checks" in data
            assert "all_checks" in data

    def test_html_export(self, checker, demo_data):
        checker.check_sum_integrity(demo_data["usage"])
        checker.check_market_share_integrity(demo_data["share"])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = checker.export_to_html(tmpdir)
            assert os.path.exists(path)
            content = open(path, "r").read()
            assert "정합성 검증 리포트" in content


# ══════════════════════════════════════════════════════════
# 통합 테스트
# ══════════════════════════════════════════════════════════


class TestIntegration:
    def test_full_pipeline(self, checker, demo_data):
        """10종 검증 전체 파이프라인 실행"""
        checker.check_sum_integrity(demo_data["usage"])
        checker.check_market_share_integrity(demo_data["share"])
        checker.check_category_ratio_integrity(demo_data["category"])
        checker.check_formula_mom(demo_data["growth"])
        checker.check_formula_yoy(demo_data["growth"])
        checker.check_range_activation(demo_data["activation"])
        checker.check_continuity(demo_data["monthly_usage"])
        checker.check_statistical_anomaly(demo_data["monthly_usage"])
        checker.check_trend_breaks(demo_data["monthly_usage"])
        checker.check_cross_kpi_consistency(demo_data["share"], demo_data["growth"])

        summary = checker.get_summary()

        assert summary["total_checks"] > 0
        assert 0 <= summary["overall_pass_rate"] <= 100
        assert summary["overall_status"] in ("PASS", "WARNING", "CRITICAL")
        assert len(summary["by_category"]) >= 5

    def test_summary_structure(self, checker, demo_data):
        checker.check_sum_integrity(demo_data["usage"])
        summary = checker.get_summary()

        required_keys = [
            "check_date", "total_checks", "passed", "failed",
            "critical_failures", "overall_pass_rate", "overall_status",
            "by_category", "failed_checks",
        ]
        for key in required_keys:
            assert key in summary, f"Missing key: {key}"

    def test_dataclass_serialization(self):
        result = IntegrityCheckResult(
            check_name="test", check_category="test",
            severity="INFO", expected_value=100,
            actual_value=100, difference=0,
            tolerance=0, status="PASS", detail="test",
        )
        from dataclasses import asdict
        d = asdict(result)
        assert isinstance(d, dict)
        assert d["check_name"] == "test"
