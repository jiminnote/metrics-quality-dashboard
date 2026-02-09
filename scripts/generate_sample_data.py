"""
샘플 데이터 생성기 — PostgreSQL 적재용
============================================================
여신금융협회 신용카드 이용실적 데이터를 모사하여
credit_card_usage / card_issuance_stats 테이블에 적재합니다.

사용법:
  # SQL INSERT 파일 생성
  python scripts/generate_sample_data.py --format sql --output data/seed.sql

  # CSV 파일 생성
  python scripts/generate_sample_data.py --format csv --output data/

  # PostgreSQL 직접 적재
  python scripts/generate_sample_data.py --db-url postgresql://user:pass@localhost:5432/metrics
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import random
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Tuple

# ──────────────────────────────────────────────────────────
# 상수 정의
# ──────────────────────────────────────────────────────────

CARD_COMPANIES = [
    "신한카드", "삼성카드", "KB국민카드", "현대카드",
    "우리카드", "하나카드", "롯데카드", "BC카드",
]

BUSINESS_CATEGORIES = [
    "음식점", "주유소", "대형마트", "온라인쇼핑",
    "교통", "의료", "교육", "여행/숙박", "보험", "기타",
]

# 카드사별 기본 월 이용금액 (백만원)
BASE_AMOUNTS: Dict[str, int] = {
    "신한카드": 4_200_000, "삼성카드": 3_800_000,
    "KB국민카드": 4_000_000, "현대카드": 2_800_000,
    "우리카드": 2_200_000, "하나카드": 2_100_000,
    "롯데카드": 1_800_000, "BC카드": 1_500_000,
}

# 업종별 비중 (합계 = 1.0)
CATEGORY_WEIGHTS = {
    "음식점": 0.20, "주유소": 0.10, "대형마트": 0.14,
    "온라인쇼핑": 0.22, "교통": 0.08, "의료": 0.07,
    "교육": 0.06, "여행/숙박": 0.07, "보험": 0.03, "기타": 0.03,
}

# 카드사별 발급 카드 수 (만 장)
BASE_ISSUED_CARDS: Dict[str, int] = {
    "신한카드": 2400, "삼성카드": 2100, "KB국민카드": 2300,
    "현대카드": 1500, "우리카드": 1200, "하나카드": 1100,
    "롯데카드": 1000, "BC카드": 900,
}


# ──────────────────────────────────────────────────────────
# 데이터 생성 로직
# ──────────────────────────────────────────────────────────

@dataclass
class UsageRecord:
    year_month: str
    card_company: str
    business_category: str
    usage_amount: int
    usage_count: int


@dataclass
class IssuanceRecord:
    year_month: str
    card_company: str
    total_issued_cards: int
    active_cards: int


def _seasonal_factor(month: int) -> float:
    """월별 계절 계수 — 1월 설날, 12월 연말 소비 증가"""
    base = math.sin(2 * math.pi * (month - 3) / 12) * 0.06
    # 1월(설날) +3%, 11-12월(연말) +5%
    holiday = 0.03 if month == 1 else (0.05 if month >= 11 else 0)
    return 1.0 + base + holiday


def generate_usage_data(
    start_year: int = 2024,
    end_year: int = 2025,
    seed: int = 42,
) -> List[UsageRecord]:
    """credit_card_usage 테이블 데이터 생성"""
    random.seed(seed)
    records: List[UsageRecord] = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            ym = f"{year}-{month:02d}-01"
            seasonal = _seasonal_factor(month)
            yoy_growth = 1 + (year - start_year) * 0.035  # 연 3.5% 성장

            for company in CARD_COMPANIES:
                company_base = BASE_AMOUNTS[company]
                company_noise = random.uniform(0.96, 1.04)
                company_total = round(company_base * yoy_growth * seasonal * company_noise)
                company_total_count = round(company_total / random.uniform(8, 15))

                # 업종별 분배
                remaining_amount = company_total
                remaining_count = company_total_count

                for i, category in enumerate(BUSINESS_CATEGORIES):
                    weight = CATEGORY_WEIGHTS[category]
                    cat_noise = random.uniform(0.85, 1.15)

                    if i == len(BUSINESS_CATEGORIES) - 1:
                        cat_amount = remaining_amount
                        cat_count = remaining_count
                    else:
                        cat_amount = round(company_total * weight * cat_noise)
                        cat_count = round(company_total_count * weight * cat_noise)
                        remaining_amount -= cat_amount
                        remaining_count -= cat_count

                    records.append(UsageRecord(
                        year_month=ym,
                        card_company=company,
                        business_category=category,
                        usage_amount=max(0, cat_amount),
                        usage_count=max(0, cat_count),
                    ))

    return records


def generate_issuance_data(
    start_year: int = 2024,
    end_year: int = 2025,
    seed: int = 42,
) -> List[IssuanceRecord]:
    """card_issuance_stats 테이블 데이터 생성"""
    random.seed(seed + 1)
    records: List[IssuanceRecord] = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            ym = f"{year}-{month:02d}-01"

            for company in CARD_COMPANIES:
                base_issued = BASE_ISSUED_CARDS[company] * 10000  # 만 → 장
                growth = 1 + (year - start_year) * 0.02 + month * 0.001
                issued = round(base_issued * growth * random.uniform(0.99, 1.01))
                activation_rate = random.uniform(0.62, 0.78)
                active = round(issued * activation_rate)

                records.append(IssuanceRecord(
                    year_month=ym,
                    card_company=company,
                    total_issued_cards=issued,
                    active_cards=active,
                ))

    return records


# ──────────────────────────────────────────────────────────
# 출력 포맷터
# ──────────────────────────────────────────────────────────

def export_sql(
    usage: List[UsageRecord],
    issuance: List[IssuanceRecord],
    output_path: str,
) -> None:
    """SQL INSERT 문 생성"""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("-- 자동 생성된 샘플 데이터 (generate_sample_data.py)\n")
        f.write(f"-- 생성일: {date.today()}\n")
        f.write(f"-- 이용내역: {len(usage)}건, 발급통계: {len(issuance)}건\n\n")

        f.write("TRUNCATE credit_card_usage;\n")
        f.write("TRUNCATE card_issuance_stats;\n\n")

        f.write("INSERT INTO credit_card_usage "
                "(year_month, card_company, business_category, usage_amount, usage_count) VALUES\n")
        for i, r in enumerate(usage):
            sep = "," if i < len(usage) - 1 else ";"
            f.write(f"  ('{r.year_month}', '{r.card_company}', '{r.business_category}', "
                    f"{r.usage_amount}, {r.usage_count}){sep}\n")

        f.write("\nINSERT INTO card_issuance_stats "
                "(year_month, card_company, total_issued_cards, active_cards) VALUES\n")
        for i, r in enumerate(issuance):
            sep = "," if i < len(issuance) - 1 else ";"
            f.write(f"  ('{r.year_month}', '{r.card_company}', "
                    f"{r.total_issued_cards}, {r.active_cards}){sep}\n")

    print(f"SQL 저장: {output_path}")


def export_csv(
    usage: List[UsageRecord],
    issuance: List[IssuanceRecord],
    output_dir: str,
) -> None:
    """CSV 파일 생성"""
    os.makedirs(output_dir, exist_ok=True)

    usage_path = os.path.join(output_dir, "credit_card_usage.csv")
    with open(usage_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["year_month", "card_company", "business_category", "usage_amount", "usage_count"])
        for r in usage:
            writer.writerow([r.year_month, r.card_company, r.business_category, r.usage_amount, r.usage_count])

    issuance_path = os.path.join(output_dir, "card_issuance_stats.csv")
    with open(issuance_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["year_month", "card_company", "total_issued_cards", "active_cards"])
        for r in issuance:
            writer.writerow([r.year_month, r.card_company, r.total_issued_cards, r.active_cards])

    print(f"CSV 저장: {usage_path}, {issuance_path}")


def main():
    parser = argparse.ArgumentParser(description="샘플 데이터 생성기")
    parser.add_argument("--format", choices=["sql", "csv"], default="sql")
    parser.add_argument("--output", type=str, default="data/seed.sql")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    usage = generate_usage_data(seed=args.seed)
    issuance = generate_issuance_data(seed=args.seed)

    print(f"생성 완료: 이용내역 {len(usage)}건, 발급통계 {len(issuance)}건")

    if args.format == "sql":
        export_sql(usage, issuance, args.output)
    else:
        export_csv(usage, issuance, args.output)


if __name__ == "__main__":
    main()
