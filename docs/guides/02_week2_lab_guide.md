# Week 2: 분류·이상탐지 기반 이상거래 탐지

**기간**: 5일 (월~금, 풀타임 40시간) — 2026-06-01 ~ 2026-06-05
**주제**: Week 1 ML 파이프라인 기준선을 상속하여 Nexus Pay 이상거래 탐지용 supervised fraud classifier, unsupervised anomaly detector, 임계값 튜닝, risk score 적재, API/DAG 검증 흐름을 구축한다.
**산출물**: `config/fraud_model_config.yaml`, `schemas/risk_score.sql`, `scripts/generate_fraud_labels_from_silver.py`, `scripts/import_spark_silver_to_raw_transactions.py`, `features/build_fraud_features.py`, `models/train_fraud_classifier.py`, `models/train_anomaly_detector.py`, `models/tune_fraud_threshold.py`, `models/batch_score_fraud.py`, `serving/fraud_app.py`, `dags/fraud_scoring_pipeline_dag.py`, `docs/models/fraud-detection-design.md`, `docs/reports/fraud-model-evaluation.md`, `tests/test_fraud_pipeline.py`
**전제 조건**: Week 1 산출물 중 `docker-compose.yml`, `schemas/init.sql`, `config/model_config.yaml`, `features/data_split.py`, `features/feature_pipeline.py`, `models/eval_report.py`, `scripts/generate_sample_transactions.py`, `scripts/load_sample_transactions.py` 가 준비되어 있고 `make up && make healthcheck` 가 성공한다.

---

## 수행 시나리오

### 배경 설정

Week 1 에서 Nexus Pay는 ML 프로젝트 표준 구조, 공통 피처 스토어, baseline classifier, 배치 추론, FastAPI 추론 엔드포인트, DAG 초안을 만들었다. Week 2 의 목표는 이 공통 구조를 첫 번째 실제 비즈니스 문제인 **이상거래 탐지**에 적용하는 것이다.

Nexus Pay 운영팀은 모든 거래를 수작업으로 검토할 수 없다. CTO는 모델이 안정적으로 운영될 수 있는 구조를 원하고, COO는 하루 처리 가능한 alert 수를 넘지 않는 임계값 정책을 요구하며, CFO는 미탐 손실과 오탐 검토 비용을 비교 가능한 숫자로 보고 싶어 한다. CISO는 민감정보 없이도 감사 가능한 모델 입력·출력·판정 근거가 남아야 한다고 요구한다.

> "이상거래 탐지는 모델 점수 하나로 끝나지 않습니다. 운영팀이 하루에 볼 수 있는 건수가 있고, CFO가 감당할 수 있는 손실이 있으며, CISO가 설명 가능한 흔적을 요구합니다. 이번 주에는 모델을 잘 만드는 것보다, 어떤 거래를 왜 high risk 로 올렸는지 운영적으로 설명할 수 있어야 합니다." — Nexus Pay CTO

이번 주는 두 종류의 모델을 분리해 설계한다.

* **Supervised fraud classifier**: `is_label_fraud` 라벨이 있는 과거 거래로 학습하고, fraud probability 를 산출한다.
* **Unsupervised anomaly detector**: 라벨이 부족하거나 새로운 패턴이 등장할 때를 대비해 정상 거래 분포에서 벗어난 정도를 산출한다.

최종 운영 산출물은 두 모델의 점수를 조합한 `risk_score` 테이블이다. `prediction_result` 는 Week 1 공통 예측 로그로 유지하고, `risk_score` 는 Week 2 fraud 도메인 전용 운영 테이블로 추가한다.

### C레벨 요구사항

| 요구자 | 요구사항 | 본 가이드 반영 위치 |
|--------|----------|----------------------|
| CTO | Week 1 파이프라인을 재사용해 모델별 확장 패턴을 증명 | Day 1~5 전체 |
| COO | 운영팀 처리 가능 건수를 고려한 high/medium/low risk 정책 | Day 3: threshold tuning |
| CFO | 오탐 검토 비용과 미탐 손실을 비용 기준으로 비교 | Day 3~4: cost report |
| CISO | 민감정보 없이 거래 ID, 점수, 판정 근거, lineage 를 남김 | Day 1: `risk_score` schema |
| CIO | 모델 입력·출력·버전·데이터 계보가 감사 가능해야 함 | Day 4~5: batch score, DAG, tests |

### 목표

1. Week 1 의 공통 피처/분할/평가 구조를 유지하면서 fraud 전용 설정과 스키마를 추가한다.
2. supervised classifier 와 anomaly detector 를 같은 feature contract 위에서 학습한다.
3. 운영팀 alert capacity 와 CFO 비용 가정을 반영해 임계값을 튜닝한다.
4. `risk_score` 테이블에 fraud probability, anomaly score, risk band, reason code, run lineage 를 적재한다.
5. batch scoring, API scoring, DAG validation, 테스트를 통해 재현 가능한 운영 흐름을 만든다.

### 일정 개요

| 일차 | 주제 | 핵심 작업 | 완료 산출물 |
|------|------|----------|-------------|
| Day 1 | 이상거래 문제 정의 + 스키마 확장 | fraud 설정, risk score DDL, 설계 문서 | `config/fraud_model_config.yaml`, `schemas/risk_score.sql`, `docs/models/fraud-detection-design.md` |
| Day 2 | upstream Silver import + classifier 학습 | Spark Silver 거래 데이터와 simulated fraud label 조인, fraud feature 생성, classifier 학습 | `scripts/generate_fraud_labels_from_silver.py`, `scripts/import_spark_silver_to_raw_transactions.py`, `features/build_fraud_features.py`, `models/train_fraud_classifier.py` |
| Day 3 | anomaly detector + 임계값 튜닝 | Isolation Forest 학습, alert capacity 기반 threshold 선택 | `models/train_anomaly_detector.py`, `models/tune_fraud_threshold.py` |
| Day 4 | risk score 적재 + API | `risk_score` batch 적재, FastAPI fraud endpoint | `models/batch_score_fraud.py`, `serving/fraud_app.py` |
| Day 5 | DAG + 테스트 + 운영 리포트 | DAG 검증, 통합 테스트, 평가 리포트 | `dags/fraud_scoring_pipeline_dag.py`, `tests/test_fraud_pipeline.py`, `docs/reports/fraud-model-evaluation.md` |

### Day N 완료 기준 요약

* Day 1 완료 기준: `risk_score` 테이블이 생성되고 fraud 모델 설계 문서가 Week 1 schema/lineage 와 연결되어 있다.
* Day 2 완료 기준: `make fraud-labels`, `make fraud-import-upstream`, `make fraud-train` 실행 시 upstream Silver 기반 `raw_transactions` 와 supervised fraud classifier artifact, MLflow run 이 생성된다.
* Day 3 완료 기준: `make fraud-threshold` 실행 시 운영팀 alert capacity 를 만족하는 threshold policy JSON 이 생성된다.
* Day 4 완료 기준: `make fraud-score` 와 `make fraud-api-test` 가 성공하고 `risk_score` 에 신규 row 가 적재된다.
* Day 5 완료 기준: `make fraud-dag-validate`, `make fraud-test` 가 성공하고 운영 리포트가 작성된다.

---

## Week 1 자산 상속 기준

Week 2 는 Week 1 자산을 다시 만들지 않는다. 다음 자산은 그대로 상속한다.

| Week 1 자산 | Week 2 사용 방식 |
|-------------|------------------|
| `schemas/init.sql` | `raw_transactions`, `feature_store`, `prediction_result`, `model_evaluation`, `data_lineage` 유지 |
| `config/model_config.yaml` | 공통 피처와 분할 기준의 기준선으로 사용 |
| `features/data_split.py` | `SplitConfig`, `time_based_split` 재사용 |
| `features/feature_pipeline.py` | `build_preprocessor`, `feature_store_to_dataframe` 재사용 |
| `models/eval_report.py` | fraud 평가 리포트의 기본 렌더링 패턴 재사용 |
| `scripts/generate_sample_transactions.py` | 라벨 포함 거래 샘플 생성 재사용 |
| `scripts/load_sample_transactions.py` | `raw_transactions` 적재 재사용 |
| `dags/ml_training_pipeline_dag.py` | DAG validation 패턴 재사용 |
| Week 1 upstream 연계 설계 | Spark Silver + simulated fraud label 을 `raw_transactions` 로 import 하는 구현으로 확장 |

Week 2 에서 새로 만드는 것은 fraud 도메인 전용 설정, 모델, threshold policy, risk score 출력이다.

---

## Day 1: 이상거래 문제 정의 + 스키마 확장

### 1-0. Fraud 운영 정의

이번 주 모델의 운영 정의는 다음과 같다.

| 항목 | 정의 |
|------|------|
| 판단 대상 | Nexus Pay 결제 거래 1건 |
| supervised label | `raw_transactions.is_label_fraud` |
| 라벨 확정 시각 | `raw_transactions.label_confirmed_at` |
| 결정 시각 | 거래 발생 시점 또는 feature `as_of_ts` |
| 주요 출력 | fraud probability, anomaly score, risk band |
| 운영 조치 | `HIGH`: 즉시 검토, `MEDIUM`: 샘플링 검토, `LOW`: 자동 통과 |

`risk_band` 는 단순히 모델 점수가 높다는 뜻이 아니라 운영 조치 수준을 의미한다. 이 구분이 있어야 COO가 처리량을 조절하고 CFO가 비용을 해석할 수 있다.

### 1-1. `config/fraud_model_config.yaml`

Week 1 의 `config/model_config.yaml` 을 복사하지 말고 fraud 전용 설정만 명시한다. 공통 설정을 상속한다는 의도를 문서와 코드 양쪽에 남긴다.

```bash
cat > config/fraud_model_config.yaml << 'EOF'
project:
  name: study-ml-pipeline
  scenario: nexuspay-fraud-detection
  inherits: config/model_config.yaml

data:
  raw_table: raw_transactions
  feature_table: feature_store
  prediction_table: prediction_result
  risk_score_table: risk_score
  lineage_table: data_lineage

upstream:
  silver_transactions_path: /upstream/silver/transactions
  fraud_labels_path: data/sample/fraud_labels.csv
  import_mode: truncate_insert

features:
  feature_set: fraud_v1
  feature_version: "v1.0.0"
  base_feature_set: baseline_v1
  base_feature_version: "v1.0.0"
  numerical:
    - amount
    - amount_zscore_30d
    - tx_count_24h
    - days_since_last_tx
  categorical:
    - channel
    - device_country
    - merchant_segment
  risk_reason_rules:
    high_amount_zscore: 3.0
    high_velocity_24h: 8
    overseas_device_country:
      - JP
      - US
      - VN

split:
  strategy: time_based
  train_until: "2026-04-30T23:59:59+09:00"
  valid_until: "2026-05-15T23:59:59+09:00"
  test_until:  "2026-05-24T23:59:59+09:00"
  label_column: is_label_fraud
  label_confirmed_column: label_confirmed_at
  min_label_lag_hours: 24

classifier:
  candidates:
    - logistic_regression
    - random_forest
  selected: random_forest
  random_state: 42
  class_weight: balanced
  registry:
    name: nexuspay_fraud_classifier

anomaly:
  algorithm: isolation_forest
  contamination: 0.02
  random_state: 42
  registry:
    name: nexuspay_fraud_anomaly_detector

threshold:
  alert_capacity_per_day: 300
  high_risk_min_precision: 0.08
  medium_risk_multiplier: 0.60
  default_high_threshold: 0.70
  default_medium_threshold: 0.35

evaluation:
  metrics: [precision, recall, f1, pr_auc, roc_auc]
  cost:
    false_positive_cost_krw: 5000
    false_negative_cost_krw: 250000
    manual_review_capacity_cost_krw: 1500000

artifacts:
  model_root: data/artifacts
  threshold_policy: data/artifacts/nexuspay_fraud_classifier/threshold_policy.json
  report_dir: data/reports
EOF
```

### 1-2. `schemas/risk_score.sql`

`prediction_result` 는 모든 모델 공통 로그이고, `risk_score` 는 fraud 운영 테이블이다. 운영팀 화면이나 리포트는 `risk_score` 를 조회한다고 가정한다.

```bash
cat > schemas/risk_score.sql << 'EOF'
-- Week 2 fraud 전용 운영 출력 테이블

CREATE TABLE IF NOT EXISTS risk_score (
    risk_score_id       BIGSERIAL PRIMARY KEY,
    transaction_id      VARCHAR(40) NOT NULL,
    customer_id         VARCHAR(40),
    model_name          VARCHAR(80) NOT NULL,
    model_version       VARCHAR(64) NOT NULL,
    feature_set         VARCHAR(40) NOT NULL,
    feature_version     VARCHAR(20) NOT NULL,
    fraud_probability   NUMERIC(7,4) NOT NULL,
    anomaly_score       NUMERIC(10,5),
    combined_score      NUMERIC(7,4) NOT NULL,
    risk_band           VARCHAR(20) NOT NULL,     -- HIGH/MEDIUM/LOW
    reason_code         TEXT,
    action              VARCHAR(40) NOT NULL,     -- REVIEW/MONITOR/PASS
    scored_at           TIMESTAMPTZ DEFAULT NOW(),
    run_id              VARCHAR(40) NOT NULL,
    UNIQUE (transaction_id, model_name, model_version)
);

CREATE INDEX IF NOT EXISTS ix_risk_score_tx
    ON risk_score (transaction_id);

CREATE INDEX IF NOT EXISTS ix_risk_score_band_scored
    ON risk_score (risk_band, scored_at);
EOF

docker compose exec -T postgres psql -U "${POSTGRES_USER:-mluser}" -d "${POSTGRES_DB:-ml_db}" < schemas/risk_score.sql
```

검증:

```bash
docker compose exec postgres psql -U mluser -d ml_db -c "\d risk_score"
```

### 1-3. `docs/models/fraud-detection-design.md`

```bash
cat > docs/models/fraud-detection-design.md << 'EOF'
# Nexus Pay Fraud Detection Design — Week 2

## 목적

Week 1 baseline ML pipeline 을 상속해 이상거래 탐지 모델을 운영 가능한 형태로 확장한다.

## 모델 구성

| 모델 | 역할 | 출력 |
|------|------|------|
| Supervised fraud classifier | 과거 fraud label 기반 확률 산출 | `fraud_probability` |
| Isolation Forest anomaly detector | 라벨이 부족한 신규 패턴 탐지 | `anomaly_score` |

## 운영 판정

최종 `risk_band` 는 classifier probability 를 우선하고 anomaly score 를 보조 신호로 사용한다.

- `HIGH`: 운영팀 즉시 검토
- `MEDIUM`: 모니터링 또는 샘플링 검토
- `LOW`: 자동 통과

## Week 1 상속 자산

- `feature_store` 입력 계약
- `time_based_split`
- sklearn preprocessing pipeline
- MLflow tracking
- `prediction_result` 공통 예측 로그
- `data_lineage` 실행 추적

## 누수 방지

- `label_confirmed_at` 이 train window 이후인 row 는 학습에서 제외한다.
- 모든 전처리 통계는 train split 에서만 fit 한다.
- 운영 점수 산출 시 `as_of_ts` 이후 정보는 feature payload 에 포함하지 않는다.
EOF
```

### 1-4. Makefile 타겟 추가

Week 1 `Makefile` 끝에 fraud 전용 타겟을 추가한다.

```makefile
.PHONY: fraud-schema fraud-features fraud-train fraud-anomaly fraud-threshold \
        fraud-labels fraud-import-upstream fraud-score fraud-api fraud-api-test \
        fraud-dag-validate fraud-test

fraud-schema:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-mluser} -d $${POSTGRES_DB:-ml_db} < schemas/risk_score.sql

fraud-labels:
	docker compose exec runner python scripts/generate_fraud_labels_from_silver.py --config config/fraud_model_config.yaml

fraud-import-upstream:
	docker compose exec runner python scripts/import_spark_silver_to_raw_transactions.py --config config/fraud_model_config.yaml --truncate

fraud-features:
	docker compose exec runner python -m features.build_fraud_features --config config/fraud_model_config.yaml

fraud-train:
	docker compose exec runner python -m models.train_fraud_classifier --config config/fraud_model_config.yaml

fraud-anomaly:
	docker compose exec runner python -m models.train_anomaly_detector --config config/fraud_model_config.yaml

fraud-threshold:
	docker compose exec runner python -m models.tune_fraud_threshold --config config/fraud_model_config.yaml

fraud-score:
	docker compose exec runner python -m models.batch_score_fraud --config config/fraud_model_config.yaml

fraud-api:
	docker compose exec api uvicorn serving.fraud_app:app --host 0.0.0.0 --port 8000 --reload

fraud-api-test:
	curl -fsS -X POST http://localhost:$${API_PORT:-8000}/fraud/score \
	  -H 'Content-Type: application/json' \
	  -d '{"transaction_id":"tx-demo-fraud","amount":980000,"amount_zscore_30d":4.2,"tx_count_24h":12,"days_since_last_tx":0.02,"channel":"APP","device_country":"US","merchant_segment":"OVERSEAS"}'

fraud-dag-validate:
	docker compose exec runner python -m dags.fraud_scoring_pipeline_dag --validate

fraud-test:
	docker compose exec runner pytest -q tests/test_fraud_pipeline.py
```

### Day 1 완료 기준

* `config/fraud_model_config.yaml` 이 생성되어 Week 1 공통 설정과 구분된다.
* `schemas/risk_score.sql` 적용 후 `\d risk_score` 가 성공한다.
* `docs/models/fraud-detection-design.md` 에 supervised/anomaly/threshold 운영 정의가 정리되어 있다.

---

## Day 2: Upstream Silver Import + Supervised Classifier 학습

### 2-0. Week 1 feature store 를 복제하지 않고 확장한다

Week 2 의 `fraud_v1` feature set 은 Week 1 의 `baseline_v1` 과 같은 기본 컬럼을 사용한다. 차이는 피처 이름이 아니라 **운영 목적과 모델 lineage** 다. 따라서 Day 2 에서는 `feature_store` 의 payload contract 를 유지하면서 `feature_set='fraud_v1'` 로 적재한다.

실습 입력은 두 경로를 모두 허용한다.

| 입력 경로 | 사용 시점 | 설명 |
|-----------|-----------|------|
| Week 1 synthetic sample | 빠른 smoke test | `generate_sample_transactions.py` 와 `load_sample_transactions.py` 로 독립 실행 |
| Spark Silver + simulated fraud labels | consulting demo 연결 | `study-data-pipeline` 의 Delta Silver 거래 데이터를 읽어 사후 확정 fraud label 과 조인 |

Spark Silver 연계 경로를 컨테이너에서 실행하려면 `runner` 서비스에 upstream Delta 경로를 읽기 전용으로 마운트한다.

```yaml
# docker-compose.yml runner volumes 추가 예시
    volumes:
      - ./:/app
      - C:/study/study-data-pipeline/data/lakehouse/delta/silver/transactions:/upstream/silver/transactions:ro
```

Delta Lake 파일을 Python 에서 직접 읽기 위해 Week 2 에서는 다음 의존성을 추가한다.

```bash
cat >> requirements.txt << 'EOF'

# Upstream Delta import
deltalake==0.20.2
pyarrow==16.1.0
EOF
```

### 2-1. `scripts/generate_fraud_labels_from_silver.py`

이 스크립트는 Spark Silver 거래 데이터를 읽고, chargeback/dispute/manual review 성격의 사후 확정 라벨을 시뮬레이션한다. 라벨은 거래 발생 시점에 존재하지 않으며, `label_confirmed_at` 은 항상 `event_timestamp` 이후로 만든다.

```python
# scripts/generate_fraud_labels_from_silver.py
"""Spark Silver 거래 데이터에서 fraud label CSV 를 시뮬레이션 생성."""
from __future__ import annotations

import argparse
import random
from pathlib import Path

import pandas as pd
import yaml
from deltalake import DeltaTable


LABEL_TYPES = ["CHARGEBACK", "DISPUTE", "MANUAL_REVIEW", "CUSTOMER_REPORT"]
REASONS = ["stolen_card", "account_takeover", "merchant_abuse", "unusual_velocity", "customer_claim"]


def _load_silver(path: str) -> pd.DataFrame:
    return DeltaTable(path).to_pandas()


def _fraud_probability(row: pd.Series, amount_p95: float) -> float:
    p = 0.01
    if bool(row.get("is_anomaly", False)):
        p += 0.12
    if float(row.get("amount") or 0) >= amount_p95:
        p += 0.08
    if row.get("channel") in {"ATM", "WEB"}:
        p += 0.02
    if row.get("status") in {"FAILED", "PENDING"}:
        p += 0.03
    return min(p, 0.85)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    silver_path = cfg["upstream"]["silver_transactions_path"]
    out_path = args.out or Path(cfg["upstream"]["fraud_labels_path"])
    rng = random.Random(args.seed)

    df = _load_silver(silver_path)
    if df.empty:
        raise ValueError(f"no silver rows found at {silver_path}")

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    amount_p95 = float(df["amount"].dropna().quantile(0.95))

    labels = []
    for _, row in df.iterrows():
        confirmed = rng.random() < _fraud_probability(row, amount_p95)
        delay_days = rng.randint(1, 14)
        labels.append(
            {
                "transaction_id": row["event_id"],
                "case_id": f"FR-{row['event_id']}",
                "label_type": rng.choice(LABEL_TYPES),
                "is_confirmed_fraud": bool(confirmed),
                "label_confirmed_at": row["event_timestamp"] + pd.Timedelta(days=delay_days, hours=rng.randint(0, 23)),
                "review_reason": rng.choice(REASONS) if confirmed else "review_cleared",
                "review_status": "CONFIRMED" if confirmed else "REJECTED",
                "analyst_team": "risk-ops",
            }
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(labels).to_csv(out_path, index=False)
    print(f"wrote fraud labels: {len(labels):,} -> {out_path}")


if __name__ == "__main__":
    main()
```

### 2-2. `scripts/import_spark_silver_to_raw_transactions.py`

이 스크립트는 Silver 거래 데이터와 fraud label CSV 를 조인한 뒤 ML Week 1 의 `raw_transactions` 스키마로 적재한다.

```python
# scripts/import_spark_silver_to_raw_transactions.py
"""Spark Silver 거래 데이터 + fraud label CSV -> raw_transactions 적재."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import yaml
from deltalake import DeltaTable
from sqlalchemy import create_engine


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_silver(path: str) -> pd.DataFrame:
    return DeltaTable(path).to_pandas()


def _normalize_channel(value) -> str:
    if pd.isna(value):
        return "APP"
    value = str(value).upper()
    return "APP" if value == "MOBILE" else value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--truncate", action="store_true")
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    silver = _load_silver(cfg["upstream"]["silver_transactions_path"])
    labels = pd.read_csv(cfg["upstream"]["fraud_labels_path"], parse_dates=["label_confirmed_at"])

    silver["event_timestamp"] = pd.to_datetime(silver["event_timestamp"], utc=True)
    labels["label_confirmed_at"] = pd.to_datetime(labels["label_confirmed_at"], utc=True)

    merged = silver.merge(labels, left_on="event_id", right_on="transaction_id", how="left")
    merged["label_confirmed_at"] = merged["label_confirmed_at"].fillna(
        merged["event_timestamp"] + pd.Timedelta(days=7)
    )
    merged["is_confirmed_fraud"] = merged["is_confirmed_fraud"].fillna(False)

    raw = pd.DataFrame(
        {
            "transaction_id": merged["event_id"].astype(str),
            "customer_id": "c-" + merged["user_id"].astype("Int64").astype(str),
            "merchant_id": merged["merchant_id"].fillna("UNKNOWN").astype(str),
            "amount": merged["amount"].fillna(0).astype(float),
            "currency": merged["currency"].fillna("KRW").astype(str).str[:3],
            "channel": merged["channel"].apply(_normalize_channel),
            "device_country": "KR",
            "merchant_segment": merged["merchant_category"].fillna("GENERAL").astype(str).str[:30],
            "is_label_fraud": merged["is_confirmed_fraud"].astype(bool),
            "transacted_at": merged["event_timestamp"],
            "label_confirmed_at": merged["label_confirmed_at"],
        }
    )

    engine = _engine()
    with engine.begin() as conn:
        if args.truncate:
            conn.exec_driver_sql("TRUNCATE TABLE raw_transactions")
        raw.to_sql("raw_transactions", conn, if_exists="append", index=False)

    print(f"loaded upstream silver rows into raw_transactions: {len(raw):,}")


if __name__ == "__main__":
    main()
```

### 2-3. `features/build_fraud_features.py`

```python
# features/build_fraud_features.py
"""raw_transactions -> fraud_v1 feature_store 적재.

Week 1 baseline feature logic 을 fraud 모델용 feature_set 으로 재사용한다.
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, text


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_raw(engine, until_ts: pd.Timestamp) -> pd.DataFrame:
    sql = text(
        """
        SELECT transaction_id, customer_id, merchant_id, amount, channel,
               device_country, merchant_segment, transacted_at, label_confirmed_at,
               is_label_fraud
          FROM raw_transactions
         WHERE transacted_at <= :until_ts
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"until_ts": until_ts.to_pydatetime()})


def _temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["customer_id", "transacted_at"]).copy()
    df["tx_count_24h"] = (
        df.groupby("customer_id")
          .apply(lambda g: g["transacted_at"].apply(
              lambda t: ((g["transacted_at"] >= t - pd.Timedelta("24h"))
                         & (g["transacted_at"] < t)).sum()))
          .reset_index(level=0, drop=True)
    )
    df["days_since_last_tx"] = (
        df.groupby("customer_id")["transacted_at"].diff().dt.total_seconds() / 86400.0
    )

    def zscore_30d(g: pd.DataFrame) -> pd.Series:
        out = []
        for ts, amt in zip(g["transacted_at"], g["amount"]):
            window = g[(g["transacted_at"] >= ts - pd.Timedelta("30D"))
                       & (g["transacted_at"] < ts)]
            if len(window) < 5 or window["amount"].std(ddof=0) == 0:
                out.append(0.0)
            else:
                out.append(float((amt - window["amount"].mean()) / window["amount"].std(ddof=0)))
        return pd.Series(out, index=g.index)

    df["amount_zscore_30d"] = df.groupby("customer_id", group_keys=False).apply(zscore_30d)
    return df


def _reason_seed(row: pd.Series, rules: dict) -> list[str]:
    reasons = []
    if row.get("amount_zscore_30d", 0) >= rules["high_amount_zscore"]:
        reasons.append("HIGH_AMOUNT_ZSCORE")
    if row.get("tx_count_24h", 0) >= rules["high_velocity_24h"]:
        reasons.append("HIGH_VELOCITY_24H")
    if row.get("device_country") in set(rules["overseas_device_country"]):
        reasons.append("OVERSEAS_DEVICE")
    return reasons


def _to_feature_rows(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    payload_cols = cfg["features"]["numerical"] + cfg["features"]["categorical"]
    rules = cfg["features"]["risk_reason_rules"]

    df = df.copy()
    df["risk_reason_seed"] = df.apply(lambda r: _reason_seed(r, rules), axis=1)
    df["payload_json"] = df[payload_cols + ["risk_reason_seed"]].apply(
        lambda r: json.dumps(r.dropna().to_dict()),
        axis=1,
    )
    return df.assign(
        feature_set=feature_set,
        feature_version=feature_version,
        as_of_ts=df["transacted_at"],
    )[["feature_set", "feature_version", "transaction_id", "as_of_ts", "payload_json"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    until_ts = pd.Timestamp(cfg["split"]["test_until"])
    engine = _engine()
    raw = _load_raw(engine, until_ts)
    raw["transacted_at"] = pd.to_datetime(raw["transacted_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw["label_confirmed_at"] = pd.to_datetime(raw["label_confirmed_at"], utc=True).dt.tz_convert("Asia/Seoul")

    rows = _to_feature_rows(_temporal_features(raw), cfg)
    run_id = str(uuid.uuid4())

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM feature_store WHERE feature_set = :fs AND feature_version = :fv"),
            {"fs": cfg["features"]["feature_set"], "fv": cfg["features"]["feature_version"]},
        )
        rows.to_sql("feature_store", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, input_window_start, "
                "input_window_end, output_target) "
                "VALUES (:run_id, 'feature', 'raw_transactions', :ws, :we, 'feature_store@fraud_v1')"
            ),
            {
                "run_id": run_id,
                "ws": raw["transacted_at"].min().to_pydatetime(),
                "we": until_ts.to_pydatetime(),
            },
        )

    print(f"fraud features updated: {len(rows):,} rows (run_id={run_id})")


if __name__ == "__main__":
    main()
```

### 2-4. `models/train_fraud_classifier.py`

```python
# models/train_fraud_classifier.py
"""Supervised fraud classifier 학습."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import joblib
import mlflow
import pandas as pd
import yaml
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, confusion_matrix, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text

from features.data_split import SplitConfig, time_based_split
from features.feature_pipeline import build_preprocessor, feature_store_to_dataframe


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_frame(engine, feature_set: str, feature_version: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT fs.transaction_id, fs.payload_json, fs.as_of_ts,
               rt.is_label_fraud, rt.label_confirmed_at, rt.transacted_at
          FROM feature_store fs
          JOIN raw_transactions rt USING (transaction_id)
         WHERE fs.feature_set = :fs AND fs.feature_version = :fv
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version})


def _estimator(name: str, cfg: dict):
    rs = cfg["classifier"]["random_state"]
    if name == "logistic_regression":
        return LogisticRegression(max_iter=300, class_weight="balanced", random_state=rs)
    if name == "random_forest":
        return RandomForestClassifier(n_estimators=300, class_weight="balanced", random_state=rs, n_jobs=-1)
    raise ValueError(f"unknown classifier: {name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    columns = cfg["features"]["numerical"] + cfg["features"]["categorical"]
    split_cfg = SplitConfig.from_dict(cfg["split"])
    label_col = cfg["split"]["label_column"]
    model_name = cfg["classifier"]["registry"]["name"]
    selected = cfg["classifier"]["selected"]
    threshold = cfg["threshold"]["default_high_threshold"]

    engine = _engine()
    raw = _load_frame(engine, feature_set, feature_version)
    raw["transacted_at"] = pd.to_datetime(raw["transacted_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw["label_confirmed_at"] = pd.to_datetime(raw["label_confirmed_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw[label_col] = raw[label_col].astype(int)

    train_df, valid_df, test_df = time_based_split(raw, split_cfg)
    X_train = feature_store_to_dataframe(train_df, columns)
    X_valid = feature_store_to_dataframe(valid_df, columns)
    X_test = feature_store_to_dataframe(test_df, columns)
    y_train = train_df[label_col].values
    y_valid = valid_df[label_col].values
    y_test = test_df[label_col].values

    pipe = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor(cfg["features"]["numerical"], cfg["features"]["categorical"])),
            ("model", _estimator(selected, cfg)),
        ]
    )

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(model_name)

    with mlflow.start_run(run_name=f"{selected}-{time.strftime('%Y%m%d-%H%M%S')}") as run:
        pipe.fit(X_train, y_train)
        valid_proba = pipe.predict_proba(X_valid)[:, 1]
        test_proba = pipe.predict_proba(X_test)[:, 1]
        test_pred = (test_proba >= threshold).astype(int)

        metrics = {
            "valid_pr_auc": average_precision_score(valid_df[label_col].values, valid_proba),
            "precision": precision_score(y_test, test_pred, zero_division=0),
            "recall": recall_score(y_test, test_pred, zero_division=0),
            "f1": f1_score(y_test, test_pred, zero_division=0),
            "pr_auc": average_precision_score(y_test, test_proba),
            "roc_auc": roc_auc_score(y_test, test_proba) if len(set(y_test)) > 1 else 0.0,
        }
        mlflow.log_params({"classifier": selected, "feature_set": feature_set, "feature_version": feature_version})
        mlflow.log_metrics(metrics)

        cm = confusion_matrix(y_test, test_pred, labels=[0, 1]).tolist()
        artifact_dir = Path(cfg["artifacts"]["model_root"]) / model_name / time.strftime("%Y%m%d-%H%M%S")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        model_version = artifact_dir.name
        model_path = artifact_dir / "model.joblib"
        joblib.dump(
            {
                "pipeline": pipe,
                "columns": columns,
                "threshold": threshold,
                "model_name": model_name,
                "model_version": model_version,
                "mlflow_run_id": run.info.run_id,
                "metrics": metrics,
                "confusion_matrix": cm,
            },
            model_path,
        )
        mlflow.log_artifact(str(model_path), artifact_path="model")

        with engine.begin() as conn:
            for name, value in metrics.items():
                conn.execute(
                    text(
                        "INSERT INTO model_evaluation(model_name, model_version, eval_window_start, "
                        "eval_window_end, metric_name, metric_value, extra_json) "
                        "VALUES (:mn, :mv, :ws, :we, :metric, :value, :extra)"
                    ),
                    {
                        "mn": model_name,
                        "mv": model_version,
                        "ws": split_cfg.valid_until.to_pydatetime(),
                        "we": split_cfg.test_until.to_pydatetime(),
                        "metric": name,
                        "value": float(value),
                        "extra": json.dumps({"threshold": threshold, "confusion_matrix": cm}),
                    },
                )

    print(f"trained fraud classifier model_version={model_version} path={model_path}")


if __name__ == "__main__":
    main()
```

### Day 2 완료 기준

```bash
make fraud-labels
make fraud-import-upstream
make fraud-features
make fraud-train
```

완료 조건:

* Spark Silver 거래 데이터와 simulated fraud label 이 조인되어 `raw_transactions` 에 적재된다.
* `feature_store` 에 `feature_set='fraud_v1'` row 가 생성된다.
* `data/artifacts/nexuspay_fraud_classifier/*/model.joblib` 이 생성된다.
* MLflow UI 에 `nexuspay_fraud_classifier` experiment run 이 보인다.
* `model_evaluation` 에 fraud classifier metric 이 적재된다.

---

## Day 3: Anomaly Detector + 임계값 튜닝

### 3-0. 왜 anomaly detector 를 분리하는가

Fraud label 은 늦게 확정되고, 새로운 공격 패턴은 과거 라벨에 없을 수 있다. 그래서 supervised classifier 는 확정 라벨 기반 운영 신호로 쓰고, anomaly detector 는 새로운 패턴을 보조 탐지하는 신호로 사용한다.

### 3-1. `models/train_anomaly_detector.py`

```python
# models/train_anomaly_detector.py
"""Isolation Forest 기반 anomaly detector 학습."""
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import joblib
import mlflow
import pandas as pd
import yaml
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text

from features.data_split import SplitConfig, time_based_split
from features.feature_pipeline import build_preprocessor, feature_store_to_dataframe


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_frame(engine, feature_set: str, feature_version: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT fs.transaction_id, fs.payload_json, fs.as_of_ts,
               rt.is_label_fraud, rt.label_confirmed_at, rt.transacted_at
          FROM feature_store fs
          JOIN raw_transactions rt USING (transaction_id)
         WHERE fs.feature_set = :fs AND fs.feature_version = :fv
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    columns = cfg["features"]["numerical"] + cfg["features"]["categorical"]
    split_cfg = SplitConfig.from_dict(cfg["split"])
    model_name = cfg["anomaly"]["registry"]["name"]

    engine = _engine()
    raw = _load_frame(engine, cfg["features"]["feature_set"], cfg["features"]["feature_version"])
    raw["transacted_at"] = pd.to_datetime(raw["transacted_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw["label_confirmed_at"] = pd.to_datetime(raw["label_confirmed_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw[cfg["split"]["label_column"]] = raw[cfg["split"]["label_column"]].astype(int)

    train_df, _, _ = time_based_split(raw, split_cfg)
    normal_train = train_df[train_df[cfg["split"]["label_column"]] == 0].copy()
    X_train = feature_store_to_dataframe(normal_train, columns)

    pipe = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor(cfg["features"]["numerical"], cfg["features"]["categorical"])),
            (
                "model",
                IsolationForest(
                    n_estimators=200,
                    contamination=cfg["anomaly"]["contamination"],
                    random_state=cfg["anomaly"]["random_state"],
                    n_jobs=-1,
                ),
            ),
        ]
    )

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(model_name)
    with mlflow.start_run(run_name=f"isolation-forest-{time.strftime('%Y%m%d-%H%M%S')}") as run:
        pipe.fit(X_train)
        artifact_dir = Path(cfg["artifacts"]["model_root"]) / model_name / time.strftime("%Y%m%d-%H%M%S")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        model_path = artifact_dir / "model.joblib"
        joblib.dump(
            {
                "pipeline": pipe,
                "columns": columns,
                "model_name": model_name,
                "model_version": artifact_dir.name,
                "mlflow_run_id": run.info.run_id,
            },
            model_path,
        )
        mlflow.log_params({"algorithm": "isolation_forest", "normal_train_rows": len(normal_train)})
        mlflow.log_artifact(str(model_path), artifact_path="model")

    print(f"trained anomaly detector path={model_path}")


if __name__ == "__main__":
    main()
```

### 3-2. `models/tune_fraud_threshold.py`

이 스크립트는 validation window 에서 운영팀 처리 가능 건수를 넘지 않는 high threshold 를 선택한다.

```python
# models/tune_fraud_threshold.py
"""Fraud classifier threshold policy 생성."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import yaml
from sklearn.metrics import precision_score, recall_score

from features.data_split import SplitConfig, time_based_split
from features.feature_pipeline import feature_store_to_dataframe
from models.train_fraud_classifier import _engine, _load_frame


def _latest_model(model_root: str, model_name: str) -> Path:
    candidates = sorted((Path(model_root) / model_name).glob("*/model.joblib"))
    if not candidates:
        raise FileNotFoundError(f"no classifier artifact for {model_name}")
    return candidates[-1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    model_name = cfg["classifier"]["registry"]["name"]
    artifact_path = _latest_model(cfg["artifacts"]["model_root"], model_name)
    bundle = joblib.load(artifact_path)

    engine = _engine()
    raw = _load_frame(engine, cfg["features"]["feature_set"], cfg["features"]["feature_version"])
    raw["transacted_at"] = pd.to_datetime(raw["transacted_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw["label_confirmed_at"] = pd.to_datetime(raw["label_confirmed_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw[cfg["split"]["label_column"]] = raw[cfg["split"]["label_column"]].astype(int)

    split_cfg = SplitConfig.from_dict(cfg["split"])
    _, valid_df, _ = time_based_split(raw, split_cfg)
    X_valid = feature_store_to_dataframe(valid_df, bundle["columns"])
    y_valid = valid_df[cfg["split"]["label_column"]].values
    proba = bundle["pipeline"].predict_proba(X_valid)[:, 1]

    rows = []
    daily_capacity = cfg["threshold"]["alert_capacity_per_day"]
    validation_days = max(1, (split_cfg.valid_until - split_cfg.train_until).days)
    max_alerts = daily_capacity * validation_days

    for threshold in np.arange(0.10, 0.96, 0.01):
        pred = (proba >= threshold).astype(int)
        alerts = int(pred.sum())
        rows.append(
            {
                "threshold": round(float(threshold), 2),
                "alerts": alerts,
                "precision": precision_score(y_valid, pred, zero_division=0),
                "recall": recall_score(y_valid, pred, zero_division=0),
            }
        )

    candidates = [
        row for row in rows
        if row["alerts"] <= max_alerts
        and row["precision"] >= cfg["threshold"]["high_risk_min_precision"]
    ]
    selected = max(candidates or rows, key=lambda r: (r["recall"], r["precision"]))
    high = selected["threshold"]
    medium = round(max(0.05, high * cfg["threshold"]["medium_risk_multiplier"]), 2)

    policy = {
        "model_name": model_name,
        "model_version": bundle["model_version"],
        "high_threshold": high,
        "medium_threshold": medium,
        "alert_capacity_per_day": daily_capacity,
        "validation_summary": selected,
        "candidates": rows,
    }

    out = Path(cfg["artifacts"]["threshold_policy"])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(policy, indent=2), encoding="utf-8")
    print(f"wrote threshold policy -> {out}")


if __name__ == "__main__":
    main()
```

### Day 3 완료 기준

```bash
make fraud-anomaly
make fraud-threshold
```

완료 조건:

* `data/artifacts/nexuspay_fraud_anomaly_detector/*/model.joblib` 이 생성된다.
* `data/artifacts/nexuspay_fraud_classifier/threshold_policy.json` 이 생성된다.
* threshold policy 에 `high_threshold`, `medium_threshold`, `alert_capacity_per_day` 가 포함된다.

---

## Day 4: Risk Score 적재 + Fraud API

### 4-0. Risk score 산식

Week 2 의 combined score 는 단순한 운영 기준선으로 둔다.

```text
combined_score = min(1.0, 0.85 * fraud_probability + 0.15 * anomaly_score_normalized)
```

`fraud_probability` 는 supervised classifier 의 확률이고, `anomaly_score_normalized` 는 Isolation Forest 의 이상 정도를 0~1로 변환한 값이다.

### 4-1. `models/batch_score_fraud.py`

```python
# models/batch_score_fraud.py
"""Fraud classifier + anomaly detector 로 risk_score 적재."""
from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

from features.feature_pipeline import feature_store_to_dataframe


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _latest(model_root: str, model_name: str) -> Path:
    candidates = sorted((Path(model_root) / model_name).glob("*/model.joblib"))
    if not candidates:
        raise FileNotFoundError(f"no artifact for {model_name}")
    return candidates[-1]


def _band(score: float, high: float, medium: float) -> tuple[str, str]:
    if score >= high:
        return "HIGH", "REVIEW"
    if score >= medium:
        return "MEDIUM", "MONITOR"
    return "LOW", "PASS"


def _reasons(payload: dict, probability: float, anomaly: float, high: float) -> str:
    reasons = list(payload.get("risk_reason_seed", []))
    if probability >= high:
        reasons.append("MODEL_HIGH_PROBABILITY")
    if anomaly >= 0.80:
        reasons.append("ANOMALY_PATTERN")
    return ",".join(sorted(set(reasons))) or "BASELINE_SCORE"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    classifier_path = _latest(cfg["artifacts"]["model_root"], cfg["classifier"]["registry"]["name"])
    anomaly_path = _latest(cfg["artifacts"]["model_root"], cfg["anomaly"]["registry"]["name"])
    threshold_policy = json.loads(Path(cfg["artifacts"]["threshold_policy"]).read_text(encoding="utf-8"))

    classifier = joblib.load(classifier_path)
    anomaly = joblib.load(anomaly_path)
    columns = classifier["columns"]
    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    model_name = cfg["classifier"]["registry"]["name"]
    model_version = classifier["model_version"]
    high = threshold_policy["high_threshold"]
    medium = threshold_policy["medium_threshold"]

    engine = _engine()
    sql = text(
        """
        SELECT fs.transaction_id, rt.customer_id, fs.payload_json, fs.as_of_ts
          FROM feature_store fs
          JOIN raw_transactions rt USING (transaction_id)
         WHERE fs.feature_set = :fs AND fs.feature_version = :fv
           AND fs.transaction_id NOT IN (
               SELECT transaction_id FROM risk_score
                WHERE model_name = :mn AND model_version = :mv
           )
        """
    )
    with engine.connect() as conn:
        rows = pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version, "mn": model_name, "mv": model_version})

    if rows.empty:
        print("no new fraud rows to score")
        return

    X = feature_store_to_dataframe(rows, columns)
    fraud_probability = classifier["pipeline"].predict_proba(X)[:, 1]
    raw_anomaly = -anomaly["pipeline"].decision_function(X)
    anomaly_score = (raw_anomaly - raw_anomaly.min()) / (raw_anomaly.max() - raw_anomaly.min() + 1e-9)
    combined = np.minimum(1.0, 0.85 * fraud_probability + 0.15 * anomaly_score)

    run_id = str(uuid.uuid4())
    payloads = rows["payload_json"].apply(lambda x: x if isinstance(x, dict) else json.loads(x))
    out_rows = []
    prediction_rows = []
    for idx, row in rows.iterrows():
        score = float(combined[len(out_rows)])
        band, action = _band(score, high, medium)
        payload = payloads.loc[idx]
        out_rows.append(
            {
                "transaction_id": row["transaction_id"],
                "customer_id": row["customer_id"],
                "model_name": model_name,
                "model_version": model_version,
                "feature_set": feature_set,
                "feature_version": feature_version,
                "fraud_probability": float(fraud_probability[len(out_rows)]),
                "anomaly_score": float(anomaly_score[len(out_rows)]),
                "combined_score": score,
                "risk_band": band,
                "reason_code": _reasons(payload, float(fraud_probability[len(out_rows)]), float(anomaly_score[len(out_rows)]), high),
                "action": action,
                "run_id": run_id,
            }
        )
        prediction_rows.append(
            {
                "transaction_id": row["transaction_id"],
                "model_name": model_name,
                "model_version": model_version,
                "feature_set": feature_set,
                "feature_version": feature_version,
                "score": score,
                "prediction": band,
                "run_id": run_id,
            }
        )

    with engine.begin() as conn:
        pd.DataFrame(out_rows).to_sql("risk_score", conn, if_exists="append", index=False)
        pd.DataFrame(prediction_rows).to_sql("prediction_result", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, output_target) "
                "VALUES (:run_id, 'predict', :src, 'risk_score,prediction_result')"
            ),
            {"run_id": run_id, "src": f"feature_store@{feature_set}/{feature_version}"},
        )

    print(f"scored fraud risk rows={len(out_rows):,} run_id={run_id}")


if __name__ == "__main__":
    main()
```

### 4-2. `serving/fraud_app.py`

```python
# serving/fraud_app.py
"""Fraud scoring FastAPI endpoint."""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


app = FastAPI(title="Nexus Pay Fraud Scoring API", version="0.2.0")


class FraudScoreRequest(BaseModel):
    transaction_id: str = Field(..., max_length=40)
    amount: float = Field(..., ge=0)
    amount_zscore_30d: float = 0.0
    tx_count_24h: int = 0
    days_since_last_tx: float = 0.0
    channel: str
    device_country: str
    merchant_segment: str


class FraudScoreResponse(BaseModel):
    transaction_id: str
    model_name: str
    model_version: str
    fraud_probability: float
    anomaly_score: float
    combined_score: float
    risk_band: str
    action: str
    reason_code: str


def _latest(model_name: str) -> Path:
    candidates = sorted((Path("data/artifacts") / model_name).glob("*/model.joblib"))
    if not candidates:
        raise RuntimeError(f"no artifact found for {model_name}")
    return candidates[-1]


def _band(score: float, high: float, medium: float) -> tuple[str, str]:
    if score >= high:
        return "HIGH", "REVIEW"
    if score >= medium:
        return "MEDIUM", "MONITOR"
    return "LOW", "PASS"


CLASSIFIER_NAME = "nexuspay_fraud_classifier"
ANOMALY_NAME = "nexuspay_fraud_anomaly_detector"
_CACHE = {}


def _bundle():
    if not _CACHE:
        classifier = joblib.load(_latest(CLASSIFIER_NAME))
        anomaly = joblib.load(_latest(ANOMALY_NAME))
        policy = json.loads(Path("data/artifacts/nexuspay_fraud_classifier/threshold_policy.json").read_text(encoding="utf-8"))
        _CACHE.update({"classifier": classifier, "anomaly": anomaly, "policy": policy})
    return _CACHE


@app.get("/fraud/health")
def health():
    try:
        b = _bundle()
        return {"status": "ok", "model": CLASSIFIER_NAME, "version": b["classifier"]["model_version"]}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/fraud/score", response_model=FraudScoreResponse)
def score(req: FraudScoreRequest):
    b = _bundle()
    classifier = b["classifier"]
    anomaly = b["anomaly"]
    policy = b["policy"]

    row = {
        "amount": req.amount,
        "amount_zscore_30d": req.amount_zscore_30d,
        "tx_count_24h": req.tx_count_24h,
        "days_since_last_tx": req.days_since_last_tx,
        "channel": req.channel,
        "device_country": req.device_country,
        "merchant_segment": req.merchant_segment,
    }
    X = pd.DataFrame([row]).reindex(columns=classifier["columns"])
    fraud_probability = float(classifier["pipeline"].predict_proba(X)[0, 1])
    raw_anomaly = float(-anomaly["pipeline"].decision_function(X)[0])
    anomaly_score = float(1 / (1 + np.exp(-raw_anomaly)))
    combined = min(1.0, 0.85 * fraud_probability + 0.15 * anomaly_score)
    band, action = _band(combined, policy["high_threshold"], policy["medium_threshold"])

    reasons = []
    if req.amount_zscore_30d >= 3:
        reasons.append("HIGH_AMOUNT_ZSCORE")
    if req.tx_count_24h >= 8:
        reasons.append("HIGH_VELOCITY_24H")
    if fraud_probability >= policy["high_threshold"]:
        reasons.append("MODEL_HIGH_PROBABILITY")
    if anomaly_score >= 0.80:
        reasons.append("ANOMALY_PATTERN")

    return FraudScoreResponse(
        transaction_id=req.transaction_id,
        model_name=CLASSIFIER_NAME,
        model_version=classifier["model_version"],
        fraud_probability=round(fraud_probability, 4),
        anomaly_score=round(anomaly_score, 4),
        combined_score=round(combined, 4),
        risk_band=band,
        action=action,
        reason_code=",".join(reasons) or "BASELINE_SCORE",
    )
```

### Day 4 완료 기준

```bash
make fraud-score
make fraud-api-test
```

검증:

```bash
docker compose exec postgres psql -U mluser -d ml_db -c \
  "SELECT risk_band, action, COUNT(*) FROM risk_score GROUP BY 1,2 ORDER BY 1,2;"
```

완료 조건:

* `risk_score` 에 `HIGH/MEDIUM/LOW` band 가 적재된다.
* `prediction_result` 에 fraud score 공통 로그가 함께 적재된다.
* `/fraud/score` 응답에 `fraud_probability`, `anomaly_score`, `combined_score`, `risk_band`, `action`, `reason_code` 가 포함된다.

---

## Day 5: DAG + 테스트 + 운영 리포트

### 5-1. `dags/fraud_scoring_pipeline_dag.py`

```python
# dags/fraud_scoring_pipeline_dag.py
"""Nexus Pay fraud scoring DAG draft."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


DEFAULT_ARGS = {
    "owner": "ml-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PIPELINE_STEPS = [
    ("fraud_features", "python -m features.build_fraud_features --config config/fraud_model_config.yaml"),
    ("fraud_train", "python -m models.train_fraud_classifier --config config/fraud_model_config.yaml"),
    ("fraud_anomaly", "python -m models.train_anomaly_detector --config config/fraud_model_config.yaml"),
    ("fraud_threshold", "python -m models.tune_fraud_threshold --config config/fraud_model_config.yaml"),
    ("fraud_score", "python -m models.batch_score_fraud --config config/fraud_model_config.yaml"),
]


def build_dag():
    if not AIRFLOW_AVAILABLE:
        raise RuntimeError("Airflow not installed in this container; use --validate for local validation")

    dag = DAG(
        dag_id="fraud_scoring_pipeline",
        description="Nexus Pay fraud feature -> train -> threshold -> score pipeline",
        start_date=datetime(2026, 6, 1),
        schedule_interval="0 4 * * *",
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["nexuspay", "fraud", "week2"],
    )

    previous = None
    for task_id, command in PIPELINE_STEPS:
        op = BashOperator(task_id=task_id, bash_command=command, dag=dag)
        if previous is not None:
            previous >> op
        previous = op
    return dag


def _validate() -> int:
    print("Fraud pipeline steps:")
    for task_id, command in PIPELINE_STEPS:
        print(f"  - {task_id}: {command}")
    expected = ["fraud_features", "fraud_train", "fraud_anomaly", "fraud_threshold", "fraud_score"]
    actual = [task_id for task_id, _ in PIPELINE_STEPS]
    if actual != expected:
        print("unexpected DAG order", file=sys.stderr)
        return 1
    print("validation OK")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    if args.validate:
        sys.exit(_validate())
    if AIRFLOW_AVAILABLE:
        globals()["dag"] = build_dag()
    else:
        print("airflow not installed; run with --validate", file=sys.stderr)
        sys.exit(1)
```

### 5-2. `tests/test_fraud_pipeline.py`

```python
# tests/test_fraud_pipeline.py
import json
from pathlib import Path

import pandas as pd


def test_threshold_policy_shape(tmp_path: Path) -> None:
    policy = {
        "model_name": "nexuspay_fraud_classifier",
        "model_version": "20260601-010101",
        "high_threshold": 0.72,
        "medium_threshold": 0.43,
        "alert_capacity_per_day": 300,
    }
    path = tmp_path / "threshold_policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["high_threshold"] > loaded["medium_threshold"]
    assert loaded["alert_capacity_per_day"] > 0


def test_risk_band_policy_order() -> None:
    scores = pd.Series([0.91, 0.55, 0.10])
    high, medium = 0.70, 0.35
    bands = [
        "HIGH" if s >= high else "MEDIUM" if s >= medium else "LOW"
        for s in scores
    ]
    assert bands == ["HIGH", "MEDIUM", "LOW"]


def test_reason_codes_are_explainable() -> None:
    reason_code = "HIGH_AMOUNT_ZSCORE,HIGH_VELOCITY_24H,MODEL_HIGH_PROBABILITY"
    allowed = {
        "HIGH_AMOUNT_ZSCORE",
        "HIGH_VELOCITY_24H",
        "OVERSEAS_DEVICE",
        "MODEL_HIGH_PROBABILITY",
        "ANOMALY_PATTERN",
        "BASELINE_SCORE",
    }
    assert set(reason_code.split(",")).issubset(allowed)
```

### 5-3. `docs/reports/fraud-model-evaluation.md`

```bash
cat > docs/reports/fraud-model-evaluation.md << 'EOF'
# Nexus Pay Fraud Model Evaluation — Week 2

## 1. 모델 목적

Nexus Pay 결제 거래를 `HIGH/MEDIUM/LOW` risk band 로 분류하여 운영팀 검토 우선순위를 제공한다.

## 2. 모델 구성

- Supervised classifier: fraud probability 산출
- Isolation Forest: anomaly score 산출
- Threshold policy: 운영팀 처리 가능 건수와 precision 기준 반영

## 3. 핵심 지표

| 지표 | 의미 | 운영 해석 |
|------|------|-----------|
| precision | alert 중 실제 fraud 비율 | 운영팀 검토 효율 |
| recall | 전체 fraud 중 탐지 비율 | 미탐 손실 방어 |
| PR-AUC | class imbalance 환경의 ranking 품질 | fraud 모델 비교 기준 |
| high alert count | HIGH band 건수 | COO 처리 용량 |

## 4. CFO 비용 해석

- False positive cost: 운영 검토 1건 비용
- False negative cost: fraud 미탐 1건 평균 손실
- Manual review capacity cost: 하루 검토 가능 인력/시간 비용

## 5. 운영 인수인계

- `risk_score` 는 운영 조회 테이블이다.
- `prediction_result` 는 모델 공통 예측 로그다.
- `data_lineage` 에 feature/predict 실행 흔적이 남아야 한다.
- threshold 변경 시 `threshold_policy.json` 을 새 버전으로 저장한다.
EOF
```

### 5-4. End-to-end 검증 시나리오

```bash
# 0. Week 1 환경 기동
make up
make healthcheck

# 1. Spark Silver 거래 데이터 + 사후 확정 fraud label 준비
# runner 컨테이너에 /upstream/silver/transactions 마운트가 필요하다.
make fraud-labels
make fraud-import-upstream

# 2. Week 2 스키마 + feature
make fraud-schema
make fraud-features

# 3. 모델 학습 + threshold
make fraud-train
make fraud-anomaly
make fraud-threshold

# 4. risk score 적재 + API 확인
make fraud-score
make fraud-api-test

# 5. DAG + 테스트
make fraud-dag-validate
make fraud-test

# 6. 결과 확인
docker compose exec postgres psql -U mluser -d ml_db -c \
  "SELECT risk_band, COUNT(*) FROM risk_score GROUP BY 1 ORDER BY 1;"
```

기대 결과:

* `risk_score` 에 50,000건 가까운 score row 가 적재된다.
* `risk_band` 는 `HIGH/MEDIUM/LOW` 로 분포한다.
* `data/artifacts/nexuspay_fraud_classifier/threshold_policy.json` 이 존재한다.
* MLflow UI 에 classifier/anomaly detector run 이 기록된다.
* `fraud-api-test` 는 JSON 응답으로 `risk_band`, `action`, `reason_code` 를 반환한다.
* `fraud-dag-validate` 와 `fraud-test` 가 성공한다.

### Day 5 완료 기준

* `make fraud-dag-validate` 가 `validation OK` 로 끝난다.
* `make fraud-test` 가 통과한다.
* `docs/reports/fraud-model-evaluation.md` 가 작성되어 CFO/COO 관점의 운영 해석을 포함한다.
* `risk_score`, `prediction_result`, `data_lineage` 를 함께 조회해 모델 입력·출력·실행 lineage 를 설명할 수 있다.

---

## Week 2 산출물 체크리스트

다음 항목이 모두 채워져야 Week 2 가 완료된 것으로 본다. 완료된 항목 옆에 날짜를 적어 두면 Week 3 가이드에서 인용하기 쉽다.

### 코드 산출물

- [ ] `study-ml-pipeline/config/fraud_model_config.yaml`
- [ ] `study-ml-pipeline/schemas/risk_score.sql`
- [ ] `study-ml-pipeline/scripts/generate_fraud_labels_from_silver.py`
- [ ] `study-ml-pipeline/scripts/import_spark_silver_to_raw_transactions.py`
- [ ] `study-ml-pipeline/features/build_fraud_features.py`
- [ ] `study-ml-pipeline/models/train_fraud_classifier.py`
- [ ] `study-ml-pipeline/models/train_anomaly_detector.py`
- [ ] `study-ml-pipeline/models/tune_fraud_threshold.py`
- [ ] `study-ml-pipeline/models/batch_score_fraud.py`
- [ ] `study-ml-pipeline/serving/fraud_app.py`
- [ ] `study-ml-pipeline/dags/fraud_scoring_pipeline_dag.py`
- [ ] `study-ml-pipeline/tests/test_fraud_pipeline.py`
- [ ] `study-ml-pipeline/Makefile` fraud 타겟 추가

### 문서 산출물

- [ ] `study-ml-pipeline/docs/models/fraud-detection-design.md`
- [ ] `study-ml-pipeline/docs/reports/fraud-model-evaluation.md`
- [ ] `study-ml-pipeline/data/reports/fraud_eval_report_*.md` (선택: 자동 생성 리포트)

### 운영 산출물

- [ ] `make fraud-schema` 성공
- [ ] `make fraud-labels` 성공
- [ ] `make fraud-import-upstream` 성공
- [ ] `make fraud-features` 성공
- [ ] `make fraud-train` 성공
- [ ] `make fraud-anomaly` 성공
- [ ] `make fraud-threshold` 성공
- [ ] `make fraud-score` 성공
- [ ] `make fraud-api-test` 성공
- [ ] `make fraud-dag-validate` 성공
- [ ] `make fraud-test` 성공
- [ ] MLflow UI 에 classifier/anomaly detector run 기록
- [ ] `risk_score` 테이블에 1만 건 이상의 score row 적재

### CTO/CIO/COO/CFO/CISO 요구사항 정합 점검

| 요구자 | 요구사항 | 충족 증빙 |
|--------|----------|-----------|
| CTO | Week 1 공통 ML pipeline 의 모델별 확장 가능성 | fraud 전용 config/model/DAG |
| CIO | feature/model/prediction lineage | `feature_store`, `prediction_result`, `risk_score`, `data_lineage` |
| COO | 처리 가능 alert volume | `threshold_policy.json`, `risk_band` 분포 |
| CFO | 오탐/미탐 비용 해석 | `fraud-model-evaluation.md`, `model_evaluation.extra_json` |
| CISO | 민감정보 없는 설명 가능 score | `reason_code`, 비식별 transaction/customer ID |

---

## 다음 주(Week 3) 연결 포인트

Week 3 (수요·매출 예측과 고객 세분화) 는 Week 1~2 의 다음 자산을 상속한다.

* `feature_store` 계약 — forecast/segment feature set 으로 확장
* `model_evaluation` — 분류 지표 외 MAE/RMSE/silhouette 등 task-specific metric 추가
* `data_lineage` — mart 생성, 학습, batch scoring 단계 추적
* `threshold_policy.json` 패턴 — forecast alert policy 또는 segment action policy 로 확장
* `risk_score` 운영 테이블 패턴 — Week 3 의 forecast/segment 운영 테이블 설계에 참고

Week 3 에서는 fraud scoring 자체를 변경하지 않는다. 모델 운영 패턴을 수요 예측과 고객 세그먼트 문제에 재사용하는 것이 핵심이다.
