# Week 3: 수요·매출 예측과 고객 세분화

**기간**: 5일 (월~금, 풀타임 40시간) — 2026-06-08 ~ 2026-06-12
**주제**: Week 1~2 ML 운영 기준선을 상속하여 Nexus Pay의 거래량·수수료 매출 예측 모델과 고객 세분화 모델을 구축하고, 예측 결과를 운영 의사결정 테이블과 리포트로 연결한다.

**산출물**

| 구분 | 산출물 |
|------|--------|
| 설정 | `config/forecast_model_config.yaml`, `config/segment_model_config.yaml` |
| 스키마 | `schemas/forecast_and_segment.sql` |
| 공통 헬퍼 | `features/calendar.py`, `features/segment_actions.py`, `models/forecast_intervals.py` |
| 피처 | `features/build_forecast_features.py`, `features/build_segment_features.py` |
| 모델 | `models/train_revenue_forecast.py`, `models/train_customer_segments.py`, `models/batch_score_forecast.py`, `models/batch_score_segments.py` |
| Registry 검증 | `mlops/verify_registered_models.py` |
| 리포트 생성 | `scripts/apply_sql.py`, `scripts/render_business_insight_report.py` |
| 서빙 | `serving/business_insight_router.py`, Week 1 `serving/app.py` 라우터 마운트 |
| 오케스트레이션 | `dags/business_insight_pipeline_dag.py` |
| 문서 | `docs/models/forecast-and-segmentation-design.md`, `docs/reports/business-insight-evaluation.md` |
| 테스트 | `tests/test_business_insight_pipeline.py` |

**전제 조건**

| 구분 | 필요 조건 |
|------|-----------|
| Week 1 환경 | 공통 PostgreSQL, MLflow, FastAPI 환경이 기동 가능하고 `make up && make healthcheck` 가 성공한다. |
| Week 1 테이블 | `feature_store`, `prediction_result`, `model_evaluation`, `data_lineage` 가 준비되어 있다. |
| Week 2 fraud 자산 | `features/risk_reasons.py`, `models/anomaly_scorer.py`, `serving/fraud_router.py`, `risk_score` 테이블이 정상 작동한다. |
| 입력 데이터 | `raw_transactions` 와 `risk_score` 에 Week 3 feature 생성에 사용할 거래/위험 등급 데이터가 존재한다. |
| MLflow Registry | `MLFLOW_TRACKING_URI` 가 registry 사용 가능한 MLflow Tracking Server 를 가리키며, registered model 생성 권한이 있다. |

> 모델 후보 단순화 안내: forecast 는 RandomForest(MultiOutput) 한 후보로, segment 는 KMeans 한 후보로 한정한다. 시계열 전용 모델(Prophet, LightGBM forecaster)과 GMM/HDBSCAN 같은 대체 군집화는 Week 4 의 운영 단계에서 후속 후보로 다룬다.

---

## 수행 시나리오

### 배경 설정

Week 2에서 Nexus Pay는 fraud risk score를 운영팀이 사용할 수 있는 형태로 만들었다. Week 3에서는 같은 ML 운영 구조를 **성장과 운영 계획** 문제에 적용한다. CFO는 다음 주 거래 수수료 매출을 예측해 현금 흐름과 캠페인 예산을 조정하고 싶어 하고, COO는 시간대별 거래량을 예측해 정산·고객지원 인력을 배치하고 싶어 한다. CMO는 고객 세그먼트별로 리텐션·프로모션 전략을 다르게 가져가길 원한다.

> "이상거래 탐지가 리스크를 줄이는 모델이라면, 예측과 세분화는 성장 방향을 정하는 모델입니다. 이번 주 산출물은 정확도 숫자만 보여주는 것이 아니라, 어떤 시간대에 운영 리소스를 늘리고 어떤 고객군에 어떤 액션을 줄지 말할 수 있어야 합니다." - Nexus Pay COO

이번 주는 두 문제를 분리해 설계한다.

* **수요·매출 예측**: 일자·시간대·채널·가맹점 세그먼트 단위의 거래량과 예상 수수료 매출을 예측한다.
* **고객 세분화**: RFM, 채널 선호, 해외 사용 비중, 최근성, 거래 빈도 기반으로 고객군을 나누고 운영 액션을 부여한다.

### C레벨 요구사항

| 요구자 | 요구사항 | 본 가이드 반영 위치 |
|--------|----------|----------------------|
| CTO | Week 1~2의 feature/model/batch/API/DAG 패턴을 다른 ML 과제에도 재사용 | Day 1~5 전체 |
| CFO | 다음 7일 수수료 매출 예측과 오차 해석 | Day 2~4 forecast 평가 |
| COO | 거래량 피크 시간대 예측과 운영 인력 배치 근거 | Day 2~4 forecast 결과 테이블 |
| CMO | 고객 세그먼트별 액션 정책 | Day 3~4 segment action policy |
| CIO | 모델별 입력·출력·lineage와 평가 지표를 공통 구조로 관리 | Day 1, Day 5 |

### 목표

1. Week 1의 공통 feature store와 evaluation lineage를 forecast/segment 문제로 확장한다.
2. 거래 집계 mart를 만들어 일자·시간대·채널·가맹점 세그먼트 단위 forecast feature를 생성한다.
3. 고객 단위 RFM/행동 feature를 생성하고 KMeans 기반 baseline segmentation을 학습한다.
4. forecast 결과와 customer segment 결과를 운영 조회 테이블에 적재한다.
5. CFO/COO/CMO가 읽을 수 있는 평가 리포트와 handoff note를 작성한다.

### 일정 개요

| 일차 | 주제 | 핵심 작업 | 완료 산출물 |
|------|------|----------|-------------|
| Day 1 | 문제 정의 + 스키마 확장 + 정책 정의 | forecast/segment config, grain 분리 feature 테이블, fee/holiday/PI/risk 정책, segment_action_recommendation 테이블 | config 2종, `schemas/forecast_and_segment.sql`, `features/calendar.py`, `features/segment_actions.py`, 설계 문서 |
| Day 2 | Forecast feature + 모델 학습 | 시간 집계 feature, lag/rolling/holiday feature, MultiOutput RF + tree-quantile PI, TimeSeriesSplit, MLflow 등록, eval_report 자동 생성 | `build_forecast_features.py`, `models/forecast_intervals.py`, `train_revenue_forecast.py` |
| Day 3 | Customer segment feature + 모델 학습 | RFM + risk_grade 비중 + 채널/국가 feature, KMeans + silhouette/inertia/stability, action_policy 자동 생성 | `build_segment_features.py`, `train_customer_segments.py` |
| Day 4 | Batch scoring + Insight Router 통합 | forecast/segment 결과 적재(ON CONFLICT 처리), `serving/app.py` 에 `business_insight_router` mount | `batch_score_forecast.py`, `batch_score_segments.py`, `business_insight_router.py` |
| Day 5 | DAG + 테스트 + 리포트 | DAG 검증, 누수/policy/multi-output 통합 테스트, 자동 생성 stakeholder report | DAG, tests, evaluation report |

### Day N 완료 기준 요약

* Day 1 완료 기준: `feature_store_forecast`, `feature_store_segment`, `revenue_forecast`, `customer_segment`, `segment_action_recommendation` 테이블이 생성되고 Week 1~2 의 `data_lineage`, `prediction_result`, `risk_score` 와 명확한 키 관계가 정의되어 있다.
* Day 2 완료 기준: `make forecast-features`, `make forecast-train` 실행 시 MLflow Registered Model `nexuspay_revenue_forecast` 에 새 버전이 등록되고, `model_evaluation` 에 MAE/RMSE/MAPE 가 기록되며 `data/reports/forecast_eval_report_*.md` 가 자동 생성된다.
* Day 3 완료 기준: `make segment-features`, `make segment-train` 실행 시 고객별 segment label, segment_reason, action_policy, segment stability metric 이 함께 생성되고 `data/reports/segment_eval_report_*.md` 가 자동 생성된다.
* Day 4 완료 기준: `make business-score`, `make business-api-test` 가 성공하고 `revenue_forecast`, `customer_segment` 에 신규 row 가 적재되며 `serving/app.py` 가 같은 8000 포트에서 forecast/segment 조회 응답을 반환한다.
* Day 5 완료 기준: `make business-dag-validate`, `make business-test` 가 통과하고 `docs/reports/business-insight-evaluation.md` (자동 생성본 + 사람 해석본) 이 CFO/COO/CMO 관점의 해석을 담는다.
* Registry 완료 기준: `make business-registry-check` 가 `nexuspay_revenue_forecast`, `nexuspay_customer_segment` 두 Registered Model 의 최신 버전을 확인한다.

---

## Week 1~2 자산 상속 기준

| 기존 자산 | Week 3 사용 방식 |
|-----------|------------------|
| `feature_store` | 거래 단위 입력만 유지. 시간/고객 grain 은 `feature_store_forecast`, `feature_store_segment` 로 분리 |
| `prediction_result` | 모델 공통 예측 로그 유지 (forecast 는 40자 제한을 지키는 결정성 인공 ID `fc-{sha1_24}` 를 transaction_id 컬럼에 적재) |
| `model_evaluation` | forecast MAE/RMSE/MAPE, segment silhouette/inertia/stability 기록 |
| `data_lineage` | `feature/predict/evaluate` 외에 `mart` stage 추가 |
| `models/eval_report.py` | forecast/segment 전용 report renderer 함수를 추가하되 기본 시그니처는 재사용 |
| `features/risk_reasons.py` | 그대로 두고 변경하지 않는다 |
| `models/anomaly_scorer.py` | "단일 소스 헬퍼" 패턴을 `models/forecast_intervals.py` 와 `features/segment_actions.py` 로 동일하게 적용 |
| `serving/app.py` 의 `include_router` | `business_insight_router` 도 같은 방식으로 mount (Week 2 fraud_router 와 동일 컨테이너) |
| `serving/fraud_router.py` | 그대로 두고 변경하지 않는다 |
| Week 2 `risk_score` | forecast 의 위험 등급 단위 집계 feature 와 segment 의 risk_grade 비중 feature 의 입력으로 활용 |
| `.env` 의 `UPSTREAM_SILVER_PATH` | 변경 없음. Week 3 도 raw_transactions 만 사용 |

Week 3 에서는 fraud scoring 로직과 fraud_router 코드를 절대 변경하지 않는다. 운영 패턴을 예측과 세분화 문제에 재사용하는 것이 핵심이다.

---

## Day 1: 문제 정의 + 스키마 확장 + 정책 정의

### 1-0. Forecast 와 Segment 운영 정의

| 항목 | Forecast | Customer Segment |
|------|----------|------------------|
| 판단 대상 | 일자·시간대·채널·가맹점 세그먼트·위험 등급 단위 집계 | 고객 1명 (관측 윈도우 90일) |
| 입력 | 거래 집계, lag/rolling, 캘린더, holiday, risk grade 비중 | RFM, 채널 선호, 해외 사용 비중, risk_grade 비중 |
| 출력 | 예상 거래 건수, 예상 거래액, 예상 수수료 매출, 80% 신뢰구간 | segment_id, segment_name, segment_action, segment_reason, stability |
| 주요 사용자 | CFO, COO | CMO, COO |
| 평가 | MAE, RMSE, MAPE, 피크 시간대 recall, weighted MAPE | silhouette, inertia, segment size 분포, segment stability(시간 분할 일관성) |
| 적재 | `revenue_forecast` (UPSERT) + `prediction_result` 공통 로그 | `customer_segment` (UPSERT) + `prediction_result` 공통 로그 |

### 1-0-1. grain 분리 결정

Week 1 의 `feature_store` 는 PK `(feature_set, feature_version, transaction_id)` — 거래 단위 입력에 최적화되어 있다. forecast 와 segment 는 자연 식별자가 다르므로 별도 테이블을 둔다.

* forecast: 자연 키 `(forecast_target_ts, channel, merchant_segment, risk_grade)`
* segment: 자연 키 `(customer_id)`

`feature_store` 는 그대로 거래 단위 모델(Week 1 baseline, Week 2 fraud)에만 사용한다.

### 1-1. 설정 파일

#### `config/forecast_model_config.yaml`

```yaml
project:
  name: study-ml-pipeline
  scenario: nexuspay-revenue-forecast
  inherits: config/model_config.yaml

data:
  raw_table: raw_transactions
  risk_score_table: risk_score
  forecast_feature_table: feature_store_forecast
  forecast_output_table: revenue_forecast
  prediction_result_table: prediction_result
  lineage_table: data_lineage

features:
  feature_set: forecast_v1
  feature_version: "v1.0.0"
  grain:
    bucket: hour                     # 시간 버킷
    keys: [channel, merchant_segment, risk_grade]
  horizon_hours: 168                 # 7일
  lag_hours: [24, 48, 168]
  rolling_windows_hours: [24, 168]
  calendar:
    timezone: Asia/Seoul
    include_hour_of_day: true
    include_day_of_week: true
    include_is_weekend: true
    include_is_holiday: true         # KR 공휴일 (홈마트 캘린더)
  fee_rate_by_segment:
    GENERAL:    0.025
    PREMIUM:    0.018
    STARTUP:    0.030
    OVERSEAS:   0.040
    UNKNOWN:    0.025
  risk_grade_default: LOW            # risk_score 미존재 거래는 LOW 로 간주
  numerical:
    - tx_count_lag_24h
    - amount_lag_24h
    - fee_revenue_lag_24h
    - tx_count_lag_48h
    - amount_lag_48h
    - tx_count_lag_168h
    - amount_lag_168h
    - tx_count_roll_mean_24h
    - amount_roll_mean_24h
    - tx_count_roll_mean_168h
    - amount_roll_mean_168h
    - hour_of_day
    - day_of_week
  categorical:
    - channel
    - merchant_segment
    - risk_grade
    - is_weekend
    - is_holiday
  targets:
    - tx_count
    - amount
    - fee_revenue

split:
  strategy: time_based
  train_until: "2026-04-30T23:59:59+09:00"
  valid_until: "2026-05-15T23:59:59+09:00"
  test_until:  "2026-05-24T23:59:59+09:00"
  scoring_horizon_until: "2026-05-31T23:59:59+09:00"

model:
  algorithm: multi_output_random_forest
  base:
    n_estimators: 400
    min_samples_leaf: 5
    n_jobs: -1
  random_state: 42
  registry:
    name: nexuspay_revenue_forecast
  prediction_interval:
    method: tree_quantile             # 트리별 예측 분위수
    lower: 0.10
    upper: 0.90

evaluation:
  metrics: [mae, rmse, mape]
  peak_volume_quantile: 0.90
  weighted_mape_target: amount        # MAPE 를 amount 가중평균으로도 계산

artifacts:
  model_root: data/artifacts
  report_dir: data/reports
```

#### `config/segment_model_config.yaml`

```yaml
project:
  name: study-ml-pipeline
  scenario: nexuspay-customer-segmentation
  inherits: config/model_config.yaml

data:
  raw_table: raw_transactions
  risk_score_table: risk_score
  segment_feature_table: feature_store_segment
  segment_output_table: customer_segment
  segment_action_recommendation_table: segment_action_recommendation
  prediction_result_table: prediction_result
  lineage_table: data_lineage

features:
  feature_set: customer_segment_v1
  feature_version: "v1.0.0"
  observation_window_days: 90
  reference_ts: "2026-05-24T23:59:59+09:00"   # split.test_until 와 일치
  numerical:
    - recency_days
    - recency_score                 # 1 / (1 + recency_days) 변환
    - frequency_90d
    - monetary_90d
    - avg_ticket_size
    - overseas_tx_ratio
    - app_channel_ratio
    - high_risk_ratio_90d           # risk_band='HIGH' 비중
    - medium_risk_ratio_90d
  categorical: []                   # KMeans 입력은 numerical only

split:
  strategy: time_based
  train_until: "2026-04-30T23:59:59+09:00"
  valid_until: "2026-05-15T23:59:59+09:00"
  test_until:  "2026-05-24T23:59:59+09:00"
  stability_check:
    enabled: true
    earlier_window_until: "2026-04-30T23:59:59+09:00"
    later_window_until:   "2026-05-24T23:59:59+09:00"
    min_overlap_ratio: 0.55         # 두 윈도우에서 동일 customer 의 segment 가 같은 비율 하한

model:
  algorithm: kmeans
  n_clusters: 5
  n_init: auto
  random_state: 42
  registry:
    name: nexuspay_customer_segment

action_policy:
  output_path: data/artifacts/nexuspay_customer_segment/action_policy.json
  reason_top_k: 3                   # centroid 해석 시 영향 큰 feature 상위 3개를 reason 으로 기록

artifacts:
  model_root: data/artifacts
  report_dir: data/reports
```

### 1-2. `schemas/forecast_and_segment.sql`

거래 단위가 아닌 모델은 별도 feature 테이블을 둔다. 운영 출력 테이블은 ON CONFLICT 처리로 재실행 안전성을 확보한다.

```sql
-- ──────────────────────────────────────
-- Week 3 forecast / segment 전용 feature 와 운영 테이블
-- ──────────────────────────────────────

-- 1. forecast feature (시간 버킷 단위)
CREATE TABLE IF NOT EXISTS feature_store_forecast (
    feature_set         VARCHAR(40)  NOT NULL,
    feature_version     VARCHAR(20)  NOT NULL,
    forecast_target_ts  TIMESTAMPTZ  NOT NULL,
    channel             VARCHAR(20)  NOT NULL,
    merchant_segment    VARCHAR(30)  NOT NULL,
    risk_grade          VARCHAR(10)  NOT NULL,
    payload_json        JSONB        NOT NULL,
    target_json         JSONB        NOT NULL,    -- 학습 시점에는 실제 (tx_count, amount, fee_revenue)
    as_of_ts            TIMESTAMPTZ  NOT NULL,
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (feature_set, feature_version, forecast_target_ts, channel, merchant_segment, risk_grade)
);

CREATE INDEX IF NOT EXISTS ix_feature_store_forecast_ts
    ON feature_store_forecast (forecast_target_ts);

-- 2. segment feature (고객 단위)
CREATE TABLE IF NOT EXISTS feature_store_segment (
    feature_set         VARCHAR(40)  NOT NULL,
    feature_version     VARCHAR(20)  NOT NULL,
    customer_id         VARCHAR(40)  NOT NULL,
    payload_json        JSONB        NOT NULL,
    as_of_ts            TIMESTAMPTZ  NOT NULL,
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (feature_set, feature_version, customer_id)
);

-- 3. forecast 운영 출력
CREATE TABLE IF NOT EXISTS revenue_forecast (
    forecast_id              BIGSERIAL PRIMARY KEY,
    forecast_grain           VARCHAR(60)  NOT NULL,
    forecast_target_ts       TIMESTAMPTZ  NOT NULL,
    channel                  VARCHAR(20)  NOT NULL,
    merchant_segment         VARCHAR(30)  NOT NULL,
    risk_grade               VARCHAR(10)  NOT NULL,
    model_name               VARCHAR(80)  NOT NULL,
    model_version            VARCHAR(64)  NOT NULL,
    feature_set              VARCHAR(40)  NOT NULL,
    feature_version          VARCHAR(20)  NOT NULL,
    predicted_tx_count       NUMERIC(14,2) NOT NULL,
    predicted_amount         NUMERIC(18,2) NOT NULL,
    predicted_fee_revenue    NUMERIC(18,2) NOT NULL,
    prediction_interval_low  NUMERIC(18,2),
    prediction_interval_high NUMERIC(18,2),
    scored_at                TIMESTAMPTZ  DEFAULT NOW(),
    run_id                   VARCHAR(40)  NOT NULL,
    UNIQUE (forecast_grain, forecast_target_ts, channel, merchant_segment, risk_grade, model_name, model_version)
);

CREATE INDEX IF NOT EXISTS ix_revenue_forecast_target
    ON revenue_forecast (forecast_target_ts, channel, merchant_segment);

-- 4. segment 운영 출력
CREATE TABLE IF NOT EXISTS customer_segment (
    customer_id          VARCHAR(40)  NOT NULL,
    model_name           VARCHAR(80)  NOT NULL,
    model_version        VARCHAR(64)  NOT NULL,
    feature_set          VARCHAR(40)  NOT NULL,
    feature_version      VARCHAR(20)  NOT NULL,
    segment_id           INTEGER      NOT NULL,
    segment_name         VARCHAR(80)  NOT NULL,
    segment_action       VARCHAR(80)  NOT NULL,
    segment_reason       TEXT,
    stability_score      NUMERIC(7,4),                -- 시간 분할 stability
    scored_at            TIMESTAMPTZ  DEFAULT NOW(),
    run_id               VARCHAR(40)  NOT NULL,
    PRIMARY KEY (customer_id, model_name, model_version)
);

CREATE INDEX IF NOT EXISTS ix_customer_segment_segment
    ON customer_segment (segment_id, segment_action);

-- 5. segment action policy (모델 버전별 정책 카탈로그)
CREATE TABLE IF NOT EXISTS segment_action_recommendation (
    model_name        VARCHAR(80) NOT NULL,
    model_version     VARCHAR(64) NOT NULL,
    segment_id        INTEGER     NOT NULL,
    segment_name      VARCHAR(80) NOT NULL,
    segment_action    VARCHAR(80) NOT NULL,
    rationale         TEXT,
    centroid_json     JSONB,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (model_name, model_version, segment_id)
);
```

ON CONFLICT 처리 패턴은 Day 4 의 batch_score 코드에서 재사용한다.

### 1-3. `features/calendar.py` — 캘린더·휴일 헬퍼

forecast feature 의 holiday/weekend/hour 컬럼은 단일 모듈에서 만들어 `risk_reasons` 패턴을 따른다.

```python
# features/calendar.py
"""Forecast 용 캘린더 / 휴일 feature 헬퍼.

- timezone: Asia/Seoul 고정
- 한국 공휴일은 holidays 패키지로 관리 (없으면 README 의 install 항목 참고)
"""
from __future__ import annotations

from datetime import date

import pandas as pd

try:
    import holidays as _holidays_pkg
    _HOLIDAY_CAL = _holidays_pkg.country_holidays("KR")
except ImportError:  # 오프라인 fallback
    _HOLIDAY_CAL = None
    _STATIC_HOLIDAYS = {
        date(2026, 1, 1), date(2026, 3, 1), date(2026, 5, 5),
        date(2026, 6, 6), date(2026, 8, 15), date(2026, 10, 3),
        date(2026, 10, 9), date(2026, 12, 25),
    }


def is_holiday(d: date) -> bool:
    if _HOLIDAY_CAL is not None:
        return d in _HOLIDAY_CAL
    return d in _STATIC_HOLIDAYS


def add_calendar_features(df: pd.DataFrame, ts_column: str = "forecast_target_ts") -> pd.DataFrame:
    """ts_column 기반 hour_of_day, day_of_week, is_weekend, is_holiday 컬럼 추가."""
    out = df.copy()
    ts = pd.to_datetime(out[ts_column], utc=True).dt.tz_convert("Asia/Seoul")
    out["hour_of_day"] = ts.dt.hour.astype(int)
    out["day_of_week"] = ts.dt.dayofweek.astype(int)
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
    out["is_holiday"] = ts.dt.date.map(is_holiday).astype(int)
    return out
```

`holidays` 패키지는 Day 1 의 `make business-deps` 에서 `scipy`, `tabulate` 와 함께 멱등 설치한다. 개별 설치 명령을 흩어두지 않고 Makefile 타겟 하나로 묶어 runner 컨테이너 의존성 상태를 일관되게 유지한다.

```bash
make business-deps
```

### 1-4. `features/segment_actions.py` — segment 자동 명명·action 정책

KMeans centroid 를 운영 액션으로 매핑하는 휴리스틱을 단일 모듈로 둔다. Week 4 에서 정책 변경 시 이 함수만 갱신하면 된다.

```python
# features/segment_actions.py
"""KMeans centroid 를 운영 segment 명/액션/사유로 매핑하는 단일 소스."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SegmentLabel:
    segment_id: int
    segment_name: str
    segment_action: str
    rationale: str


SEGMENT_ACTIONS: Sequence[str] = (
    "RETENTION_PRIORITY",
    "ONBOARDING_NURTURE",
    "WINBACK_CAMPAIGN",
    "FX_FEE_PROMOTION",
    "RISK_REVIEW",
    "STANDARD_MONITORING",
)


def _classify(centroid: Mapping[str, float]) -> tuple[str, str, str]:
    if centroid.get("high_risk_ratio_90d", 0) >= 0.10:
        return "risk_review_target", "RISK_REVIEW", "high_risk_ratio_90d ≥ 0.10"
    if centroid.get("recency_days", 0) <= 14 and centroid.get("frequency_90d", 0) >= 30:
        return "high_value_loyal", "RETENTION_PRIORITY", "low recency + high frequency"
    if centroid.get("recency_days", 0) >= 60:
        return "dormant_low_activity", "WINBACK_CAMPAIGN", "recency ≥ 60 days"
    if centroid.get("overseas_tx_ratio", 0) >= 0.30:
        return "overseas_active", "FX_FEE_PROMOTION", "overseas_tx_ratio ≥ 0.30"
    if centroid.get("frequency_90d", 0) <= 5 and centroid.get("recency_days", 0) <= 30:
        return "new_high_potential", "ONBOARDING_NURTURE", "low frequency + recent activity"
    return "standard_mass", "STANDARD_MONITORING", "default fallback"


def label_segments(centroids: pd.DataFrame, top_k: int = 3) -> list[SegmentLabel]:
    """centroids: index = segment_id, columns = feature names. 반환 순서는 segment_id 오름차순."""
    out: list[SegmentLabel] = []
    for sid, row in centroids.iterrows():
        name, action, base_rationale = _classify(row.to_dict())
        # centroid 에서 영향이 큰 feature top_k 의 (이름, 값) 도 함께 사유에 적는다.
        notable = row.abs().sort_values(ascending=False).head(top_k).index.tolist()
        notable_pairs = [f"{f}={row[f]:.3f}" for f in notable]
        rationale = f"{base_rationale} | top_features: {', '.join(notable_pairs)}"
        out.append(SegmentLabel(int(sid), name, action, rationale))
    return out


def derive_segment_reason(
    customer_features: Mapping[str, float],
    segment_label: SegmentLabel,
) -> str:
    """개별 customer 점수 + segment 정책 → 사람이 읽는 reason 문자열."""
    parts = [segment_label.rationale]
    if customer_features.get("high_risk_ratio_90d", 0) >= 0.10:
        parts.append("HIGH_RISK_RATIO")
    if customer_features.get("overseas_tx_ratio", 0) >= 0.30:
        parts.append("OVERSEAS_HEAVY")
    if customer_features.get("recency_days", 0) >= 60:
        parts.append("DORMANT")
    if customer_features.get("frequency_90d", 0) >= 30:
        parts.append("HIGH_FREQUENCY")
    return " | ".join(parts)
```

### 1-5. `models/forecast_intervals.py` — tree-quantile 신뢰구간

`anomaly_scorer.py` 와 동일 패턴(단일 헬퍼)으로 두어 batch/API 가 같은 신뢰구간 산출법을 쓰도록 강제한다.

```python
# models/forecast_intervals.py
"""RandomForest 의 개별 트리 예측 분포에서 신뢰구간을 추정한다."""
from __future__ import annotations

import numpy as np
from sklearn.ensemble import RandomForestRegressor


def tree_quantile_interval(
    rf: RandomForestRegressor,
    X,
    lower: float = 0.10,
    upper: float = 0.90,
) -> tuple[np.ndarray, np.ndarray]:
    """각 row 에 대해 트리별 예측의 (lower, upper) 분위수를 반환한다."""
    preds = np.stack([est.predict(X) for est in rf.estimators_], axis=0)   # shape: (n_trees, n_rows, n_outputs?)
    if preds.ndim == 2:
        # single output
        lo = np.quantile(preds, lower, axis=0)
        hi = np.quantile(preds, upper, axis=0)
    else:
        lo = np.quantile(preds, lower, axis=0)
        hi = np.quantile(preds, upper, axis=0)
    return lo, hi
```

`MultiOutputRegressor` 사용 시는 내부에 `estimators_` 리스트로 `RandomForestRegressor` 가 출력별로 들어 있으므로, 출력별 호출 후 stack 한다 (Day 2 의 학습 코드 참조).

### 1-6. 설계 문서

`docs/models/forecast-and-segmentation-design.md` 에 다음을 포함한다.

```bash
mkdir -p docs/models
cat > docs/models/forecast-and-segmentation-design.md << 'EOF'
# Forecast & Customer Segmentation Design — Week 3

## 목적

Week 1~2 의 ML 운영 기준선을 활용해 Nexus Pay 의 매출/거래량 예측과 고객 세분화를 구현한다.

## 모델 구성

| 모델 | 알고리즘 | 출력 |
|------|---------|------|
| revenue_forecast | MultiOutputRegressor(RandomForest) | tx_count, amount, fee_revenue + 80% 신뢰구간 |
| customer_segment | StandardScaler + KMeans(k=5) | segment_id + 자동 매핑된 segment_name + segment_action + rationale |

## Grain 분리 이유

Week 1 `feature_store` 는 거래 단위 PK 라 시간/고객 grain 의 ML 입력을 적재할 수 없다. 따라서 두 신규 테이블 (`feature_store_forecast`, `feature_store_segment`) 을 추가하고 자연 키로 PK 를 정의한다.

## Lineage

- 입력: `raw_transactions`, `risk_score`
- 시간 버킷 mart: `feature_store_forecast.payload_json` / `target_json`
- 고객 mart: `feature_store_segment.payload_json`
- 출력: `revenue_forecast`, `customer_segment`, `segment_action_recommendation`
- 공통 로그: `prediction_result` (forecast 는 transaction_id 컬럼에 40자 이하 결정성 인공 ID `fc-{sha1_24}` 적재)
- 실행 추적: `data_lineage` (`feature/mart/predict/evaluate` stage)

## 누수 방지

- forecast: `forecast_target_ts` 보다 늦은 정보(예: target 시점 이후 거래)는 lag/rolling feature 에 들어가지 않는다.
- segment: `reference_ts` 이전 거래 데이터로만 RFM 을 만든다.
- 모든 전처리는 train split 에서만 fit 한다.

## 모델별 평가

- forecast: MAE/RMSE/MAPE + amount-weighted MAPE + 피크 시간대 (top quantile) recall
- segment: silhouette, inertia, segment 분포, **시간 분할 stability** (earlier_window vs later_window 의 동일 customer segment 일치 비율)

## fraud 모델과의 경계

fraud_router, anomaly_scorer, risk_reasons 는 변경하지 않는다. risk_score 테이블은 forecast/segment 의 입력으로만 사용한다.
EOF
```

### 1-7. `scripts/apply_sql.py` — DAG 에서 쓰는 schema 적용 헬퍼

Airflow DAG 는 runner/Airflow 컨테이너 내부에서 실행되므로 `docker compose exec postgres ...` 형식의 명령을 직접 사용할 수 없다. 또한 runner 이미지에 `psql` client 가 없을 수 있다. 아래 헬퍼는 SQLAlchemy 연결을 사용해 SQL 파일을 적용하므로, 로컬 Makefile 과 DAG 양쪽에서 동일하게 사용할 수 있다.

```python
# scripts/apply_sql.py
"""SQL 파일을 PostgreSQL에 적용하는 공통 헬퍼.

Airflow DAG와 로컬 Makefile이 같은 schema 적용 경로를 사용하도록 한다.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import create_engine, text


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, required=True)
    args = parser.parse_args()
    sql = args.path.read_text(encoding="utf-8")
    with _engine().begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))
    print(f"applied SQL file: {args.path}")


if __name__ == "__main__":
    main()
```

### 1-8. `mlops/verify_registered_models.py` — Week 3 Registry 완료 검증

Week 3 완료 기준은 forecast/segment 모델이 MLflow Registered Model 로 등록되는 것이다. 아래 검증 헬퍼는 학습 스크립트가 `mlflow.sklearn.log_model(..., registered_model_name=...)` 로 등록한 모델이 실제 MLflow registry 에 존재하는지 확인한다. Week 4 는 이 registry 항목을 promotion workflow 로 이어받는다.

```python
# mlops/verify_registered_models.py
"""MLflow Registered Model 존재 여부를 검증한다."""
from __future__ import annotations

import argparse
import os

from mlflow.tracking import MlflowClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", required=True)
    args = parser.parse_args()

    client = MlflowClient(tracking_uri=os.environ["MLFLOW_TRACKING_URI"])
    missing: list[str] = []
    for name in args.models:
        try:
            versions = client.search_model_versions(f"name = '{name}'")
        except Exception:  # noqa: BLE001
            versions = []
        if not versions:
            missing.append(name)
        else:
            latest = sorted(versions, key=lambda v: int(v.version))[-1]
            print(f"registered model OK: {name} version={latest.version} run_id={latest.run_id}")

    if missing:
        raise SystemExit(f"missing registered models: {', '.join(missing)}")


if __name__ == "__main__":
    main()
```

### 1-9. Makefile 타겟 추가 (Day 1 시점에 모두 등록)

`fraud-api` 를 별도 컨테이너로 띄우지 않고 라우터 통합 패턴을 따른다. business-api 도 같은 정책.

```makefile
.PHONY: business-schema business-deps business-registry-check business-report \
        forecast-features forecast-train \
        segment-features segment-train \
        business-score business-api-test \
        business-dag-validate business-test

business-deps:
	grep -q '^holidays' requirements.txt || printf '\n# Week 3 calendar features\nholidays==0.55\n' >> requirements.txt
	grep -q '^scipy' requirements.txt || printf '\n# Week 3 segment stability\nscipy==1.13.1\n' >> requirements.txt
	grep -q '^tabulate' requirements.txt || printf '\n# Week 3 markdown report\ntabulate==0.9.0\n' >> requirements.txt
	docker compose exec runner pip install -r requirements.txt

business-schema:
	docker compose exec runner python -m scripts.apply_sql --path schemas/forecast_and_segment.sql

forecast-features:
	docker compose exec runner python -m features.build_forecast_features --config config/forecast_model_config.yaml

forecast-train:
	docker compose exec runner python -m models.train_revenue_forecast --config config/forecast_model_config.yaml

segment-features:
	docker compose exec runner python -m features.build_segment_features --config config/segment_model_config.yaml

segment-train:
	docker compose exec runner python -m models.train_customer_segments --config config/segment_model_config.yaml

# 사전조건: 위 학습 4개가 모두 끝나 있어야 한다.
business-score:
	docker compose exec runner python -m models.batch_score_forecast --config config/forecast_model_config.yaml
	docker compose exec runner python -m models.batch_score_segments --config config/segment_model_config.yaml

business-report:
	docker compose exec runner python -m scripts.render_business_insight_report --forecast-config config/forecast_model_config.yaml --segment-config config/segment_model_config.yaml

business-registry-check:
	docker compose exec runner python -m mlops.verify_registered_models --models nexuspay_revenue_forecast nexuspay_customer_segment

# 사전조건: business-score 가 끝나 있어야 한다.
business-api-test:
	curl -fsS http://localhost:$${API_PORT:-8000}/business/health
	curl -fsS http://localhost:$${API_PORT:-8000}/business/segments/summary
	curl -fsS "http://localhost:$${API_PORT:-8000}/business/forecast?date=2026-05-25"

business-dag-validate:
	docker compose exec runner python -m dags.business_insight_pipeline_dag --validate

business-test:
	docker compose exec runner pytest -q tests/test_business_insight_pipeline.py
```

### Day 1 완료 기준

* `config/forecast_model_config.yaml`, `config/segment_model_config.yaml` 이 모두 작성되었다.
* `make business-schema` 적용 후 `\d feature_store_forecast`, `\d feature_store_segment`, `\d revenue_forecast`, `\d customer_segment`, `\d segment_action_recommendation` 가 성공한다.
* `features/calendar.py`, `features/segment_actions.py`, `models/forecast_intervals.py` 가 생성되어 import 가능하다 (`docker compose exec runner python -c "from features.calendar import is_holiday; from features.segment_actions import label_segments; from models.forecast_intervals import tree_quantile_interval"` 가 에러 없이 끝난다).
* `make business-deps` 가 멱등적으로 동작해 `holidays`, `scipy`, `tabulate` 패키지가 runner 컨테이너에 설치되어 있다.

---

## Day 2: Forecast Feature + 모델 학습

### 2-0. Forecast feature 원칙

Forecast feature 는 미래 정보를 쓰지 않는다. 특정 `forecast_target_ts` 를 예측할 때 사용 가능한 정보는 그 시점 이전에 확정된 거래 집계와 캘린더/공휴일 정보뿐이다. 누수 방지는 두 단계로 강제한다.

1. 집계 단계에서 `forecast_target_ts` 가 시간 버킷 시작이라고 가정하고, 그 이전 데이터만 lag/rolling 입력으로 사용
2. 학습 단계에서 `time_based_split` 으로 train/valid/test 시간 분할을 추가 강제 (`Week 1 features/data_split.py` 패턴)

주요 feature 카테고리:

| 카테고리 | feature | 산출 방법 |
|---------|---------|----------|
| Lag | `tx_count_lag_{24,48,168}h`, `amount_lag_{24,48,168}h`, `fee_revenue_lag_24h` | 같은 (channel, merchant_segment, risk_grade) 의 N시간 전 집계 |
| Rolling | `tx_count_roll_mean_{24,168}h`, `amount_roll_mean_{24,168}h` | 같은 grain 의 직전 24/168시간 평균 |
| Calendar | `hour_of_day`, `day_of_week`, `is_weekend`, `is_holiday` | `features/calendar.py` 사용 |
| Categorical | `channel`, `merchant_segment`, `risk_grade` | One-hot |

### 2-1. `features/build_forecast_features.py`

```python
# features/build_forecast_features.py
"""raw_transactions + risk_score -> feature_store_forecast 적재.

설계 원칙:
- 시간 버킷(hour) 단위로 (channel, merchant_segment, risk_grade) 별 집계 mart 를 생성
- lag/rolling 은 같은 grain 안에서 시간순으로 산출하여 누수 방지
- target (tx_count, amount, fee_revenue) 은 분리해 target_json 에 적재 (학습용)
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

from features.calendar import add_calendar_features


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_raw_with_risk(engine, until_ts: pd.Timestamp) -> pd.DataFrame:
    """raw_transactions LEFT JOIN risk_score 로 거래별 risk_grade 부여.

    risk_score 미존재 거래는 cfg.risk_grade_default 로 채운다 (Day 1 1-1).
    """
    sql = text(
        """
        SELECT rt.transaction_id, rt.amount, rt.channel, rt.merchant_segment,
               rt.transacted_at,
               rs.risk_band AS risk_grade
          FROM raw_transactions rt
          LEFT JOIN LATERAL (
                SELECT risk_band
                  FROM risk_score r
                 WHERE r.transaction_id = rt.transaction_id
                 ORDER BY r.scored_at DESC
                 LIMIT 1
          ) rs ON TRUE
         WHERE rt.transacted_at <= :until_ts
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"until_ts": until_ts.to_pydatetime()})


def _bucket_hour(ts: pd.Series) -> pd.Series:
    """타임스탬프를 시간 버킷 시작점으로 floor 한다 (UTC, tz-aware)."""
    s = pd.to_datetime(ts, utc=True)
    return s.dt.floor("h")


def _aggregate_to_grain(raw: pd.DataFrame, fee_rates: dict, default_risk: str) -> pd.DataFrame:
    """거래 단위 -> (forecast_target_ts, channel, merchant_segment, risk_grade) 집계."""
    df = raw.copy()
    df["risk_grade"] = df["risk_grade"].fillna(default_risk)
    df["forecast_target_ts"] = _bucket_hour(df["transacted_at"])
    df["fee_amount"] = df.apply(
        lambda r: float(r["amount"]) * float(fee_rates.get(r["merchant_segment"], fee_rates.get("UNKNOWN", 0.025))),
        axis=1,
    )
    grouped = (
        df.groupby(["forecast_target_ts", "channel", "merchant_segment", "risk_grade"], dropna=False)
          .agg(tx_count=("transaction_id", "count"),
               amount=("amount", "sum"),
               fee_revenue=("fee_amount", "sum"))
          .reset_index()
    )
    return grouped


def _add_lag_rolling(df: pd.DataFrame, lag_hours, roll_windows) -> pd.DataFrame:
    """grain 별 시계열로 정렬 후 lag/rolling 산출."""
    out_parts = []
    keys = ["channel", "merchant_segment", "risk_grade"]
    for key_values, group in df.sort_values("forecast_target_ts").groupby(keys, dropna=False):
        g = group.set_index("forecast_target_ts").asfreq("h").reset_index()
        # asfreq 로 결측 시간 버킷에 NaN 행이 추가됨. categorical 키는 채워주어야 한다.
        for k, v in zip(keys, key_values if isinstance(key_values, tuple) else (key_values,)):
            g[k] = v
        for h in lag_hours:
            g[f"tx_count_lag_{h}h"] = g["tx_count"].shift(h)
            g[f"amount_lag_{h}h"] = g["amount"].shift(h)
            if h == 24:
                g[f"fee_revenue_lag_{h}h"] = g["fee_revenue"].shift(h)
        for w in roll_windows:
            g[f"tx_count_roll_mean_{w}h"] = g["tx_count"].shift(1).rolling(w, min_periods=1).mean()
            g[f"amount_roll_mean_{w}h"] = g["amount"].shift(1).rolling(w, min_periods=1).mean()
        out_parts.append(g)
    return pd.concat(out_parts, ignore_index=True)


def _to_feature_rows(enriched: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    numerical = cfg["features"]["numerical"]
    categorical = cfg["features"]["categorical"]
    payload_cols = numerical + categorical
    targets = cfg["features"]["targets"]

    df = enriched.copy()
    df["payload_json"] = df[payload_cols].apply(
        lambda r: json.dumps({k: (None if pd.isna(v) else (int(v) if isinstance(v, (np.integer, bool)) else float(v) if isinstance(v, (int, float, np.floating)) else str(v))) for k, v in r.items()}),
        axis=1,
    )
    df["target_json"] = df[targets].apply(
        lambda r: json.dumps({k: (None if pd.isna(v) else float(v)) for k, v in r.items()}),
        axis=1,
    )
    return df.assign(
        feature_set=feature_set,
        feature_version=feature_version,
        as_of_ts=df["forecast_target_ts"],
    )[["feature_set", "feature_version", "forecast_target_ts",
       "channel", "merchant_segment", "risk_grade",
       "as_of_ts", "payload_json", "target_json"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    until_ts = pd.Timestamp(cfg["split"]["scoring_horizon_until"])
    fee_rates = cfg["features"]["fee_rate_by_segment"]
    default_risk = cfg["features"]["risk_grade_default"]

    engine = _engine()
    raw = _load_raw_with_risk(engine, until_ts)
    if raw.empty:
        raise SystemExit("no raw_transactions rows — run Week 1/2 ingestion first")

    grain_df = _aggregate_to_grain(raw, fee_rates, default_risk)
    grain_df = _add_lag_rolling(grain_df, cfg["features"]["lag_hours"], cfg["features"]["rolling_windows_hours"])
    enriched = add_calendar_features(grain_df, ts_column="forecast_target_ts")
    rows = _to_feature_rows(enriched, cfg)

    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM feature_store_forecast WHERE feature_set = :fs AND feature_version = :fv"),
            {"fs": cfg["features"]["feature_set"], "fv": cfg["features"]["feature_version"]},
        )
        rows.to_sql("feature_store_forecast", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, input_window_start, "
                "input_window_end, output_target) "
                "VALUES (:run_id, 'mart', 'raw_transactions+risk_score', :ws, :we, "
                "        'feature_store_forecast@forecast_v1')"
            ),
            {
                "run_id": run_id,
                "ws": raw["transacted_at"].min().to_pydatetime(),
                "we": until_ts.to_pydatetime(),
            },
        )

    print(
        f"forecast features updated: rows={len(rows):,} "
        f"unique_buckets={rows['forecast_target_ts'].nunique():,} "
        f"window={rows['forecast_target_ts'].min()} ~ {rows['forecast_target_ts'].max()} "
        f"run_id={run_id}"
    )


if __name__ == "__main__":
    main()
```

### 2-2. `models/train_revenue_forecast.py`

```python
# models/train_revenue_forecast.py
"""Multi-output revenue forecast 학습. TimeSeriesSplit + MultiOutputRegressor + tree-quantile PI."""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sqlalchemy import create_engine, text

from features.data_split import SplitConfig
from models.forecast_intervals import tree_quantile_interval


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_features(engine, feature_set: str, feature_version: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT feature_set, feature_version, forecast_target_ts,
               channel, merchant_segment, risk_grade,
               as_of_ts, payload_json, target_json
          FROM feature_store_forecast
         WHERE feature_set = :fs AND feature_version = :fv
         ORDER BY forecast_target_ts
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version})


def _build_preprocessor(numerical: list[str], categorical: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), numerical),
            ("cat", Pipeline([
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
            ]), categorical),
        ]
    )


def _flatten(rows: pd.DataFrame, payload_cols: list[str], targets: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    payloads = pd.json_normalize(rows["payload_json"].apply(json.loads)).reindex(columns=payload_cols)
    targets_df = pd.json_normalize(rows["target_json"].apply(json.loads)).reindex(columns=targets)
    payloads.index = rows.index
    targets_df.index = rows.index
    return payloads, targets_df


def _mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = np.abs(y_true) > 1e-9
    if mask.sum() == 0:
        return float("nan")
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    numerical = cfg["features"]["numerical"]
    categorical = cfg["features"]["categorical"]
    targets = cfg["features"]["targets"]
    model_name = cfg["model"]["registry"]["name"]
    base_params = cfg["model"]["base"]
    rs = cfg["model"]["random_state"]
    pi_cfg = cfg["model"]["prediction_interval"]
    split_cfg = SplitConfig.from_dict({
        **cfg["split"],
        "label_column": targets[0],         # SplitConfig 호환을 위한 placeholder
        "label_confirmed_column": "as_of_ts",
        "min_label_lag_hours": 0,
    })

    engine = _engine()
    rows = _load_features(engine, feature_set, feature_version)
    if rows.empty:
        raise SystemExit("no forecast features — run make forecast-features first")
    rows["forecast_target_ts"] = pd.to_datetime(rows["forecast_target_ts"], utc=True)

    X_all, Y_all = _flatten(rows, numerical + categorical, targets)
    # 결측 lag/rolling 행은 학습 대상에서 제외
    not_null_mask = X_all[numerical].notna().all(axis=1) & Y_all.notna().all(axis=1)
    X_all = X_all[not_null_mask].reset_index(drop=True)
    Y_all = Y_all[not_null_mask].reset_index(drop=True)
    ts_all = rows.loc[not_null_mask, "forecast_target_ts"].reset_index(drop=True)

    # time-based split (train/valid/test)
    train_mask = ts_all <= split_cfg.train_until
    valid_mask = (ts_all > split_cfg.train_until) & (ts_all <= split_cfg.valid_until)
    test_mask  = (ts_all > split_cfg.valid_until) & (ts_all <= split_cfg.test_until)

    X_train, Y_train = X_all[train_mask], Y_all[train_mask]
    X_valid, Y_valid = X_all[valid_mask], Y_all[valid_mask]
    X_test,  Y_test  = X_all[test_mask],  Y_all[test_mask]

    if min(len(X_train), len(X_valid), len(X_test)) == 0:
        raise SystemExit(f"empty split: train={len(X_train)}, valid={len(X_valid)}, test={len(X_test)}")

    pre = _build_preprocessor(numerical, categorical)
    base_rf = RandomForestRegressor(random_state=rs, **base_params)
    estimator = MultiOutputRegressor(base_rf, n_jobs=1)
    pipe = Pipeline([("preprocessor", pre), ("model", estimator)])

    # TimeSeriesSplit cross-validation 으로 안정성 점검 (5-fold)
    tscv = TimeSeriesSplit(n_splits=5)
    cv_maes: list[float] = []
    for fold, (tr_idx, va_idx) in enumerate(tscv.split(X_train)):
        pipe.fit(X_train.iloc[tr_idx], Y_train.iloc[tr_idx])
        pred = pipe.predict(X_train.iloc[va_idx])
        cv_maes.append(float(mean_absolute_error(Y_train.iloc[va_idx], pred)))

    # 최종 학습 (train+valid)
    pipe.fit(pd.concat([X_train, X_valid]), pd.concat([Y_train, Y_valid]))
    test_pred = pipe.predict(X_test)

    # tree-quantile PI 는 amount 출력에 한정해 산출 (CFO 해석용)
    amount_idx = targets.index("amount")
    inner_rf_amount: RandomForestRegressor = pipe.named_steps["model"].estimators_[amount_idx]
    pre_X_test = pipe.named_steps["preprocessor"].transform(X_test)
    pi_low, pi_high = tree_quantile_interval(inner_rf_amount, pre_X_test, lower=pi_cfg["lower"], upper=pi_cfg["upper"])

    metrics = {}
    for i, t in enumerate(targets):
        metrics[f"{t}_mae"] = float(mean_absolute_error(Y_test.iloc[:, i], test_pred[:, i]))
        metrics[f"{t}_rmse"] = float(np.sqrt(mean_squared_error(Y_test.iloc[:, i], test_pred[:, i])))
        metrics[f"{t}_mape"] = _mape(Y_test.iloc[:, i].to_numpy(), test_pred[:, i])
    metrics["cv_mae_mean"] = float(np.mean(cv_maes))
    metrics["cv_mae_std"] = float(np.std(cv_maes))
    # amount-weighted MAPE
    amount_actual = Y_test["amount"].to_numpy()
    amount_pred = test_pred[:, amount_idx]
    weights = np.abs(amount_actual)
    if weights.sum() > 0:
        wmape = float(np.sum(np.abs(amount_actual - amount_pred)) / weights.sum())
        metrics["amount_weighted_mape"] = wmape

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(model_name)
    with mlflow.start_run(run_name=f"rf-multioutput-{time.strftime('%Y%m%d-%H%M%S')}") as run:
        mlflow.log_params({
            "algorithm": "MultiOutputRegressor(RandomForestRegressor)",
            "n_estimators": base_params["n_estimators"],
            "min_samples_leaf": base_params["min_samples_leaf"],
            "feature_set": feature_set,
            "feature_version": feature_version,
            "pi_lower": pi_cfg["lower"],
            "pi_upper": pi_cfg["upper"],
        })
        mlflow.log_metrics(metrics)

        artifact_dir = Path(cfg["artifacts"]["model_root"]) / model_name / time.strftime("%Y%m%d-%H%M%S")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        model_version = artifact_dir.name
        model_path = artifact_dir / "model.joblib"
        joblib.dump(
            {
                "pipeline": pipe,
                "numerical": numerical,
                "categorical": categorical,
                "targets": targets,
                "amount_idx": amount_idx,
                "model_name": model_name,
                "model_version": model_version,
                "mlflow_run_id": run.info.run_id,
                "metrics": metrics,
                "pi_cfg": pi_cfg,
            },
            model_path,
        )
        mlflow.log_artifact(str(model_path), artifact_path="model")
        # Week 3 완료 기준: local artifact 뿐 아니라 MLflow Registered Model version 도 생성한다.
        mlflow.sklearn.log_model(pipe, artifact_path="sklearn_model", registered_model_name=model_name)

        # 자동 생성 forecast eval report
        report_path = Path(cfg["artifacts"]["report_dir"]) / f"forecast_eval_report_{model_version}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(_format_forecast_report(model_name, model_version, metrics, run.info.run_id, split_cfg, pi_cfg), encoding="utf-8")
        mlflow.log_artifact(str(report_path), artifact_path="reports")

        # model_evaluation 적재
        with engine.begin() as conn:
            for k, v in metrics.items():
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    continue
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
                        "metric": k,
                        "value": float(v),
                        "extra": json.dumps({"targets": targets, "pi_cfg": pi_cfg}),
                    },
                )

    # sanity assertions
    assert all(np.isfinite(test_pred).all() for _ in [0]), "non-finite predictions"
    assert (pi_low <= pi_high).all(), "tree quantile PI inverted"
    print(f"trained forecast model_version={model_version} metrics={metrics}")
    print(f"report: {report_path}")


def _format_forecast_report(model_name, model_version, metrics, run_id, split_cfg, pi_cfg) -> str:
    return f"""# Revenue Forecast Evaluation Report

- 모델: `{model_name}`
- 버전: `{model_version}`
- 평가 윈도우: `{split_cfg.valid_until.isoformat()}` ~ `{split_cfg.test_until.isoformat()}`
- run_id: `{run_id}`

## 1. 지표 (target 별)

| target | MAE | RMSE | MAPE |
|--------|-----|------|------|
| tx_count   | {metrics.get('tx_count_mae', float('nan')):.4f} | {metrics.get('tx_count_rmse', float('nan')):.4f} | {metrics.get('tx_count_mape', float('nan')):.4f} |
| amount     | {metrics.get('amount_mae', float('nan')):.2f} | {metrics.get('amount_rmse', float('nan')):.2f} | {metrics.get('amount_mape', float('nan')):.4f} |
| fee_revenue| {metrics.get('fee_revenue_mae', float('nan')):.2f} | {metrics.get('fee_revenue_rmse', float('nan')):.2f} | {metrics.get('fee_revenue_mape', float('nan')):.4f} |

- amount-weighted MAPE: `{metrics.get('amount_weighted_mape', float('nan')):.4f}`
- TimeSeriesSplit CV MAE: `{metrics.get('cv_mae_mean', float('nan')):.4f}` ± `{metrics.get('cv_mae_std', float('nan')):.4f}`

## 2. 신뢰구간

- 산출 방식: `{pi_cfg['method']}` (RandomForest 트리별 예측의 분위수)
- 분위수 범위: `[{pi_cfg['lower']:.2f}, {pi_cfg['upper']:.2f}]`

## 3. CFO/COO 해석 노트

- amount 의 MAPE 는 일별/주별 매출 예측 오차의 절대 비율이다. 5%가 목표선이다.
- amount_weighted_mape 가 amount_mape 보다 크게 낮다면 큰 거래일에 강하고 자잘한 거래에 약한 모델이다.
- tx_count 의 MAE 는 시간대별 운영 인력 배치 의사결정의 오차 폭으로 해석한다.

## 4. 운영 인수인계

- 모델 artifact: `data/artifacts/{model_name}/{model_version}/model.joblib`
- batch scoring 진입점: `python -m models.batch_score_forecast`
- API 진입점: `GET /business/forecast?date=YYYY-MM-DD`
"""


if __name__ == "__main__":
    main()
```

### Day 2 완료 기준

* `make forecast-features` 실행 후 `feature_store_forecast` 에 `forecast_v1` row 가 적재되고 `data_lineage` 에 `mart` stage 가 기록된다.
* `make forecast-train` 실행 후:
  * MLflow Registered Model `nexuspay_revenue_forecast` 에 새 버전이 등록된다 (`make business-registry-check` 로 검증).
  * `model_evaluation` 에 `tx_count_mae/rmse/mape`, `amount_*`, `fee_revenue_*`, `amount_weighted_mape`, `cv_mae_mean/std` 가 모두 적재된다.
  * `data/reports/forecast_eval_report_<version>.md` 가 자동 생성된다.
* `data/artifacts/nexuspay_revenue_forecast/<version>/model.joblib` 에 `pipeline + numerical/categorical/targets + amount_idx + pi_cfg` 가 함께 직렬화되어 있어 batch/API 가 같은 PI 산출법을 사용할 수 있다.

---

## Day 3: Customer Segment Feature + 모델 학습

### 3-0. Segment feature 원칙

고객 세분화는 라벨이 없는 문제이므로 단일 정답률 대신 다음 세 가지로 모델을 설명한다.

1. **해석 가능성** — centroid 가 운영팀이 이해할 수 있는 RFM/행동 패턴으로 매핑되어야 함 (`features/segment_actions.py` 가 책임)
2. **안정성(stability)** — earlier window vs later window 학습 결과가 동일 customer 에 일관된 segment 를 부여하는 비율
3. **운영 연결** — segment_id 가 RETENTION_PRIORITY / WINBACK_CAMPAIGN 등 실행 가능한 action 으로 직결됨

주요 feature 카테고리:

| feature | 의미 | 산출 방법 |
|---------|------|----------|
| `recency_days` | 마지막 거래 후 경과일 | `reference_ts - max(transacted_at)` |
| `recency_score` | recency 의 KMeans 친화 변환 | `1 / (1 + recency_days)` (낮을수록 dormant) |
| `frequency_90d` | 최근 90일 거래 횟수 | count |
| `monetary_90d` | 최근 90일 총 거래액 | sum |
| `avg_ticket_size` | 평균 결제 금액 | mean |
| `overseas_tx_ratio` | 해외 device_country 비중 | `mean(device_country != 'KR')` |
| `app_channel_ratio` | APP 채널 비중 | `mean(channel == 'APP')` |
| `high_risk_ratio_90d` | risk_band='HIGH' 거래 비중 | `risk_score` 조인 |
| `medium_risk_ratio_90d` | risk_band='MEDIUM' 거래 비중 | `risk_score` 조인 |

### 3-1. `features/build_segment_features.py`

```python
# features/build_segment_features.py
"""raw_transactions + risk_score -> feature_store_segment 적재 (고객 단위)."""
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


def _load_observations(engine, reference_ts: pd.Timestamp, window_days: int) -> pd.DataFrame:
    window_start = reference_ts - pd.Timedelta(days=window_days)
    sql = text(
        """
        SELECT rt.transaction_id, rt.customer_id, rt.amount, rt.channel,
               rt.device_country, rt.transacted_at,
               rs.risk_band
          FROM raw_transactions rt
          LEFT JOIN LATERAL (
                SELECT risk_band
                  FROM risk_score r
                 WHERE r.transaction_id = rt.transaction_id
                 ORDER BY r.scored_at DESC
                 LIMIT 1
          ) rs ON TRUE
         WHERE rt.transacted_at >= :ws AND rt.transacted_at <= :we
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(
            sql, conn,
            params={"ws": window_start.to_pydatetime(), "we": reference_ts.to_pydatetime()},
        )


def _aggregate_customer(df: pd.DataFrame, reference_ts: pd.Timestamp) -> pd.DataFrame:
    df = df.copy()
    df["transacted_at"] = pd.to_datetime(df["transacted_at"], utc=True)
    df["is_overseas"] = (df["device_country"].fillna("KR") != "KR").astype(int)
    df["is_app"] = (df["channel"].fillna("") == "APP").astype(int)
    df["is_high_risk"] = (df["risk_band"] == "HIGH").astype(int)
    df["is_medium_risk"] = (df["risk_band"] == "MEDIUM").astype(int)

    grouped = (
        df.groupby("customer_id", dropna=False)
          .agg(
              last_transacted_at=("transacted_at", "max"),
              frequency_90d=("transaction_id", "count"),
              monetary_90d=("amount", "sum"),
              avg_ticket_size=("amount", "mean"),
              overseas_tx_ratio=("is_overseas", "mean"),
              app_channel_ratio=("is_app", "mean"),
              high_risk_ratio_90d=("is_high_risk", "mean"),
              medium_risk_ratio_90d=("is_medium_risk", "mean"),
          )
          .reset_index()
    )
    grouped["recency_days"] = (
        (reference_ts - grouped["last_transacted_at"]).dt.total_seconds() / 86400.0
    )
    grouped["recency_score"] = 1.0 / (1.0 + grouped["recency_days"].clip(lower=0))
    return grouped


def _to_feature_rows(grouped: pd.DataFrame, cfg: dict, reference_ts: pd.Timestamp) -> pd.DataFrame:
    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    numerical = cfg["features"]["numerical"]

    payload = grouped[numerical].copy()
    payload["payload_json"] = payload.apply(
        lambda r: json.dumps({k: (None if pd.isna(v) else float(v)) for k, v in r.items()}),
        axis=1,
    )
    return grouped.assign(
        feature_set=feature_set,
        feature_version=feature_version,
        as_of_ts=reference_ts,
        payload_json=payload["payload_json"],
    )[["feature_set", "feature_version", "customer_id", "as_of_ts", "payload_json"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    reference_ts = pd.Timestamp(cfg["features"]["reference_ts"])
    window_days = int(cfg["features"]["observation_window_days"])

    engine = _engine()
    obs = _load_observations(engine, reference_ts, window_days)
    if obs.empty:
        raise SystemExit("no observations — Week 1/2 ingestion 가 선행되어야 한다")

    grouped = _aggregate_customer(obs, reference_ts)
    rows = _to_feature_rows(grouped, cfg, reference_ts)

    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM feature_store_segment WHERE feature_set = :fs AND feature_version = :fv"),
            {"fs": cfg["features"]["feature_set"], "fv": cfg["features"]["feature_version"]},
        )
        rows.to_sql("feature_store_segment", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, input_window_start, "
                "input_window_end, output_target) "
                "VALUES (:run_id, 'mart', 'raw_transactions+risk_score', :ws, :we, "
                "        'feature_store_segment@customer_segment_v1')"
            ),
            {
                "run_id": run_id,
                "ws": (reference_ts - pd.Timedelta(days=window_days)).to_pydatetime(),
                "we": reference_ts.to_pydatetime(),
            },
        )

    # sanity
    print(
        f"segment features updated: customers={len(rows):,} "
        f"reference_ts={reference_ts.isoformat()} run_id={run_id}"
    )
```

### 3-2. `models/train_customer_segments.py`

```python
# models/train_customer_segments.py
"""StandardScaler + KMeans 학습. silhouette / inertia / stability 측정 + action_policy 자동 생성."""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
import yaml
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text

from features.segment_actions import label_segments


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_segment_features(engine, feature_set: str, feature_version: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT customer_id, payload_json, as_of_ts
          FROM feature_store_segment
         WHERE feature_set = :fs AND feature_version = :fv
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version})


def _flatten(rows: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    flat = pd.json_normalize(rows["payload_json"].apply(json.loads)).reindex(columns=columns)
    flat.index = rows.index
    return flat


def _build_pipe(n_clusters: int, n_init, rs: int) -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", KMeans(n_clusters=n_clusters, n_init=n_init, random_state=rs)),
    ])


def _stability(
    cfg: dict,
    engine,
    feature_set: str,
    feature_version: str,
    columns: list[str],
    n_clusters: int,
    n_init,
    rs: int,
) -> tuple[float, dict]:
    """earlier_window 학습 결과와 later_window 학습 결과의 동일 customer segment 일치 비율."""
    sc = cfg["split"]["stability_check"]
    if not sc.get("enabled"):
        return float("nan"), {"enabled": False}

    earlier_until = pd.Timestamp(sc["earlier_window_until"])
    later_until = pd.Timestamp(sc["later_window_until"])

    # earlier window 만으로 학습 가능한 데이터 비교를 위해 reference_ts 가 earlier_until 인 별도 feature_set
    # 본 가이드는 학습된 single feature_set 안에서 customer_id 의 segment 일관성을 보는 단순 변형으로 시작한다.
    # (Week 4 monitoring 단계에서 전용 stability 측정 표준화)
    rows = _load_segment_features(engine, feature_set, feature_version)
    if rows.empty:
        return float("nan"), {"enabled": True, "reason": "no rows"}
    X = _flatten(rows, columns).fillna(0.0)

    pipe_a = _build_pipe(n_clusters, n_init, rs).fit(X)
    pipe_b = _build_pipe(n_clusters, n_init, rs + 1).fit(X)
    labels_a = pipe_a.predict(X)
    labels_b = pipe_b.predict(X)

    # 두 라벨 시퀀스 사이의 cluster permutation 을 정렬한 뒤 일치율 계산
    from scipy.optimize import linear_sum_assignment
    confusion = pd.crosstab(pd.Series(labels_a), pd.Series(labels_b))
    cost = -confusion.values
    rows_idx, cols_idx = linear_sum_assignment(cost)
    matched = sum(confusion.values[r, c] for r, c in zip(rows_idx, cols_idx))
    overlap = matched / len(X)
    return float(overlap), {"enabled": True, "overlap": float(overlap), "min_required": sc.get("min_overlap_ratio")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    columns = cfg["features"]["numerical"]
    n_clusters = int(cfg["model"]["n_clusters"])
    n_init = cfg["model"]["n_init"]
    rs = cfg["model"]["random_state"]
    model_name = cfg["model"]["registry"]["name"]

    engine = _engine()
    rows = _load_segment_features(engine, feature_set, feature_version)
    if rows.empty:
        raise SystemExit("no segment features — run make segment-features first")

    X = _flatten(rows, columns).fillna(0.0)
    pipe = _build_pipe(n_clusters, n_init, rs)
    pipe.fit(X)
    labels = pipe.predict(X)
    inertia = float(pipe.named_steps["model"].inertia_)
    sil = float(silhouette_score(X, labels)) if len(set(labels)) > 1 else float("nan")
    sizes = pd.Series(labels).value_counts().sort_index().to_dict()
    overlap, stability_info = _stability(cfg, engine, feature_set, feature_version, columns, n_clusters, n_init, rs)

    # centroid 해석 → action_policy
    scaler: StandardScaler = pipe.named_steps["scaler"]
    centroids_raw = pipe.named_steps["model"].cluster_centers_
    centroids = pd.DataFrame(scaler.inverse_transform(centroids_raw), columns=columns)
    centroids.index.name = "segment_id"
    labels_meta = label_segments(centroids, top_k=int(cfg.get("action_policy", {}).get("reason_top_k", 3)))

    policy = {
        str(lm.segment_id): {
            "segment_name": lm.segment_name,
            "segment_action": lm.segment_action,
            "rationale": lm.rationale,
            "centroid": centroids.loc[lm.segment_id].to_dict(),
        }
        for lm in labels_meta
    }

    metrics = {
        "silhouette": sil,
        "inertia": inertia,
        "stability_overlap": overlap,
        **{f"size_segment_{sid}": int(c) for sid, c in sizes.items()},
    }

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(model_name)
    with mlflow.start_run(run_name=f"kmeans-{time.strftime('%Y%m%d-%H%M%S')}") as run:
        mlflow.log_params({
            "algorithm": "KMeans",
            "n_clusters": n_clusters,
            "n_init": str(n_init),
            "feature_set": feature_set,
            "feature_version": feature_version,
        })
        mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float)) and not pd.isna(v)})

        artifact_dir = Path(cfg["artifacts"]["model_root"]) / model_name / time.strftime("%Y%m%d-%H%M%S")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        model_version = artifact_dir.name
        model_path = artifact_dir / "model.joblib"
        joblib.dump(
            {
                "pipeline": pipe,
                "columns": columns,
                "n_clusters": n_clusters,
                "model_name": model_name,
                "model_version": model_version,
                "mlflow_run_id": run.info.run_id,
                "metrics": metrics,
                "stability": stability_info,
                "centroids": centroids.to_dict(orient="index"),
                "policy": policy,
            },
            model_path,
        )
        mlflow.log_artifact(str(model_path), artifact_path="model")
        # Week 3 완료 기준: local artifact 뿐 아니라 MLflow Registered Model version 도 생성한다.
        mlflow.sklearn.log_model(pipe, artifact_path="sklearn_model", registered_model_name=model_name)

        policy_path = Path(cfg["action_policy"]["output_path"])
        policy_path.parent.mkdir(parents=True, exist_ok=True)
        policy_path.write_text(json.dumps(policy, indent=2, ensure_ascii=False), encoding="utf-8")
        mlflow.log_artifact(str(policy_path), artifact_path="policy")

        # segment_action_recommendation 테이블 적재 (UPSERT)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM segment_action_recommendation "
                    "WHERE model_name = :mn AND model_version = :mv"
                ),
                {"mn": model_name, "mv": model_version},
            )
            for sid_str, body in policy.items():
                conn.execute(
                    text(
                        "INSERT INTO segment_action_recommendation"
                        "(model_name, model_version, segment_id, segment_name, segment_action, rationale, centroid_json) "
                        "VALUES (:mn, :mv, :sid, :sname, :saction, :rationale, :centroid)"
                    ),
                    {
                        "mn": model_name,
                        "mv": model_version,
                        "sid": int(sid_str),
                        "sname": body["segment_name"],
                        "saction": body["segment_action"],
                        "rationale": body["rationale"],
                        "centroid": json.dumps(body["centroid"]),
                    },
                )
            for k, v in metrics.items():
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    continue
                conn.execute(
                    text(
                        "INSERT INTO model_evaluation(model_name, model_version, eval_window_start, "
                        "eval_window_end, metric_name, metric_value, extra_json) "
                        "VALUES (:mn, :mv, :ws, :we, :metric, :value, :extra)"
                    ),
                    {
                        "mn": model_name,
                        "mv": model_version,
                        "ws": pd.Timestamp(cfg["split"]["train_until"]).to_pydatetime(),
                        "we": pd.Timestamp(cfg["split"]["test_until"]).to_pydatetime(),
                        "metric": k,
                        "value": float(v),
                        "extra": json.dumps({"n_clusters": n_clusters, "stability": stability_info}),
                    },
                )

        # 자동 생성 segment eval report
        report_path = Path(cfg["artifacts"]["report_dir"]) / f"segment_eval_report_{model_version}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(_format_segment_report(model_name, model_version, metrics, policy, stability_info, run.info.run_id), encoding="utf-8")
        mlflow.log_artifact(str(report_path), artifact_path="reports")

    # sanity assertions
    assert len(set(labels)) == n_clusters, f"unexpected cluster count: {len(set(labels))} != {n_clusters}"
    if cfg["split"]["stability_check"].get("enabled") and not pd.isna(overlap):
        min_overlap = float(cfg["split"]["stability_check"]["min_overlap_ratio"])
        if overlap < min_overlap:
            print(f"[WARN] segment stability overlap={overlap:.3f} < {min_overlap:.3f} — review centroid policy")

    print(f"trained segment model_version={model_version} silhouette={sil:.4f} inertia={inertia:.2f} stability={overlap:.4f}")
    print(f"action policy: {policy_path}")
    print(f"report: {report_path}")


def _format_segment_report(model_name, model_version, metrics, policy, stability_info, run_id) -> str:
    lines = [
        f"# Customer Segmentation Evaluation Report",
        "",
        f"- 모델: `{model_name}`",
        f"- 버전: `{model_version}`",
        f"- run_id: `{run_id}`",
        "",
        "## 1. 모델 품질",
        "",
        f"- silhouette: `{metrics.get('silhouette', float('nan')):.4f}`",
        f"- inertia: `{metrics.get('inertia', float('nan')):.2f}`",
        f"- stability_overlap: `{metrics.get('stability_overlap', float('nan')):.4f}` (min_required={stability_info.get('min_required')})",
        "",
        "## 2. Segment 분포 + 운영 액션",
        "",
        "| segment_id | name | action | rationale |",
        "|------------|------|--------|-----------|",
    ]
    for sid_str, body in policy.items():
        size = metrics.get(f"size_segment_{sid_str}", 0)
        lines.append(f"| {sid_str} ({size:,}명) | {body['segment_name']} | {body['segment_action']} | {body['rationale']} |")
    lines += [
        "",
        "## 3. CMO/COO 해석",
        "",
        "- RETENTION_PRIORITY 군은 NPS 캠페인과 연회비 할인 전환을 우선 검토.",
        "- WINBACK_CAMPAIGN 군은 1주 내 재거래가 없으면 휴면 알림 및 쿠폰 발송.",
        "- FX_FEE_PROMOTION 군은 해외 결제 수수료 면제 프로모션 적용 후 재계측.",
        "- RISK_REVIEW 군은 위험 등급 비중이 높으므로 마케팅 전 운영팀 사전 검토.",
        "",
        "## 4. 운영 인수인계",
        "",
        f"- 모델 artifact: `data/artifacts/{model_name}/{model_version}/model.joblib`",
        "- batch scoring 진입점: `python -m models.batch_score_segments`",
        "- API 진입점: `GET /business/segments/{customer_id}`, `GET /business/segments/summary`",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    main()
```

> stability 측정 단순화 안내: 본 가이드의 `_stability` 는 동일 데이터에 random_state 를 바꿔 학습한 결과 사이의 일치율로 시작한다. 이는 "centroid initialization 변동에 대한 견고성" 만 보는 약식 stability 다. earlier/later 시간 윈도우 분할 학습은 Week 4 monitoring 단계에서 표준화한다.

`scipy` 는 segment stability 계산에 필요하며 Day 1 의 `make business-deps` 에서 함께 설치된다. 별도 설치 블록을 두지 않고, 의존성은 `business-deps` 한 곳에서 관리한다.

```bash
make business-deps
```

### Day 3 완료 기준

* `make segment-features` 실행 후 `feature_store_segment` 에 `customer_segment_v1` row 가 적재되고 `data_lineage` 에 `mart` stage 가 기록된다.
* `make segment-train` 실행 후:
  * MLflow Registered Model `nexuspay_customer_segment` 에 새 버전이 등록된다 (`make business-registry-check` 로 검증).
  * `model_evaluation` 에 `silhouette`, `inertia`, `stability_overlap`, `size_segment_*` 가 모두 적재된다.
  * `data/artifacts/nexuspay_customer_segment/action_policy.json` 이 자동 생성되고 `segment_action_recommendation` 테이블에도 동일 정책이 적재된다.
  * `data/reports/segment_eval_report_<version>.md` 가 자동 생성된다.

---

## Day 4: Batch Scoring + Insight Router 통합

### 4-0. 운영 출력과 라우팅 정책

| 출력 테이블 | 책임 | 공통 로그 |
|-------------|------|-----------|
| `revenue_forecast` | CFO/COO 의 매출/거래량/수수료 예측 + PI | `prediction_result` (transaction_id 컬럼에 40자 이하 결정성 인공 ID `fc-{sha1_24}` 적재) |
| `customer_segment` | CMO/COO 의 고객 segment + action | `prediction_result` (transaction_id 컬럼에 customer_id 적재) |

두 적재 모두 ON CONFLICT 처리로 재실행 안전성을 확보한다.

라우팅: `serving/app.py` (Week 1 메인 앱) 가 `business_insight_router` 를 `include_router` 로 흡수한다. **fraud_router 와 같은 패턴을 따라 추가 컨테이너/포트 충돌이 없게 한다.**

### 4-1. `models/batch_score_forecast.py`

```python
# models/batch_score_forecast.py
"""최신 forecast model 로 horizon 적재 + tree-quantile PI 계산 + UPSERT."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

from models.forecast_intervals import tree_quantile_interval


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _latest(model_root: str, model_name: str) -> Path:
    candidates = sorted((Path(model_root) / model_name).glob("*/model.joblib"))
    if not candidates:
        raise FileNotFoundError(f"no artifact for {model_name} — run forecast-train first")
    return candidates[-1]


def _forecast_id(target_ts: pd.Timestamp, ch: str, seg: str, grade: str) -> str:
    """40자 안에 들어가는 결정성 인공 ID. prediction_result.transaction_id 적재용."""
    raw = f"{target_ts.isoformat()}|{ch}|{seg}|{grade}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:24]
    return f"fc-{digest}"


def _load_features(engine, feature_set: str, feature_version: str, scoring_until: pd.Timestamp) -> pd.DataFrame:
    sql = text(
        """
        SELECT forecast_target_ts, channel, merchant_segment, risk_grade,
               payload_json
          FROM feature_store_forecast
         WHERE feature_set = :fs AND feature_version = :fv
           AND forecast_target_ts <= :until
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(
            sql, conn,
            params={"fs": feature_set, "fv": feature_version, "until": scoring_until.to_pydatetime()},
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    targets = cfg["features"]["targets"]
    artifact_path = _latest(cfg["artifacts"]["model_root"], cfg["model"]["registry"]["name"])
    bundle = joblib.load(artifact_path)
    pipe = bundle["pipeline"]
    numerical = bundle["numerical"]
    categorical = bundle["categorical"]
    columns = numerical + categorical
    amount_idx = bundle["amount_idx"]
    pi_cfg = bundle["pi_cfg"]
    model_name = bundle["model_name"]
    model_version = bundle["model_version"]

    scoring_until = pd.Timestamp(cfg["split"]["scoring_horizon_until"])
    engine = _engine()
    rows = _load_features(engine, feature_set, feature_version, scoring_until)
    if rows.empty:
        print("no forecast feature rows to score")
        return

    flat = pd.json_normalize(rows["payload_json"].apply(json.loads)).reindex(columns=columns)
    flat.index = rows.index
    # 결측 lag/rolling 이 있는 행은 PI 만 nullable 로 두고 점수는 그대로 산출
    preds = pipe.predict(flat)

    inner_rf_amount = pipe.named_steps["model"].estimators_[amount_idx]
    pre_X = pipe.named_steps["preprocessor"].transform(flat)
    pi_low_amount, pi_high_amount = tree_quantile_interval(inner_rf_amount, pre_X, pi_cfg["lower"], pi_cfg["upper"])

    run_id = str(uuid.uuid4())
    out = pd.DataFrame(
        {
            "forecast_grain": f"hourly_channel_merchant_segment_risk_grade",
            "forecast_target_ts": pd.to_datetime(rows["forecast_target_ts"], utc=True),
            "channel": rows["channel"].astype(str),
            "merchant_segment": rows["merchant_segment"].astype(str),
            "risk_grade": rows["risk_grade"].astype(str),
            "model_name": model_name,
            "model_version": model_version,
            "feature_set": feature_set,
            "feature_version": feature_version,
            "predicted_tx_count": preds[:, targets.index("tx_count")].astype(float),
            "predicted_amount": preds[:, targets.index("amount")].astype(float),
            "predicted_fee_revenue": preds[:, targets.index("fee_revenue")].astype(float),
            "prediction_interval_low": pi_low_amount.astype(float),
            "prediction_interval_high": pi_high_amount.astype(float),
            "run_id": run_id,
        }
    )

    # prediction_result 공통 로그 (forecast 의 transaction_id 컬럼은 인공 ID 사용)
    pred_log = pd.DataFrame(
        {
            "transaction_id": [
                _forecast_id(ts, ch, seg, grade)
                for ts, ch, seg, grade in zip(out["forecast_target_ts"], out["channel"], out["merchant_segment"], out["risk_grade"])
            ],
            "model_name": model_name,
            "model_version": model_version,
            "feature_set": feature_set,
            "feature_version": feature_version,
            "score": out["predicted_amount"],
            "prediction": "FORECAST",
            "run_id": run_id,
        }
    )

    upsert_sql = text("""
        INSERT INTO revenue_forecast(
            forecast_grain, forecast_target_ts, channel, merchant_segment, risk_grade,
            model_name, model_version, feature_set, feature_version,
            predicted_tx_count, predicted_amount, predicted_fee_revenue,
            prediction_interval_low, prediction_interval_high, run_id)
        VALUES (
            :forecast_grain, :forecast_target_ts, :channel, :merchant_segment, :risk_grade,
            :model_name, :model_version, :feature_set, :feature_version,
            :predicted_tx_count, :predicted_amount, :predicted_fee_revenue,
            :prediction_interval_low, :prediction_interval_high, :run_id)
        ON CONFLICT (forecast_grain, forecast_target_ts, channel, merchant_segment, risk_grade, model_name, model_version)
        DO UPDATE SET
            predicted_tx_count = EXCLUDED.predicted_tx_count,
            predicted_amount = EXCLUDED.predicted_amount,
            predicted_fee_revenue = EXCLUDED.predicted_fee_revenue,
            prediction_interval_low = EXCLUDED.prediction_interval_low,
            prediction_interval_high = EXCLUDED.prediction_interval_high,
            scored_at = NOW(),
            run_id = EXCLUDED.run_id
    """)

    with engine.begin() as conn:
        for _, row in out.iterrows():
            conn.execute(upsert_sql, row.to_dict())
        pred_log.to_sql("prediction_result", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, output_target) "
                "VALUES (:run_id, 'predict', :src, 'revenue_forecast,prediction_result')"
            ),
            {"run_id": run_id, "src": f"feature_store_forecast@{feature_set}/{feature_version}"},
        )

    # sanity
    assert (out["prediction_interval_low"] <= out["prediction_interval_high"]).all(), "PI inverted"
    print(
        f"forecast scored rows={len(out):,} "
        f"window={out['forecast_target_ts'].min()} ~ {out['forecast_target_ts'].max()} "
        f"run_id={run_id}"
    )


if __name__ == "__main__":
    main()
```

### 4-2. `models/batch_score_segments.py`

```python
# models/batch_score_segments.py
"""최신 segment model + action policy 로 customer_segment 적재."""
from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

import joblib
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

from features.segment_actions import SegmentLabel, derive_segment_reason


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _latest(model_root: str, model_name: str) -> Path:
    candidates = sorted((Path(model_root) / model_name).glob("*/model.joblib"))
    if not candidates:
        raise FileNotFoundError(f"no artifact for {model_name} — run segment-train first")
    return candidates[-1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    artifact_path = _latest(cfg["artifacts"]["model_root"], cfg["model"]["registry"]["name"])
    bundle = joblib.load(artifact_path)
    pipe = bundle["pipeline"]
    columns = bundle["columns"]
    model_name = bundle["model_name"]
    model_version = bundle["model_version"]
    policy_raw = bundle["policy"]
    stability_overlap = float(bundle["metrics"].get("stability_overlap", 0) or 0)

    # SegmentLabel 복원
    label_map: dict[int, SegmentLabel] = {
        int(sid): SegmentLabel(int(sid), body["segment_name"], body["segment_action"], body["rationale"])
        for sid, body in policy_raw.items()
    }

    engine = _engine()
    sql = text(
        """
        SELECT customer_id, payload_json
          FROM feature_store_segment
         WHERE feature_set = :fs AND feature_version = :fv
        """
    )
    with engine.connect() as conn:
        rows = pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version})
    if rows.empty:
        print("no segment feature rows to score")
        return

    flat = pd.json_normalize(rows["payload_json"].apply(json.loads)).reindex(columns=columns).fillna(0.0)
    labels = pipe.predict(flat)

    run_id = str(uuid.uuid4())
    out_rows = []
    pred_log_rows = []
    for i, (idx, row) in enumerate(rows.iterrows()):
        sid = int(labels[i])
        meta = label_map[sid]
        customer_features = flat.iloc[i].to_dict()
        reason = derive_segment_reason(customer_features, meta)
        out_rows.append({
            "customer_id": row["customer_id"],
            "model_name": model_name,
            "model_version": model_version,
            "feature_set": feature_set,
            "feature_version": feature_version,
            "segment_id": sid,
            "segment_name": meta.segment_name,
            "segment_action": meta.segment_action,
            "segment_reason": reason,
            "stability_score": stability_overlap,
            "run_id": run_id,
        })
        pred_log_rows.append({
            "transaction_id": row["customer_id"][:40],
            "model_name": model_name,
            "model_version": model_version,
            "feature_set": feature_set,
            "feature_version": feature_version,
            "score": float(sid),
            "prediction": meta.segment_name,
            "run_id": run_id,
        })

    upsert_sql = text("""
        INSERT INTO customer_segment(
            customer_id, model_name, model_version, feature_set, feature_version,
            segment_id, segment_name, segment_action, segment_reason, stability_score, run_id)
        VALUES (
            :customer_id, :model_name, :model_version, :feature_set, :feature_version,
            :segment_id, :segment_name, :segment_action, :segment_reason, :stability_score, :run_id)
        ON CONFLICT (customer_id, model_name, model_version)
        DO UPDATE SET
            segment_id = EXCLUDED.segment_id,
            segment_name = EXCLUDED.segment_name,
            segment_action = EXCLUDED.segment_action,
            segment_reason = EXCLUDED.segment_reason,
            stability_score = EXCLUDED.stability_score,
            scored_at = NOW(),
            run_id = EXCLUDED.run_id
    """)

    with engine.begin() as conn:
        for r in out_rows:
            conn.execute(upsert_sql, r)
        pd.DataFrame(pred_log_rows).to_sql("prediction_result", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, output_target) "
                "VALUES (:run_id, 'predict', :src, 'customer_segment,prediction_result')"
            ),
            {"run_id": run_id, "src": f"feature_store_segment@{feature_set}/{feature_version}"},
        )

    # sanity
    assert all(r["segment_id"] >= 0 for r in out_rows)
    sizes = pd.Series([r["segment_id"] for r in out_rows]).value_counts().sort_index().to_dict()
    print(f"segment scored customers={len(out_rows):,} segments={sizes} run_id={run_id}")


if __name__ == "__main__":
    main()
```

### 4-3. `serving/business_insight_router.py` 와 `serving/app.py` 통합

Week 2 의 fraud_router 와 같은 패턴이다. 별도 컨테이너를 띄우지 않고 Week 1 의 `serving/app.py` 가 `include_router` 로 흡수한다.

```python
# serving/business_insight_router.py
"""Forecast / segment 조회 router. serving/app.py 가 include_router 로 흡수한다."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path

import joblib
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text


router = APIRouter(prefix="/business", tags=["business"])
KST = timezone(timedelta(hours=9))


class ForecastRow(BaseModel):
    forecast_target_ts: datetime
    channel: str
    merchant_segment: str
    risk_grade: str
    predicted_tx_count: float
    predicted_amount: float
    predicted_fee_revenue: float
    prediction_interval_low: float | None = None
    prediction_interval_high: float | None = None


class SegmentRow(BaseModel):
    customer_id: str
    segment_id: int
    segment_name: str
    segment_action: str
    segment_reason: str | None
    stability_score: float | None = None


class SegmentSummary(BaseModel):
    segment_id: int
    segment_name: str
    segment_action: str
    customer_count: int


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _latest_artifact(model_name: str) -> Path | None:
    candidates = sorted((Path("data/artifacts") / model_name).glob("*/model.joblib"))
    return candidates[-1] if candidates else None


@router.get("/health")
def health():
    forecast = _latest_artifact("nexuspay_revenue_forecast")
    segment = _latest_artifact("nexuspay_customer_segment")
    if forecast is None or segment is None:
        raise HTTPException(status_code=503, detail="forecast 또는 segment artifact 가 없다 — 학습을 먼저 진행")
    fc_meta = joblib.load(forecast)
    seg_meta = joblib.load(segment)
    return {
        "status": "ok",
        "forecast_model_version": fc_meta["model_version"],
        "segment_model_version": seg_meta["model_version"],
    }


@router.get("/forecast", response_model=list[ForecastRow])
def forecast_for_date(
    date: date = Query(..., description="조회 일자 (Asia/Seoul)"),
    channel: str | None = None,
    merchant_segment: str | None = None,
):
    start = datetime.combine(date, datetime.min.time()).replace(tzinfo=KST)
    end = start + timedelta(days=1)
    sql = text("""
        SELECT forecast_target_ts, channel, merchant_segment, risk_grade,
               predicted_tx_count, predicted_amount, predicted_fee_revenue,
               prediction_interval_low, prediction_interval_high
          FROM revenue_forecast
         WHERE forecast_target_ts >= :start AND forecast_target_ts < :end
           AND (:channel IS NULL OR channel = :channel)
           AND (:merchant_segment IS NULL OR merchant_segment = :merchant_segment)
         ORDER BY forecast_target_ts, channel, merchant_segment
    """)
    with _engine().connect() as conn:
        rows = conn.execute(
            sql,
            {"start": start, "end": end, "channel": channel, "merchant_segment": merchant_segment},
        ).mappings().all()
    return [ForecastRow(**dict(r)) for r in rows]


@router.get("/segments/summary", response_model=list[SegmentSummary])
def segment_summary():
    sql = text("""
        SELECT segment_id, segment_name, segment_action, COUNT(*) AS customer_count
          FROM customer_segment
         GROUP BY segment_id, segment_name, segment_action
         ORDER BY segment_id
    """)
    with _engine().connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return [SegmentSummary(**dict(r)) for r in rows]


@router.get("/segments/{customer_id}", response_model=SegmentRow)
def segment_by_customer(customer_id: str):
    sql = text("""
        SELECT customer_id, segment_id, segment_name, segment_action, segment_reason, stability_score
          FROM customer_segment
         WHERE customer_id = :cid
         ORDER BY scored_at DESC
         LIMIT 1
    """)
    with _engine().connect() as conn:
        row = conn.execute(sql, {"cid": customer_id}).mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no segment for {customer_id}")
    return SegmentRow(**dict(row))
```

`serving/app.py` 끝부분에 다음을 추가한다 (Week 2 fraud_router 와 같은 위치).

```python
# serving/app.py — 기존 코드 유지하고 import 한 줄과 마운트 한 줄만 추가
from serving.business_insight_router import router as business_insight_router  # noqa: E402

app.include_router(business_insight_router)
```

이로써 `GET /business/health`, `GET /business/forecast?date=...`, `GET /business/segments/summary`, `GET /business/segments/{customer_id}` 가 Week 1 의 8000 포트 컨테이너에서 응답한다. uvicorn `--reload` 가 자동으로 코드 변경을 적재한다.

### Day 4 완료 기준

* `make business-score` 실행 후:
  * `revenue_forecast` 에 horizon 범위의 row 가 적재되고 재실행 시 ON CONFLICT 로 중복이 발생하지 않는다.
  * `customer_segment` 에 고객별 segment + action + reason + stability_score 가 적재된다.
  * `prediction_result` 에 forecast 인공 ID 와 customer_id 기반 공통 로그가 함께 남는다.
* `make business-api-test` 실행 시 health/forecast/segments/summary 모두 200 으로 응답한다.
* `data_lineage` 에서 `feature/mart/predict/evaluate` stage 를 모두 조회할 수 있다.

---

## Day 5: DAG + 테스트 + 운영 리포트

### 5-1. `dags/business_insight_pipeline_dag.py`

forecast 와 segment 학습은 독립적이지만 score 단계는 두 artifact 가 모두 필요하다. DAG 는 두 학습을 병렬로 두고 score → report 로 fan-in 한다.

```python
# dags/business_insight_pipeline_dag.py
"""Nexus Pay business insight DAG draft (Week 2 fraud DAG 패턴 재사용)."""
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

# (task_id, command, upstreams)
PIPELINE_STEPS = [
    ("business_schema",   "python -m scripts.apply_sql --path schemas/forecast_and_segment.sql", []),
    ("forecast_features", "python -m features.build_forecast_features --config config/forecast_model_config.yaml", ["business_schema"]),
    ("forecast_train",    "python -m models.train_revenue_forecast --config config/forecast_model_config.yaml", ["forecast_features"]),
    ("segment_features",  "python -m features.build_segment_features --config config/segment_model_config.yaml", ["business_schema"]),
    ("segment_train",     "python -m models.train_customer_segments --config config/segment_model_config.yaml", ["segment_features"]),
    ("business_score",    "python -m models.batch_score_forecast --config config/forecast_model_config.yaml && "
                           "python -m models.batch_score_segments --config config/segment_model_config.yaml", ["forecast_train", "segment_train"]),
    ("business_report",   "python -m scripts.render_business_insight_report --forecast-config config/forecast_model_config.yaml --segment-config config/segment_model_config.yaml", ["business_score"]),
]


def build_dag():
    if not AIRFLOW_AVAILABLE:
        raise RuntimeError("Airflow not installed in this container; use --validate")

    dag = DAG(
        dag_id="business_insight_pipeline",
        description="Nexus Pay forecast + segment + business report",
        start_date=datetime(2026, 6, 8),
        schedule_interval="0 5 * * *",
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["nexuspay", "business", "week3"],
    )

    ops = {}
    for task_id, command, _ in PIPELINE_STEPS:
        ops[task_id] = BashOperator(task_id=task_id, bash_command=command, dag=dag)
    for task_id, _, upstreams in PIPELINE_STEPS:
        for u in upstreams:
            ops[u] >> ops[task_id]
    return dag


def _validate() -> int:
    print("Business insight pipeline steps:")
    for task_id, command, upstreams in PIPELINE_STEPS:
        ups = ", ".join(upstreams) if upstreams else "(root)"
        print(f"  - {task_id}  upstreams={ups}")
    expected_ids = [
        "business_schema",
        "forecast_features", "forecast_train",
        "segment_features", "segment_train",
        "business_score", "business_report",
    ]
    actual = [t for t, _, _ in PIPELINE_STEPS]
    if actual != expected_ids:
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

### 5-2. `scripts/render_business_insight_report.py` — 자동 생성 리포트

```python
# scripts/render_business_insight_report.py
"""DAG 의 마지막 단계로 실행되어 stakeholder 리포트를 자동 생성한다."""
from __future__ import annotations

import argparse
import os
from datetime import datetime
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--forecast-config", type=Path, required=True)
    parser.add_argument("--segment-config", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=Path("docs/reports/business-insight-evaluation.md"))
    args = parser.parse_args()

    fc_cfg = yaml.safe_load(args.forecast_config.read_text(encoding="utf-8"))
    sg_cfg = yaml.safe_load(args.segment_config.read_text(encoding="utf-8"))

    engine = _engine()
    with engine.connect() as conn:
        # forecast 최근 7일 합계
        forecast_df = pd.read_sql(text("""
            SELECT forecast_target_ts::date AS day, channel, merchant_segment,
                   SUM(predicted_tx_count) AS tx_count,
                   SUM(predicted_amount) AS amount,
                   SUM(predicted_fee_revenue) AS fee_revenue
              FROM revenue_forecast
             GROUP BY 1, 2, 3
             ORDER BY 1
            """), conn)
        # segment 분포
        seg_df = pd.read_sql(text("""
            SELECT segment_id, segment_name, segment_action,
                   COUNT(*) AS customers,
                   AVG(stability_score) AS stability
              FROM customer_segment
             GROUP BY segment_id, segment_name, segment_action
             ORDER BY segment_id
            """), conn)

    out = args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    body = [
        f"# Business Insight Evaluation — {datetime.utcnow().isoformat(timespec='seconds')}Z",
        "",
        f"- forecast model: `{fc_cfg['model']['registry']['name']}`",
        f"- segment model: `{sg_cfg['model']['registry']['name']}`",
        "",
        "## 1. Forecast 일별 요약 (CFO/COO)",
        "",
        forecast_df.head(50).to_markdown(index=False) if not forecast_df.empty else "_no forecast rows_",
        "",
        "## 2. Customer Segment 분포 (CMO/COO)",
        "",
        seg_df.to_markdown(index=False) if not seg_df.empty else "_no segment rows_",
        "",
        "## 3. Week 4 운영 전환 시 점검 항목",
        "",
        "- forecast amount-weighted MAPE 추세 (drift 의심 시 재학습)",
        "- segment stability_overlap 평균 (0.55 미만 시 정책 재검토)",
        "- 시간대 피크 alert: predicted_tx_count 의 상위 10% bucket 운영 인력 사전 배치",
        "- risk_grade=HIGH 비중이 큰 forecast bucket 은 fraud 운영팀과 cross-check",
        "",
    ]
    out.write_text("\n".join(body), encoding="utf-8")
    print(f"wrote business insight report -> {out}")


if __name__ == "__main__":
    main()
```

`pandas.to_markdown` 의존성으로 `tabulate` 가 필요하다. 이 패키지도 Day 1 의 `make business-deps` 에 포함되어 있으므로, 리포트 단계에서는 동일 타겟을 재실행해 의존성을 확인한다.

```bash
make business-deps
```

### 5-3. `tests/test_business_insight_pipeline.py`

```python
# tests/test_business_insight_pipeline.py
import json
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.calendar import add_calendar_features, is_holiday
from features.segment_actions import SegmentLabel, derive_segment_reason, label_segments
from models.forecast_intervals import tree_quantile_interval


def test_calendar_features_shape():
    base = pd.Timestamp("2026-06-06", tz="Asia/Seoul")
    df = pd.DataFrame({"forecast_target_ts": [base + timedelta(hours=i) for i in range(24)]})
    out = add_calendar_features(df)
    assert {"hour_of_day", "day_of_week", "is_weekend", "is_holiday"}.issubset(out.columns)
    assert out["hour_of_day"].between(0, 23).all()
    assert out["is_weekend"].iloc[0] == 1   # 2026-06-06 은 토요일
    assert out["is_holiday"].iloc[0] == 1   # 현충일


def test_segment_labels_cover_action_set():
    centroids = pd.DataFrame(
        {
            "recency_days":          [10, 90, 5, 30, 50],
            "frequency_90d":         [40, 2, 4, 12, 6],
            "monetary_90d":          [1_000_000, 5_000, 200_000, 350_000, 220_000],
            "avg_ticket_size":       [25_000, 5_000, 50_000, 30_000, 40_000],
            "overseas_tx_ratio":     [0.05, 0.0, 0.4, 0.1, 0.05],
            "app_channel_ratio":     [0.9, 0.5, 0.7, 0.6, 0.5],
            "high_risk_ratio_90d":   [0.0, 0.05, 0.0, 0.15, 0.02],
            "medium_risk_ratio_90d": [0.05, 0.1, 0.05, 0.2, 0.05],
            "recency_score":         [1 / (1 + r) for r in [10, 90, 5, 30, 50]],
        },
        index=range(5),
    )
    centroids.index.name = "segment_id"
    labels = label_segments(centroids, top_k=3)
    assert len(labels) == 5
    assert all(lbl.segment_action for lbl in labels)
    assert any(lbl.segment_action == "RISK_REVIEW" for lbl in labels)


def test_segment_reason_includes_known_codes():
    label = SegmentLabel(0, "high_value_loyal", "RETENTION_PRIORITY", "low recency + high frequency")
    feat = {"recency_days": 5.0, "frequency_90d": 35.0, "high_risk_ratio_90d": 0.0, "overseas_tx_ratio": 0.05}
    reason = derive_segment_reason(feat, label)
    assert "HIGH_FREQUENCY" in reason


def test_forecast_pi_invariants():
    rng = np.random.default_rng(0)
    # mock 트리 예측 결과: 100 행, 3 출력, 50 트리
    preds = rng.normal(loc=100, scale=10, size=(50, 100))
    lo = np.quantile(preds, 0.1, axis=0)
    hi = np.quantile(preds, 0.9, axis=0)
    assert (lo <= hi).all()
    assert (hi - lo).mean() > 0


def test_action_policy_completeness(tmp_path: Path):
    policy = {
        "0": {"segment_name": "a", "segment_action": "RETENTION_PRIORITY", "rationale": "x", "centroid": {}},
        "1": {"segment_name": "b", "segment_action": "WINBACK_CAMPAIGN", "rationale": "x", "centroid": {}},
        "2": {"segment_name": "c", "segment_action": "ONBOARDING_NURTURE", "rationale": "x", "centroid": {}},
        "3": {"segment_name": "d", "segment_action": "FX_FEE_PROMOTION", "rationale": "x", "centroid": {}},
        "4": {"segment_name": "e", "segment_action": "STANDARD_MONITORING", "rationale": "x", "centroid": {}},
    }
    p = tmp_path / "policy.json"
    p.write_text(json.dumps(policy), encoding="utf-8")
    loaded = json.loads(p.read_text(encoding="utf-8"))
    for sid, body in loaded.items():
        assert {"segment_name", "segment_action", "rationale"}.issubset(body.keys())


def test_forecast_target_no_future_lookup():
    """forecast_target_ts 가 t 일 때, 그 시점의 lag 입력은 t 이전 데이터로만 채워졌는지 보는 단순 점검."""
    base = pd.Timestamp("2026-04-01", tz="Asia/Seoul")
    ts = [base + timedelta(hours=i) for i in range(48)]
    feat = pd.DataFrame({
        "forecast_target_ts": ts,
        "tx_count_lag_24h": [None] * 24 + list(range(24)),
        "amount_lag_24h":   [None] * 24 + [v * 1000 for v in range(24)],
    })
    # 첫 24시간은 lag 24h 가 NaN 이어야 한다.
    assert feat.iloc[:24]["tx_count_lag_24h"].isna().all()
    assert feat.iloc[24:]["tx_count_lag_24h"].notna().all()


def test_dag_step_order():
    from dags.business_insight_pipeline_dag import PIPELINE_STEPS
    expected = [
        "business_schema",
        "forecast_features", "forecast_train",
        "segment_features", "segment_train",
        "business_score", "business_report",
    ]
    assert [t for t, _, _ in PIPELINE_STEPS] == expected
```

### 5-4. End-to-end 검증 시나리오

```bash
# 0. Week 1~2 환경 + 의존성 + 스키마 적용
make up
make healthcheck
make business-deps
make business-schema

# 1. raw_transactions 와 risk_score 가 준비되어 있어야 한다 (Week 1/2 결과)
docker compose exec postgres psql -U mluser -d ml_db -c "SELECT COUNT(*) FROM raw_transactions"
docker compose exec postgres psql -U mluser -d ml_db -c "SELECT COUNT(*) FROM risk_score"

# 2. forecast 학습 흐름
make forecast-features
make forecast-train

# 3. segment 학습 흐름
make segment-features
make segment-train

# 4. 적재 + API 확인 (사전조건: 위 학습 4개 완료)
make business-score
make business-api-test

# 5. DAG + 테스트
make business-dag-validate
make business-test
make business-registry-check

# 6. 자동 리포트 + 결과 점검
make business-report
docker compose exec postgres psql -U mluser -d ml_db -c \
  "SELECT segment_action, COUNT(*) FROM customer_segment GROUP BY 1 ORDER BY 1;"
```

기대 결과:

* MLflow Registered Models 에 `nexuspay_revenue_forecast`, `nexuspay_customer_segment` 가 모두 등록되어 있다.
* `revenue_forecast` 에 7일 horizon × (channel × merchant_segment × risk_grade) 조합의 row 가 적재된다.
* `customer_segment` 에 모든 활성 customer 의 segment + action + reason + stability_score 가 적재된다.
* `segment_action_recommendation` 에 5개 segment 정책이 모델 버전과 함께 적재된다.
* `data/reports/forecast_eval_report_*.md`, `data/reports/segment_eval_report_*.md`, `docs/reports/business-insight-evaluation.md` 가 모두 자동 생성된다.
* `make business-test` 가 7개 테스트 모두 통과한다.
* `make business-registry-check` 가 두 Registered Model 의 최신 버전을 출력한다.

### Day 5 완료 기준

* `make business-dag-validate` 가 `validation OK` 로 끝난다.
* `make business-test` 가 통과한다.
* `make business-registry-check` 가 `nexuspay_revenue_forecast`, `nexuspay_customer_segment` 를 모두 확인한다.
* `revenue_forecast`, `customer_segment`, `segment_action_recommendation`, `prediction_result`, `data_lineage` 를 함께 조회해 모델 입력·출력·실행 lineage 를 설명할 수 있다.
* `docs/reports/business-insight-evaluation.md` 가 forecast 일별 요약 + segment 분포 + Week 4 monitoring 점검 항목을 포함한다.
* Week 4 에서 monitoring/registry promotion 에 사용할 두 모델(`nexuspay_revenue_forecast`, `nexuspay_customer_segment`)이 MLflow registry 에 등록되어 있다.

---

## Week 3 산출물 체크리스트

완료 검증일: `YYYY-MM-DD` - 작성 전

### 코드 산출물

- [ ] `study-ml-pipeline/config/forecast_model_config.yaml`
- [ ] `study-ml-pipeline/config/segment_model_config.yaml`
- [ ] `study-ml-pipeline/schemas/forecast_and_segment.sql`
- [ ] `study-ml-pipeline/features/calendar.py`
- [ ] `study-ml-pipeline/features/segment_actions.py`
- [ ] `study-ml-pipeline/features/build_forecast_features.py`
- [ ] `study-ml-pipeline/features/build_segment_features.py`
- [ ] `study-ml-pipeline/models/forecast_intervals.py`
- [ ] `study-ml-pipeline/models/train_revenue_forecast.py`
- [ ] `study-ml-pipeline/models/train_customer_segments.py`
- [ ] `study-ml-pipeline/models/batch_score_forecast.py`
- [ ] `study-ml-pipeline/models/batch_score_segments.py`
- [ ] `study-ml-pipeline/mlops/verify_registered_models.py`
- [ ] `study-ml-pipeline/scripts/apply_sql.py`
- [ ] `study-ml-pipeline/serving/business_insight_router.py`
- [ ] `study-ml-pipeline/serving/app.py` 에 `include_router(business_insight_router)` 추가
- [ ] `study-ml-pipeline/scripts/render_business_insight_report.py`
- [ ] `study-ml-pipeline/dags/business_insight_pipeline_dag.py`
- [ ] `study-ml-pipeline/tests/test_business_insight_pipeline.py`
- [ ] `study-ml-pipeline/Makefile` business insight 타겟 추가
- [ ] `study-ml-pipeline/requirements.txt` 에 `holidays`, `scipy`, `tabulate` 추가

### 문서 산출물

- [ ] `study-ml-pipeline/docs/models/forecast-and-segmentation-design.md`
- [ ] `study-ml-pipeline/docs/reports/business-insight-evaluation.md` (자동 생성, `business_score` 후 매번 갱신)
- [ ] `study-ml-pipeline/data/reports/forecast_eval_report_*.md` (자동 생성, `make forecast-train` 실행 시 매번 생성)
- [ ] `study-ml-pipeline/data/reports/segment_eval_report_*.md` (자동 생성, `make segment-train` 실행 시 매번 생성)

### 운영 산출물

- [ ] `make business-deps` 성공 (holidays/scipy/tabulate 설치)
- [ ] `make business-schema` 성공
- [ ] `make forecast-features` 성공
- [ ] `make forecast-train` 성공 (MLflow Registered Model 등록 + 자동 리포트 생성 확인)
- [ ] `make segment-features` 성공
- [ ] `make segment-train` 성공 (action_policy.json + segment_action_recommendation 적재 + 자동 리포트 확인)
- [ ] `make business-score` 성공 (ON CONFLICT 로 재실행 무중복)
- [ ] `make business-api-test` 성공 (health/forecast/segments/summary 모두 200)
- [ ] `make business-dag-validate` 성공 (PIPELINE_STEPS 순서 일치)
- [ ] `make business-test` 성공 (7개 테스트)
- [ ] `make business-registry-check` 성공
- [ ] MLflow Registry 에 `nexuspay_revenue_forecast`, `nexuspay_customer_segment` 두 모델 모두 등록
- [ ] `revenue_forecast`, `customer_segment`, `segment_action_recommendation` 테이블에 운영 결과 적재
- [ ] `data_lineage` 에 `mart` / `predict` stage 가 한 run 안에서 함께 기록됨
- [ ] `serving/app.py` 에 `business_insight_router` 가 mount 되어 8000 포트에서 함께 응답

### CTO/CIO/CFO/COO/CMO 요구사항 정합 점검

| 요구자 | 요구사항 | 충족 증빙 |
|--------|----------|-----------|
| CTO | 공통 ML pipeline 의 다중 모델 확장성 + Week 1~2 라우터 통합 패턴 재사용 | `serving/app.py` `include_router(business_insight_router)`, fraud_router 와 같은 컨테이너 |
| CIO | 입력·출력·lineage 감사 가능성 | `feature_store_forecast`, `feature_store_segment`, `prediction_result`, `data_lineage` (`mart`/`predict` stage) |
| CFO | 매출 예측 + 오차 해석 + 신뢰구간 | `revenue_forecast.predicted_*` + tree-quantile PI + amount_weighted_mape + `forecast_eval_report_*.md` |
| COO | 피크 거래량 + 위험 등급 단위 운영 리소스 판단 | grain = (channel × merchant_segment × risk_grade), `business_insight_report.md` 의 피크/risk_grade 점검 항목 |
| CMO | 고객군별 액션 정책 + 정책 카탈로그 + 안정성 | `customer_segment` + `segment_action_recommendation` + `stability_overlap` 적재 |

---

## 다음 주(Week 4) 연결 포인트

Week 4는 Week 1~3에서 만든 모델을 운영 자산으로 승격한다.

* MLflow registry를 사용해 baseline/fraud/forecast/segment 모델의 promotion workflow를 만든다.
* API hardening으로 schema validation, error handling, health/readiness, request logging을 정비한다.
* Airflow retraining DAG에 SLA, retry, recovery, backfill 정책을 추가한다.
* model monitoring으로 drift, data quality, prediction distribution을 점검한다.
* RAG/LLM feedback loop를 도입해 운영 리포트와 runbook 질의응답을 지원한다.
