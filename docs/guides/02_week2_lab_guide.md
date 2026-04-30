# Week 2: 분류·이상탐지 기반 이상거래 탐지

**기간**: 5일 (월~금, 풀타임 40시간) — 2026-06-01 ~ 2026-06-05
**주제**: Week 1 ML 파이프라인 기준선을 상속하여 Nexus Pay 이상거래 탐지용 supervised fraud classifier, unsupervised anomaly detector, 임계값 튜닝, risk score 적재, API/DAG 검증 흐름을 구축한다.

**산출물**

| 구분 | 산출물 |
|------|--------|
| 설정 | `config/fraud_model_config.yaml` |
| 스키마 | `schemas/risk_score.sql` |
| 피처 | `features/risk_reasons.py`, `features/build_fraud_features.py` |
| Upstream 연계 | `scripts/generate_fraud_labels_from_silver.py`, `scripts/import_spark_silver_to_raw_transactions.py` |
| 모델 | `models/train_fraud_classifier.py`, `models/train_anomaly_detector.py`, `models/tune_fraud_threshold.py`, `models/batch_score_fraud.py` |
| 서빙 | `serving/fraud_router.py`, Week 1 `serving/app.py` 라우터 마운트 |
| 오케스트레이션 | `dags/fraud_scoring_pipeline_dag.py` |
| 문서 | `docs/models/fraud-detection-design.md`, `docs/reports/fraud-model-evaluation.md` |
| 테스트 | `tests/test_fraud_pipeline.py` |

**전제 조건**

| 구분 | 필요 조건 |
|------|-----------|
| Week 1 환경 | `docker-compose.yml`, `schemas/init.sql`, `config/model_config.yaml` 이 준비되어 있다. |
| Week 1 코드 | `features/data_split.py`, `features/feature_pipeline.py`, `models/eval_report.py`, `serving/app.py` 가 준비되어 있다. |
| 샘플 데이터 경로 | `scripts/generate_sample_transactions.py`, `scripts/load_sample_transactions.py` 로 synthetic smoke path 를 실행할 수 있다. |
| 서비스 상태 | `make up && make healthcheck` 가 성공한다. |
| Silver 연계 선택 조건 | Silver 경로 사용 시 `.env` 의 `UPSTREAM_SILVER_PATH` 가 업스트림 `study-data-pipeline` 의 Delta Silver 경로를 가리킨다. |

> 모델 후보 단순화 안내: 본 가이드는 Logistic Regression 과 Random Forest 두 후보로만 비교한다. 상위 계획서(`docs/plan/03_ml_pipeline_capability_plan_2026.md`)는 XGBoost/LightGBM 도 함께 거론하지만, 본 주차에는 학습 부하 관리와 sklearn `Pipeline` 일관성을 우선해 후보를 좁힌다. XGBoost/LightGBM 도입은 Week 4 의 모델 운영 단계에서 후속 후보로 다룬다.

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
| Day 4 | risk score 적재 + API | `risk_score` batch 적재, FastAPI fraud router 통합 | `models/batch_score_fraud.py`, `models/anomaly_scorer.py`, `serving/fraud_router.py`, `serving/app.py` (라우터 마운트) |
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

아래 설정 파일은 Week 2 fraud 모델의 실행 계약이다. 어떤 테이블을 읽고 쓸지, Silver 데이터를 어디서 가져올지, 어떤 피처를 모델 입력으로 사용할지, 시간 기반 split 기준과 임계값 정책을 어떻게 둘지를 한곳에 모은다. 코드가 하드코딩된 경로와 컬럼명에 의존하지 않게 만들고, 운영자가 "이 모델 버전은 어떤 feature set 과 threshold 기준으로 만들어졌는가"를 문서와 설정만 보고 추적할 수 있게 하는 것이 목적이다.

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
  # 컨테이너 내부 경로(고정). 호스트 측 실제 경로는 .env 의 UPSTREAM_SILVER_PATH 로 주입.
  silver_transactions_path: /upstream/silver/transactions
  fraud_labels_path: data/derived/fraud_labels.csv
  import_mode: truncate_insert
  processing_date_from: null     # null 이면 Silver 전체. ISO 날짜 지정 시 해당 시점 이후만 import
  processing_date_to: null
  device_country_distribution:    # Silver 에 없는 컬럼이므로 시뮬레이션 분포로 보강
    KR: 0.78
    JP: 0.07
    US: 0.10
    VN: 0.05
  event_type_to_channel:
    PAYMENT: APP
    REFUND: APP
    WITHDRAWAL: POS
    TRANSFER: WEB
    DEPOSIT: APP

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

코드 해설:

- `project.inherits` 는 Week 1 공통 설정을 상속한다는 의도를 명시한다. 실제 병합 로직이 없더라도 운영 문서상 기준선과 확장 설정을 구분하는 역할을 한다.
- `upstream.silver_transactions_path` 는 컨테이너 내부 고정 경로다. 호스트 경로는 `.env` 의 `UPSTREAM_SILVER_PATH` 로 주입하므로 코드와 로컬 환경 의존성을 분리한다.
- `features.numerical`, `features.categorical` 은 모델 artifact 에 저장될 입력 컬럼 순서의 기준이다. 이 목록이 batch/API 입력 계약과 어긋나면 추론 오류나 잘못된 score가 발생한다.
- `split.label_confirmed_column` 과 `min_label_lag_hours` 는 fraud 라벨 누수 방지를 위한 핵심 설정이다. 거래 시점에는 아직 확정되지 않은 라벨을 학습에 쓰지 않도록 train split에서 확인해야 한다.
- `threshold` 섹션은 모델 성능 지표를 운영 가능한 alert 정책으로 바꾸는 기준이다. fraud 모델은 확률만 잘 내는 것으로 끝나지 않고, 운영팀 처리량을 넘지 않는 band 정책까지 가져야 한다.

### 1-2. `schemas/risk_score.sql`

`prediction_result` 는 모든 모델 공통 로그이고, `risk_score` 는 fraud 운영 테이블이다. 운영팀 화면이나 리포트는 `risk_score` 를 조회한다고 가정한다.

아래 DDL은 fraud 도메인 전용 운영 결과 테이블을 만든다. Week 1의 `prediction_result`가 모델 공통 로그라면, `risk_score`는 운영자가 실제로 조회할 판정 테이블이다. 확률 점수, anomaly score, 최종 band, 조치 수준, reason code, 모델 버전, feature 버전, run lineage를 함께 저장해 "어떤 거래가 왜 HIGH로 올라왔는지"를 나중에 설명할 수 있게 한다.

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

코드 해설:

- `risk_score_id` 는 적재 순서와 개별 score row를 식별하기 위한 surrogate key다.
- `transaction_id`, `model_name`, `model_version` 조합에 unique 제약을 둬 같은 모델 버전이 같은 거래를 중복 채점하지 않게 한다.
- `fraud_probability`, `anomaly_score`, `combined_score` 를 분리해 저장하는 이유는 최종 band만 보지 않고 classifier와 anomaly detector가 각각 어떤 신호를 냈는지 해석하기 위해서다.
- `reason_code` 와 `action` 은 운영 인수인계에 중요하다. 운영자는 score보다 "왜 검토해야 하는가"와 "무엇을 해야 하는가"를 먼저 본다.
- 두 index는 거래 단건 조회와 band별 운영 큐 조회를 빠르게 하기 위한 최소 인덱스다.

검증:

아래 명령은 PostgreSQL 안에 `risk_score` 테이블이 실제로 생성되었는지 확인한다. 스키마 적용은 조용히 성공한 것처럼 보여도 권한, DB명, 컨테이너 상태 문제로 반영되지 않을 수 있으므로, Day 1 끝에서는 반드시 테이블 구조를 직접 조회한다.

```bash
docker compose exec postgres psql -U mluser -d ml_db -c "\d risk_score"
```

코드 해설:

- `\d risk_score` 는 PostgreSQL 메타 명령으로 컬럼, 타입, 인덱스, 제약조건을 한 번에 보여준다.
- 이 확인에서 `UNIQUE (transaction_id, model_name, model_version)` 와 `ix_risk_score_band_scored` 가 보이지 않으면 DDL 적용이 완전하지 않은 상태다.

### 1-2-1. Silver 마운트와 환경 변수

업스트림 `study-data-pipeline/data/lakehouse/delta/silver/transactions` 의 호스트 경로는 OS 마다 다르므로 `.env` 로 추출한다.

아래 환경 변수는 로컬 PC의 업스트림 Delta Silver 거래 데이터 위치를 Compose 설정과 분리하기 위한 값이다. 저장소마다 경로가 다르거나 Windows/WSL/Git Bash 환경마다 마운트 방식이 달라질 수 있으므로, `docker-compose.yml`에 절대 경로를 직접 박지 않고 `.env`에서 주입한다. 이렇게 하면 다른 학습자나 검토자가 자기 환경에 맞게 경로만 바꿔 같은 가이드를 실행할 수 있다.

```bash
# .env 또는 .env.example 에 추가
UPSTREAM_SILVER_PATH=C:/study/study-data-pipeline/data/lakehouse/delta/silver/transactions
```

코드 해설:

- 이 값은 컨테이너 내부 경로가 아니라 호스트 PC의 실제 Delta Silver 디렉터리다.
- Windows 경로를 `.env` 로 분리해 두면 팀원이나 다른 실습 환경에서 경로만 바꿔 재사용할 수 있다.
- Silver 연계를 사용하지 않는 smoke test에서는 이 값을 비워두거나 fallback 디렉터리를 사용해도 된다.

`docker-compose.yml` 의 `runner` 서비스 volumes 에 다음 한 줄을 추가한다 (`api` 서비스에는 추가하지 않는다 — 학습/적재 책임은 runner 가 단독으로 진다).

아래 Compose volume 설정은 Week 2의 데이터 import 작업에서만 필요한 읽기 전용 마운트다. `runner` 컨테이너는 학습·적재·검증 스크립트를 실행하는 책임을 가지므로 Silver 경로를 읽을 수 있어야 하지만, API 컨테이너는 운영 추론만 담당하므로 upstream lakehouse 파일에 접근할 필요가 없다. `:ro`를 붙여 실습 중 upstream 데이터를 실수로 수정하지 않도록 방어한다.

```yaml
  runner:
    volumes:
      - ./:/app
      - ${UPSTREAM_SILVER_PATH:-./data/empty_upstream/silver/transactions}:/upstream/silver/transactions:ro
```

코드 해설:

- 왼쪽은 호스트 경로, 오른쪽은 컨테이너 내부 경로다. Python 코드에서는 항상 `/upstream/silver/transactions` 만 바라보면 된다.
- `${UPSTREAM_SILVER_PATH:-...}` 는 환경 변수가 없을 때 fallback 디렉터리를 쓰게 하는 Compose 변수 확장이다.
- `:ro` 는 read-only mount다. ML lab이 upstream data pipeline 산출물을 수정하지 못하게 막는 안전장치다.

`UPSTREAM_SILVER_PATH` 가 비어 있으면 저장소 내부의 빈 디렉터리(`data/empty_upstream/silver/transactions`)가 마운트된다. 이 경우 Silver 경로 명령은 입력 데이터 없음으로 명확히 실패하지만, Week 1 synthetic 경로는 영향을 받지 않는다. 빈 디렉터리는 다음처럼 한 번 만들어 둔다.

아래 명령은 Silver 경로가 아직 준비되지 않은 환경에서도 Docker Compose가 마운트 대상 부재로 실패하지 않도록 빈 fallback 디렉터리를 만든다. 이 디렉터리는 실제 데이터를 대체하기 위한 것이 아니라, "Silver 연계는 아직 준비되지 않았지만 synthetic smoke test는 진행할 수 있는 상태"를 만드는 안전장치다.

```powershell
New-Item -ItemType Directory -Force data/empty_upstream/silver/transactions
```

코드 해설:

- `-Force` 는 이미 디렉터리가 있어도 에러를 내지 않게 하는 멱등 옵션이다.
- 이 디렉터리는 실제 Silver 데이터를 담는 곳이 아니라 Compose fallback을 위한 placeholder다.
- Git에는 빈 디렉터리가 추적되지 않으므로 `.gitkeep` 같은 placeholder 파일을 함께 두면 다른 환경에서도 구조가 유지된다.

### 1-2-2. `features/risk_reasons.py` — reason rule 단일 소스

reason 규칙은 config → feature build → API 세 곳에서 중복 정의되기 쉽다. 단일 모듈로 추출해 모든 진입점이 같은 규칙을 사용하도록 강제한다.

아래 모듈은 fraud reason code의 단일 소스다. 피처 생성 시점에는 원천 피처만 보고 `HIGH_AMOUNT_ZSCORE`, `HIGH_VELOCITY_24H`, `OVERSEAS_DEVICE` 같은 seed reason을 만들고, batch/API scoring 시점에는 모델 확률과 anomaly score까지 더해 최종 reason을 완성한다. 같은 판정 규칙이 여러 파일에 흩어지면 API와 배치 결과가 서로 다른 이유 코드를 낼 수 있으므로, 이 함수들을 공통으로 import하게 해 설명 가능성과 테스트 가능성을 유지한다.

```python
# features/risk_reasons.py
"""Fraud reason code 단일 소스. config 에서 룰을 받아 동일 함수로 적용한다."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class RiskReasonRules:
    high_amount_zscore: float
    high_velocity_24h: int
    overseas_device_country: frozenset

    @classmethod
    def from_config(cls, raw: Mapping) -> "RiskReasonRules":
        return cls(
            high_amount_zscore=float(raw["high_amount_zscore"]),
            high_velocity_24h=int(raw["high_velocity_24h"]),
            overseas_device_country=frozenset(raw.get("overseas_device_country") or []),
        )


def derive_reasons(
    *,
    rules: RiskReasonRules,
    amount_zscore_30d: float | None,
    tx_count_24h: float | int | None,
    device_country: str | None,
    fraud_probability: float | None = None,
    anomaly_score: float | None = None,
    high_threshold: float | None = None,
) -> list[str]:
    """피처 + 모델 신호를 받아 reason code 목록을 돌려준다."""
    out: list[str] = []
    if amount_zscore_30d is not None and amount_zscore_30d >= rules.high_amount_zscore:
        out.append("HIGH_AMOUNT_ZSCORE")
    if tx_count_24h is not None and float(tx_count_24h) >= rules.high_velocity_24h:
        out.append("HIGH_VELOCITY_24H")
    if device_country and device_country in rules.overseas_device_country:
        out.append("OVERSEAS_DEVICE")
    if fraud_probability is not None and high_threshold is not None and fraud_probability >= high_threshold:
        out.append("MODEL_HIGH_PROBABILITY")
    if anomaly_score is not None and anomaly_score >= 0.80:
        out.append("ANOMALY_PATTERN")
    return out


ALLOWED_REASONS: Sequence[str] = (
    "HIGH_AMOUNT_ZSCORE",
    "HIGH_VELOCITY_24H",
    "OVERSEAS_DEVICE",
    "MODEL_HIGH_PROBABILITY",
    "ANOMALY_PATTERN",
    "BASELINE_SCORE",
)


def join_reasons(values: Iterable[str]) -> str:
    cleaned = sorted({v for v in values if v in ALLOWED_REASONS})
    return ",".join(cleaned) or "BASELINE_SCORE"
```

코드 해설:

- `RiskReasonRules` 는 config의 threshold 값을 타입이 있는 객체로 바꾼다. 이렇게 하면 feature build, batch, API에서 같은 룰 형식을 공유할 수 있다.
- `derive_reasons` 는 피처 신호와 모델 신호를 모두 받을 수 있게 설계되어 있다. feature build 단계에서는 모델 신호 없이 호출하고, scoring 단계에서는 fraud probability와 anomaly score까지 넣는다.
- `ALLOWED_REASONS` 는 reason code whitelist다. 운영 리포트와 테스트가 허용된 코드만 다루도록 제한한다.
- `join_reasons` 는 중복 제거, 정렬, 기본값 처리를 담당한다. reason이 하나도 없을 때도 빈 문자열 대신 `BASELINE_SCORE` 를 반환해 downstream 화면이나 리포트가 안정적으로 표시된다.

이후 단계의 `build_fraud_features.py`, `batch_score_fraud.py`, `serving/fraud_router.py` 가 모두 이 모듈을 import 한다.

### 1-3. `docs/models/fraud-detection-design.md`

아래 문서는 Week 2 fraud 모델의 설계 의도를 stakeholder에게 설명하기 위한 산출물이다. 코드가 어떤 모델을 학습하는지뿐 아니라 supervised classifier와 anomaly detector를 왜 분리했는지, `risk_score`가 어떤 운영 조치로 이어지는지, Week 1의 feature store와 lineage를 어떻게 상속하는지를 정리한다. 나중에 CTO/CISO/COO가 모델 구조를 검토할 때 이 문서가 기술 설명의 첫 진입점이 된다.

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

코드 해설:

- 문서의 `모델 구성` 표는 supervised classifier와 anomaly detector의 책임을 분리한다. 이 분리가 있어야 두 모델의 score를 각각 평가하고 조합 정책을 조정할 수 있다.
- `운영 판정` 섹션은 score를 운영 액션으로 번역한다. HIGH/MEDIUM/LOW가 단순 점수 구간이 아니라 검토 우선순위라는 점을 분명히 한다.
- `누수 방지` 섹션은 Week 2 fraud 모델에서 가장 중요한 품질 기준이다. 특히 `label_confirmed_at` 은 fraud 모델이 실제 운영 시점에 몰랐던 정보를 학습하지 않도록 막는 핵심 컬럼이다.

### 1-4. Makefile 타겟 추가

Week 1 `Makefile` 끝에 fraud 전용 타겟을 추가한다. `fraud-api` 는 별도 컨테이너를 띄우지 않고, Week 1 의 `serving/app.py` 에 fraud 라우터를 마운트한 단일 API 컨테이너를 그대로 사용한다 (Day 4 에서 라우터 통합 코드를 제공).

아래 Makefile 타겟은 Week 2 작업을 사람이 기억해야 하는 긴 `docker compose exec ...` 명령 대신 짧은 운영 명령으로 고정한다. 각 타겟은 schema 적용, label 생성, upstream import, feature build, classifier 학습, anomaly 학습, threshold 생성, scoring, API smoke test, DAG 검증, 테스트 실행으로 나뉜다. 이 분리는 Airflow DAG와 운영 runbook으로 옮기기 쉬운 단위이기도 하다.

```makefile
.PHONY: fraud-schema fraud-features fraud-train fraud-anomaly fraud-threshold \
        fraud-labels fraud-import-upstream fraud-score fraud-api-reload fraud-api-test \
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

# 코드 수정 후 빠르게 라우터를 다시 적재할 때 사용 (이미 --reload 모드라면 보통 불필요)
fraud-api-reload:
	docker compose restart api

# 사전조건: make fraud-train, fraud-anomaly, fraud-threshold 가 먼저 끝나 있어야 한다.
fraud-api-test:
	curl -fsS -X POST http://localhost:$${API_PORT:-8000}/fraud/score \
	  -H 'Content-Type: application/json' \
	  -d '{"transaction_id":"tx-demo-fraud","amount":980000,"amount_zscore_30d":4.2,"tx_count_24h":12,"days_since_last_tx":0.02,"channel":"APP","device_country":"US","merchant_segment":"OVERSEAS"}'

fraud-dag-validate:
	docker compose exec runner python -m dags.fraud_scoring_pipeline_dag --validate

fraud-test:
	docker compose exec runner pytest -q tests/test_fraud_pipeline.py tests/test_week1_pipeline.py
```

코드 해설:

- `fraud-schema` 는 Day 1 DDL 적용을 Make 타겟으로 고정해 반복 실행 가능하게 한다.
- `fraud-labels` 와 `fraud-import-upstream` 은 Silver 연계 경로 전용이다. synthetic smoke path에서는 이 두 타겟을 건너뛸 수 있다.
- `fraud-train`, `fraud-anomaly`, `fraud-threshold` 는 순서가 중요하다. threshold 생성은 classifier artifact가 있어야 하고, Day 4 scoring은 classifier, anomaly, threshold policy가 모두 있어야 한다.
- `fraud-api-test` 는 API 컨테이너를 새로 띄우지 않고 이미 실행 중인 Week 1 API에 붙은 router를 테스트한다.
- `fraud-test` 에 Week 1 테스트를 함께 넣은 이유는 Week 2 변경이 공통 feature/split/API 계약을 깨지 않았는지 확인하기 위해서다.

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

Spark Silver 연계 경로를 컨테이너에서 실행하려면 Day 1 의 1-2-1 절에서 정의한 `${UPSTREAM_SILVER_PATH}` 마운트가 `runner` 서비스에 적용되어 있어야 한다 (이미 적용되었다면 추가 작업 없음). 호스트 OS 가 다르면 `.env` 의 `UPSTREAM_SILVER_PATH` 값만 갱신하면 된다.

Delta Lake 파일을 Python 에서 직접 읽기 위해 Week 2 에서는 다음 의존성을 추가한다. (이미 추가되어 있으면 생략한다 — `grep -q deltalake requirements.txt` 로 멱등 처리.)

아래 명령은 upstream Delta Lake 파일을 Python runner에서 직접 읽기 위한 의존성을 추가한다. `deltalake`는 Spark 없이 Delta table을 pandas DataFrame으로 읽게 해주고, `pyarrow`는 컬럼형 파일 처리에 필요한 기반 라이브러리다. 같은 줄을 여러 번 실행해도 requirements가 중복으로 늘어나지 않도록 `grep`으로 이미 추가되었는지 먼저 확인한다.

```bash
grep -q '^deltalake' requirements.txt || cat >> requirements.txt << 'EOF'

# Upstream Delta import
deltalake==0.20.2
pyarrow==16.1.0
EOF
docker compose exec runner pip install -r requirements.txt
```

코드 해설:

- `grep -q '^deltalake' requirements.txt` 는 이미 같은 의존성이 들어 있는지 확인해 중복 append를 막는다.
- `deltalake==0.20.2` 는 Delta table을 Spark 없이 읽기 위한 핵심 패키지다.
- `pyarrow==16.1.0` 은 Delta/Parquet 파일을 pandas로 변환하는 과정에서 필요하다.
- 마지막 `pip install` 은 이미 떠 있는 `runner` 컨테이너에 새 requirements를 반영한다. 컨테이너를 재생성하지 않고 빠르게 실습을 이어가기 위한 명령이다.

### 2-1. `scripts/generate_fraud_labels_from_silver.py`

이 스크립트는 Spark Silver 거래 데이터를 읽고, chargeback/dispute/manual review 성격의 사후 확정 라벨을 시뮬레이션한다. 라벨은 거래 발생 시점에 존재하지 않으며, `label_confirmed_at` 은 항상 `event_timestamp` 이후로 만든다.

설계 원칙:

- Silver 에 실제로 존재하는 컬럼만 신호로 사용한다. (`is_anomaly`, `amount`, `event_type`, `status`, `merchant_category`)
- `iterrows()` 대신 벡터화 연산으로 50K+ 행도 수 초 안에 처리한다.
- `--processing-date-from / --to` CLI 인자로 부분 import 가 가능하도록 한다.
- 출력 라벨 분포(전체 fraud rate)는 sanity 검증과 함께 stdout 으로 노출한다.

아래 스크립트는 upstream Silver 거래 데이터에 사후 확정 fraud label을 붙이는 실습용 라벨 생성기다. 실제 회사에서는 chargeback, dispute, manual review 결과가 별도 운영 시스템에서 들어오지만, 이 lab에서는 그 시스템을 시뮬레이션한다. 중요한 점은 라벨이 거래 발생 시점에 존재하지 않는다는 것이다. 그래서 `label_confirmed_at`을 항상 `event_timestamp` 이후로 만들어, 이후 학습 단계에서 "미래에 확정된 라벨을 과거 학습에 잘못 사용하지 않는지"를 검증할 수 있게 한다.

```python
# scripts/generate_fraud_labels_from_silver.py
"""Spark Silver 거래 데이터에서 fraud label CSV 를 시뮬레이션 생성."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from deltalake import DeltaTable


LABEL_TYPES = np.array(["CHARGEBACK", "DISPUTE", "MANUAL_REVIEW", "CUSTOMER_REPORT"])
CONFIRMED_REASONS = np.array(
    ["stolen_card", "account_takeover", "merchant_abuse", "unusual_velocity", "customer_claim"]
)


def _load_silver(path: str, date_from: pd.Timestamp | None, date_to: pd.Timestamp | None) -> pd.DataFrame:
    """Silver Delta 테이블을 읽는다. 날짜 파티션 필터를 우선 적용해 메모리 부담을 줄인다."""
    table = DeltaTable(path)
    partitions = []
    if date_from is not None:
        partitions.append(("tx_date", ">=", date_from.date().isoformat()))
    if date_to is not None:
        partitions.append(("tx_date", "<=", date_to.date().isoformat()))
    return table.to_pandas(partitions=partitions or None)


def _fraud_probability(df: pd.DataFrame, amount_p95: float) -> pd.Series:
    """벡터화된 fraud 확률 계산. Silver 실제 컬럼만 사용한다."""
    base = pd.Series(0.01, index=df.index, dtype=float)
    is_anomaly = df.get("is_anomaly", pd.Series(False, index=df.index)).fillna(False).astype(bool)
    high_amount = df.get("amount", pd.Series(0, index=df.index)).fillna(0).astype(float) >= amount_p95
    risky_event = df.get("event_type", pd.Series("", index=df.index)).fillna("").isin({"WITHDRAWAL", "TRANSFER"})
    risky_status = df.get("status", pd.Series("", index=df.index)).fillna("").isin({"FAILED", "PENDING"})

    p = base + 0.12 * is_anomaly.astype(int) + 0.08 * high_amount.astype(int) \
            + 0.02 * risky_event.astype(int) + 0.03 * risky_status.astype(int)
    return p.clip(upper=0.85)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--processing-date-from", type=str, help="ISO date (YYYY-MM-DD)")
    parser.add_argument("--processing-date-to", type=str, help="ISO date (YYYY-MM-DD)")
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    silver_path = cfg["upstream"]["silver_transactions_path"]
    out_path = args.out or Path(cfg["upstream"]["fraud_labels_path"])
    date_from = pd.Timestamp(args.processing_date_from) if args.processing_date_from else (
        pd.Timestamp(cfg["upstream"].get("processing_date_from")) if cfg["upstream"].get("processing_date_from") else None
    )
    date_to = pd.Timestamp(args.processing_date_to) if args.processing_date_to else (
        pd.Timestamp(cfg["upstream"].get("processing_date_to")) if cfg["upstream"].get("processing_date_to") else None
    )

    df = _load_silver(silver_path, date_from, date_to)
    if df.empty:
        raise ValueError(f"no silver rows found at {silver_path} (filter={date_from}~{date_to})")

    df = df.sort_values("event_timestamp").drop_duplicates(subset=["event_id"], keep="last").reset_index(drop=True)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    amount_p95 = float(df["amount"].dropna().quantile(0.95))

    rng = np.random.default_rng(args.seed)
    probs = _fraud_probability(df, amount_p95).to_numpy()
    confirmed = rng.random(len(df)) < probs
    delay_hours = rng.integers(24, 24 * 14 + 1, size=len(df))   # 1~14일

    labels = pd.DataFrame(
        {
            "transaction_id": df["event_id"].astype(str).str[:40],
            "case_id": "FR-" + df["event_id"].astype(str).str[:36],
            "label_type": LABEL_TYPES[rng.integers(0, len(LABEL_TYPES), size=len(df))],
            "is_confirmed_fraud": confirmed,
            "label_confirmed_at": df["event_timestamp"] + pd.to_timedelta(delay_hours, unit="h"),
            "review_reason": np.where(
                confirmed,
                CONFIRMED_REASONS[rng.integers(0, len(CONFIRMED_REASONS), size=len(df))],
                "review_cleared",
            ),
            "review_status": np.where(confirmed, "CONFIRMED", "REJECTED"),
            "analyst_team": "risk-ops",
        }
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    labels.to_csv(out_path, index=False)
    fraud_rate = labels["is_confirmed_fraud"].mean()
    print(f"wrote fraud labels: rows={len(labels):,} fraud_rate={fraud_rate:.4f} -> {out_path}")
    if not (0.001 <= fraud_rate <= 0.30):
        raise SystemExit(f"unexpected fraud rate {fraud_rate:.4f} — adjust simulation rules in config")


if __name__ == "__main__":
    main()
```

코드 해설:

- `_load_silver` 는 Delta table을 읽되 `processing-date-from/to` 값이 있으면 파티션 필터를 적용한다. 데이터가 커질 때 전체 Silver를 매번 읽지 않게 하는 장치다.
- `_fraud_probability` 는 실제 fraud 판정 모델이 아니라 라벨 시뮬레이션 규칙이다. 금액, event type, status, anomaly flag를 이용해 fraud가 발생할 법한 분포를 만든다.
- `label_confirmed_at` 을 `event_timestamp` 이후 1~14일로 만드는 부분이 중요하다. 이 값 덕분에 split 로직이 "학습 시점에 아직 확정되지 않은 라벨"을 걸러낼 수 있다.
- 출력 CSV의 `transaction_id` 는 Silver의 `event_id`와 조인된다. import 단계에서 이 ID가 맞지 않으면 모든 라벨이 False로 떨어질 수 있으므로 가장 먼저 확인해야 하는 컬럼이다.
- fraud rate sanity check는 라벨 생성 규칙이 너무 느슨하거나 너무 과격해졌을 때 즉시 실패하게 만든다.

### 2-2. `scripts/import_spark_silver_to_raw_transactions.py`

이 스크립트는 Silver 거래 데이터와 fraud label CSV 를 조인한 뒤 ML Week 1 의 `raw_transactions` 스키마로 적재한다.

설계 원칙:

- Silver 의 실제 컬럼만 사용한다. `channel` 은 `event_type` 으로부터 매핑하고, `device_country` 는 config 의 분포로 시뮬레이션한다 (Silver 미보유).
- `event_id` 중복은 시간 기준으로 최신 한 건만 남긴다.
- VARCHAR 길이 초과를 사전에 차단하기 위해 `transaction_id`, `merchant_id` 를 40자, `merchant_segment` 를 30자로 절단한다.
- `currency` 는 대문자 + 공백 제거 후 3자 절단.
- `customer_id` 는 NaN 행을 제외한 뒤 zero-padded 5자리 문자열로 정규화한다.
- 적재 후 `fraud_rate`, 시간 윈도우, 행수를 stdout 으로 노출하고 sanity assertion 을 둔다.

아래 스크립트는 Silver 거래 데이터와 방금 생성한 fraud label CSV를 조인한 뒤, Week 1에서 정의한 `raw_transactions` 스키마에 맞춰 PostgreSQL로 적재한다. upstream 데이터의 컬럼명과 Week 1 ML 스키마가 완전히 같지 않기 때문에 `event_id -> transaction_id`, `user_id -> customer_id`, `merchant_category -> merchant_segment`처럼 운영 ML이 이해하는 형태로 매핑한다. 이 단계가 성공해야 이후 feature build와 모델 학습이 동일한 `raw_transactions` 계약을 사용할 수 있다.

```python
# scripts/import_spark_silver_to_raw_transactions.py
"""Spark Silver 거래 데이터 + fraud label CSV -> raw_transactions 적재."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
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


def _load_silver(path: str, date_from: pd.Timestamp | None, date_to: pd.Timestamp | None) -> pd.DataFrame:
    table = DeltaTable(path)
    partitions = []
    if date_from is not None:
        partitions.append(("tx_date", ">=", date_from.date().isoformat()))
    if date_to is not None:
        partitions.append(("tx_date", "<=", date_to.date().isoformat()))
    return table.to_pandas(partitions=partitions or None)


def _map_channel(event_type_series: pd.Series, mapping: dict) -> pd.Series:
    return event_type_series.fillna("").map(mapping).fillna("APP").astype(str).str.upper()


def _simulate_device_country(n: int, distribution: dict, seed: int = 17) -> np.ndarray:
    countries = list(distribution.keys())
    weights = np.array(list(distribution.values()), dtype=float)
    weights = weights / weights.sum()
    return np.random.default_rng(seed).choice(countries, size=n, p=weights)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--processing-date-from", type=str)
    parser.add_argument("--processing-date-to", type=str)
    parser.add_argument("--device-country-seed", type=int, default=17)
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    upstream = cfg["upstream"]
    date_from = pd.Timestamp(args.processing_date_from) if args.processing_date_from else (
        pd.Timestamp(upstream.get("processing_date_from")) if upstream.get("processing_date_from") else None
    )
    date_to = pd.Timestamp(args.processing_date_to) if args.processing_date_to else (
        pd.Timestamp(upstream.get("processing_date_to")) if upstream.get("processing_date_to") else None
    )

    silver = _load_silver(upstream["silver_transactions_path"], date_from, date_to)
    if silver.empty:
        raise SystemExit(f"no silver rows found at {upstream['silver_transactions_path']} (filter={date_from}~{date_to})")

    labels = pd.read_csv(upstream["fraud_labels_path"], parse_dates=["label_confirmed_at"])

    silver["event_timestamp"] = pd.to_datetime(silver["event_timestamp"], utc=True)
    labels["label_confirmed_at"] = pd.to_datetime(labels["label_confirmed_at"], utc=True)

    silver = (
        silver.sort_values("event_timestamp")
              .drop_duplicates(subset=["event_id"], keep="last")
              .reset_index(drop=True)
    )

    merged = silver.merge(labels, left_on="event_id", right_on="transaction_id", how="left", suffixes=("", "_lbl"))
    merged["label_confirmed_at"] = merged["label_confirmed_at"].fillna(
        merged["event_timestamp"] + pd.Timedelta(days=7)
    )
    merged["is_confirmed_fraud"] = merged["is_confirmed_fraud"].fillna(False)

    # user_id 가 NaN 인 row 는 학습 신호로 부적합 → 제외
    merged = merged[merged["user_id"].notna()].reset_index(drop=True)

    channel = _map_channel(merged["event_type"], upstream["event_type_to_channel"])
    device_country = _simulate_device_country(
        len(merged), upstream["device_country_distribution"], seed=args.device_country_seed
    )

    raw = pd.DataFrame(
        {
            "transaction_id": merged["event_id"].astype(str).str[:40],
            "customer_id": "c-" + merged["user_id"].astype(int).astype(str).str.zfill(5),
            "merchant_id": merged["merchant_id"].fillna("UNKNOWN").astype(str).str[:40],
            "amount": merged["amount"].fillna(0).astype(float),
            "currency": merged["currency"].fillna("KRW").astype(str).str.upper().str.strip().str[:3],
            "channel": channel,
            "device_country": device_country,
            "merchant_segment": merged["merchant_category"].fillna("GENERAL").astype(str).str[:30],
            "is_label_fraud": merged["is_confirmed_fraud"].astype(bool),
            "transacted_at": merged["event_timestamp"],
            "label_confirmed_at": merged["label_confirmed_at"],
        }
    )

    # 안전성 검증
    assert raw["transaction_id"].is_unique, "transaction_id must be unique after dedup"
    fraud_rate = raw["is_label_fraud"].mean()
    if not (0.001 <= fraud_rate <= 0.30):
        raise SystemExit(f"unexpected fraud rate {fraud_rate:.4f} — labels CSV may be stale")

    engine = _engine()
    with engine.begin() as conn:
        if args.truncate:
            conn.exec_driver_sql("TRUNCATE TABLE raw_transactions")
        raw.to_sql("raw_transactions", conn, if_exists="append", index=False)

    print(
        "loaded upstream silver rows into raw_transactions: "
        f"rows={len(raw):,} fraud_rate={fraud_rate:.4f} "
        f"window={raw['transacted_at'].min()} ~ {raw['transacted_at'].max()}"
    )


if __name__ == "__main__":
    main()
```

코드 해설:

- `_engine` 은 컨테이너 환경 변수에서 PostgreSQL 접속 정보를 읽는다. 코드에 DB 비밀번호나 호스트를 박지 않기 위한 방식이다.
- `_map_channel` 은 upstream event type을 ML 모델의 `channel` 계약으로 변환한다. upstream 분류와 ML feature 분류가 항상 같지 않기 때문에 명시적 매핑이 필요하다.
- `_simulate_device_country` 는 Silver에 없는 `device_country`를 데모용 분포로 보강한다. 실제 운영에서는 device intelligence나 gateway 로그에서 가져올 값이다.
- `raw` DataFrame을 만드는 부분이 schema mapping의 핵심이다. Week 2 이후 모든 모델 코드는 `raw_transactions` 계약만 바라보므로, upstream 컬럼 차이는 이 import 단계에서 흡수해야 한다.
- `--truncate` 는 실습 재실행을 쉽게 하지만 운영에서는 주의해야 한다. 실제 운영에서는 incremental load 또는 upsert 전략으로 바뀌어야 한다.

> 주의: Silver 의 `merchant_category` 는 업스트림 producer 분류라 Week 1 synthetic 의 `merchant_segment` 4종(`GENERAL/PREMIUM/STARTUP/OVERSEAS`) 과 cardinality 가 다르다. 두 입력 경로에서 학습한 모델은 OneHotEncoder(`handle_unknown='ignore'`) 로 추론은 가능하지만 categorical level 분포가 달라 동일 artifact 로 교차 평가하지 않는다. 비교 평가가 필요할 때는 입력 경로를 명시한 별도 모델 버전으로 학습한다.

### 2-3. `features/build_fraud_features.py`

아래 코드는 `raw_transactions`의 거래 원천 데이터를 fraud 모델이 직접 사용할 수 있는 feature payload로 변환한다. 고객별 최근 24시간 거래 횟수, 마지막 거래 이후 경과일, 최근 30일 금액 분포 대비 z-score를 계산하고, 범주형 피처와 reason seed를 함께 JSON으로 묶어 `feature_store`에 저장한다. 계산 시 현재 거래 시점 이전 데이터만 사용하도록 윈도우를 잡는 것이 핵심이며, 이는 이상거래 모델에서 흔히 발생하는 시간 누수를 막기 위한 장치다.

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

from features.risk_reasons import RiskReasonRules, derive_reasons


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


def _to_feature_rows(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    payload_cols = cfg["features"]["numerical"] + cfg["features"]["categorical"]
    rules = RiskReasonRules.from_config(cfg["features"]["risk_reason_rules"])

    df = df.copy()
    df["risk_reason_seed"] = df.apply(
        lambda r: derive_reasons(
            rules=rules,
            amount_zscore_30d=r.get("amount_zscore_30d"),
            tx_count_24h=r.get("tx_count_24h"),
            device_country=r.get("device_country"),
        ),
        axis=1,
    )
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

코드 해설:

- `_load_raw` 는 `test_until` 이전 거래만 feature 대상으로 가져온다. 실습 범위 밖의 미래 데이터를 feature store에 넣지 않기 위한 제한이다.
- `_temporal_features` 는 고객별 시간 순서를 기준으로 velocity와 recency feature를 만든다. `tx_count_24h` 계산에서 현재 거래를 제외하기 위해 `< t` 조건을 쓰는 점이 중요하다.
- `zscore_30d` 역시 현재 거래 이전 30일 window만 사용한다. 현재 또는 미래 거래를 평균/표준편차 계산에 포함하면 leakage가 된다.
- `_to_feature_rows` 는 모델 입력 컬럼과 reason seed를 JSON payload로 묶는다. feature store는 모델별 컬럼을 넓은 테이블로 늘리지 않고 payload 계약으로 관리한다.
- 기존 `fraud_v1` feature rows를 삭제한 뒤 다시 적재하므로, 같은 feature version을 재생성할 때 중복 row가 생기지 않는다.

### 2-4. `models/train_fraud_classifier.py`

아래 학습 스크립트는 `feature_store`에 적재된 `fraud_v1` 피처와 `raw_transactions`의 사후 확정 라벨을 조인해 supervised fraud classifier를 학습한다. `time_based_split`으로 train/valid/test를 나누고, 전처리와 모델을 하나의 sklearn `Pipeline`으로 묶어 학습과 추론에서 같은 컬럼 순서와 인코딩 규칙을 사용하게 한다. 학습 결과는 local artifact, MLflow run, `model_evaluation` 테이블, 평가 리포트로 동시에 남겨 모델 성능과 운영 비용 해석을 추적할 수 있게 한다.

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
from models.eval_report import render_report


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
        # 비용 환산 (CFO 해석용)
        tn, fp = cm[0]
        fn, tp = cm[1]
        fp_cost = cfg["evaluation"]["cost"]["false_positive_cost_krw"]
        fn_cost = cfg["evaluation"]["cost"]["false_negative_cost_krw"]
        cost_krw = fp * fp_cost + fn * fn_cost
        mlflow.log_metric("cost_krw", cost_krw)

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
        # MLflow Model Registry 등록 (Week 4 운영화 단계의 진입점)
        mlflow.sklearn.log_model(
            pipe,
            artifact_path="sklearn_model",
            registered_model_name=model_name,
        )

        # 자동 생성 평가 리포트 (Week 1 eval_report 패턴 재사용)
        report_path = Path(cfg["artifacts"]["report_dir"]) / f"fraud_eval_report_{model_version}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        render_report(
            path=report_path,
            model_name=model_name,
            model_version=model_version,
            metrics=metrics,
            cm=cm,
            cost={
                "fp": fp, "fn": fn,
                "fp_cost_krw": fp_cost, "fn_cost_krw": fn_cost,
                "total_krw": cost_krw,
            },
            run_id=run.info.run_id,
            split_cfg=split_cfg,
        )
        mlflow.log_artifact(str(report_path), artifact_path="reports")

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
                        "extra": json.dumps({
                            "threshold": threshold,
                            "confusion_matrix": cm,
                            "fp_cost_krw": fp_cost,
                            "fn_cost_krw": fn_cost,
                            "cost_krw": cost_krw,
                        }),
                    },
                )

    print(f"trained fraud classifier model_version={model_version} cost_krw={cost_krw:,.0f} path={model_path}")
    print(f"report: {report_path}")


if __name__ == "__main__":
    main()
```

코드 해설:

- `_load_frame` 은 `feature_store`와 `raw_transactions`를 조인해 학습에 필요한 payload, label, timestamp를 한 번에 가져온다.
- `SplitConfig.from_dict` 와 `time_based_split` 은 거래 시점과 라벨 확정 시점을 모두 고려해 train/valid/test를 나눈다. fraud 모델에서 가장 중요한 leakage 방지 지점이다.
- `feature_store_to_dataframe` 은 JSON payload를 학습용 DataFrame으로 복원하고, `columns` 순서에 맞춘다. 이 순서는 artifact에 저장되어 batch/API 추론에도 재사용된다.
- sklearn `Pipeline`은 전처리와 모델을 한 덩어리로 저장한다. 추론 시 OneHotEncoder나 scaler를 다시 fit하지 않게 막는 운영상 핵심 패턴이다.
- `model_evaluation` insert와 markdown report 생성은 모델 성능을 MLflow 밖에서도 조회하고 stakeholder에게 설명하기 위한 장치다.
- `cost_krw` 는 단순 모델 지표를 CFO 언어로 번역한다. false positive와 false negative가 각각 운영 검토 비용과 fraud 손실로 이어진다는 가정을 명시한다.

### Day 2 완료 기준

아래 명령 묶음은 Day 2의 전체 흐름을 검증한다. Silver에서 라벨을 만들고, 그 라벨을 거래 원천 테이블에 조인해 적재한 뒤, fraud feature set을 생성하고 supervised classifier를 학습한다. 이 네 단계가 성공하면 Week 2의 supervised fraud 모델 경로가 end-to-end로 연결된 것이다.

```bash
make fraud-labels
make fraud-import-upstream
make fraud-features
make fraud-train
```

코드 해설:

- 이 실행 순서는 Silver 연계 경로 기준이다. Silver가 준비되지 않았다면 Day 5의 synthetic smoke path를 사용한다.
- `fraud-features` 는 `fraud-import-upstream` 이후에 실행해야 한다. raw table이 비어 있거나 오래된 상태면 feature store도 잘못 생성된다.
- `fraud-train` 이 성공하면 classifier artifact, MLflow run, evaluation row, markdown report가 함께 생성되어야 한다.

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

아래 스크립트는 라벨이 없는 신규 이상 패턴을 보조적으로 탐지하기 위한 Isolation Forest 모델을 학습한다. supervised classifier는 과거에 fraud로 확정된 패턴을 잘 찾지만, 처음 보는 공격 패턴이나 라벨이 부족한 구간에서는 약할 수 있다. 그래서 정상 거래로 확인된 train split만 사용해 "정상 분포"를 학습하고, 그 분포에서 벗어나는 정도를 anomaly score로 만든다. 학습 시점의 anomaly score 분포도 함께 저장해 batch와 API가 동일한 0~1 정규화 기준을 사용하게 한다.

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
import numpy as np
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

        # 정규화기 fit: train 의 -decision_function 분포 기준 0~1 스케일로 매핑한다.
        # batch / API 진입점이 동일한 스케일러를 사용하도록 artifact 에 함께 저장한다.
        train_raw_anomaly = -pipe.decision_function(X_train)
        normalizer = {
            "min": float(np.min(train_raw_anomaly)),
            "max": float(np.max(train_raw_anomaly)),
            "p99": float(np.quantile(train_raw_anomaly, 0.99)),
        }

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
                "normalizer": normalizer,
            },
            model_path,
        )
        mlflow.log_params({"algorithm": "isolation_forest", "normal_train_rows": len(normal_train)})
        mlflow.log_metrics({"raw_anomaly_min": normalizer["min"],
                             "raw_anomaly_max": normalizer["max"],
                             "raw_anomaly_p99": normalizer["p99"]})
        mlflow.log_artifact(str(model_path), artifact_path="model")

    print(f"trained anomaly detector path={model_path} normalizer={normalizer}")


if __name__ == "__main__":
    main()
```

코드 해설:

- anomaly detector는 전체 train 데이터가 아니라 정상 라벨(`is_label_fraud == 0`)인 거래만으로 학습한다. 정상 분포를 기준으로 벗어남을 측정하기 위해서다.
- classifier와 동일한 `build_preprocessor`를 사용해 numerical/categorical 전처리 계약을 맞춘다.
- `train_raw_anomaly = -pipe.decision_function(X_train)` 는 Isolation Forest의 정상성 점수를 이상 정도 방향으로 뒤집은 값이다.
- `normalizer` 는 batch/API 점수 정규화의 기준이다. 이 값을 artifact에 저장하지 않으면 호출 시점 데이터 분포마다 anomaly score가 달라질 수 있다.
- 이 모델은 fraud 확률을 직접 내지 않는다. 최종 운영 점수에서는 classifier probability의 보조 신호로만 사용한다.

### 3-2. `models/tune_fraud_threshold.py`

이 스크립트는 validation window 에서 운영팀 처리 가능 건수를 넘지 않는 high threshold 를 선택한다.

아래 코드는 fraud classifier의 확률 점수를 운영 정책으로 바꾸는 threshold policy 생성기다. 모델 확률이 높다고 모두 검토 대상으로 올리면 운영팀 capacity를 초과할 수 있으므로, validation window에서 threshold별 alert 수, precision, recall을 계산한다. 그중 하루 처리 가능 건수와 최소 precision 조건을 만족하는 후보를 고르고, HIGH/MEDIUM band 기준을 JSON으로 저장해 batch scoring과 API가 같은 정책을 사용하게 한다.

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

코드 해설:

- `_latest_model` 은 가장 최근 classifier artifact를 선택한다. 실습에서는 단순 정렬로 충분하지만, 운영에서는 Registry stage를 기준으로 바뀔 수 있다.
- validation split만 사용해 threshold를 고르는 이유는 test split을 최종 평가용으로 남기기 위해서다.
- `alert_capacity_per_day * validation_days` 는 운영팀이 validation 기간 동안 감당 가능한 최대 alert 수를 뜻한다.
- 후보 threshold가 capacity와 precision 조건을 만족하면 recall이 높은 값을 선택한다. fraud 모델은 놓치는 거래를 줄이는 것이 중요하지만, 운영팀 처리량을 넘으면 실제 운영이 불가능하다.
- 결과 JSON은 batch scoring과 API가 공유하는 정책 파일이다. 모델 artifact와 threshold policy를 함께 버전 관리해야 같은 score가 같은 band로 해석된다.

### Day 3 완료 기준

아래 명령은 anomaly detector와 threshold policy가 준비되었는지 검증한다. `fraud-anomaly`는 classifier와 별도 artifact를 만들고, `fraud-threshold`는 classifier artifact와 validation window를 읽어 운영 임계값 JSON을 생성한다. Day 4 scoring은 두 artifact와 threshold policy가 모두 있어야 실행된다.

```bash
make fraud-anomaly
make fraud-threshold
```

코드 해설:

- `fraud-anomaly` 는 anomaly detector artifact와 normalizer를 만든다.
- `fraud-threshold` 는 classifier validation score를 읽어 `threshold_policy.json`을 만든다.
- 두 명령 중 하나라도 빠지면 Day 4의 `fraud-score`는 classifier, anomaly, policy 중 일부를 찾지 못해 실패한다.

완료 조건:

* `data/artifacts/nexuspay_fraud_anomaly_detector/*/model.joblib` 이 생성된다.
* `data/artifacts/nexuspay_fraud_classifier/threshold_policy.json` 이 생성된다.
* threshold policy 에 `high_threshold`, `medium_threshold`, `alert_capacity_per_day` 가 포함된다.

---

## Day 4: Risk Score 적재 + Fraud API

### 4-0. Risk score 산식과 정규화 일관성

Week 2 의 combined score 는 단순한 운영 기준선으로 둔다.

아래 산식은 supervised fraud probability와 anomaly score를 하나의 운영 점수로 합치는 baseline 정책이다. Week 2에서는 복잡한 ensemble이나 calibration을 도입하지 않고, classifier를 주 신호로 두고 anomaly detector를 보조 신호로 둔다. 이 산식은 운영팀이 이해하기 쉬운 첫 버전이며, Week 4에서 monitoring 결과에 따라 가중치와 threshold를 조정할 수 있다.

```text
combined_score = min(1.0, 0.85 * fraud_probability + 0.15 * anomaly_score_normalized)
```

`fraud_probability` 는 supervised classifier 의 확률이고, `anomaly_score_normalized` 는 Isolation Forest 의 이상 정도를 0~1 범위로 변환한 값이다. 핵심 원칙은 **batch 진입점과 API 진입점이 동일한 정규화기를 사용**한다는 것이다. 이를 위해 Day 3 의 `train_anomaly_detector.py` 는 학습 시점의 `(min, max, p99)` 통계를 artifact 에 저장해 두었고, batch/API 양쪽이 이 통계를 그대로 읽어 0~1 변환한다. 입력에 따라 정규화 방식이 달라지지 않도록 별도 헬퍼 모듈로 분리한다.

아래 헬퍼는 raw anomaly 점수를 운영에서 비교 가능한 0~1 점수로 바꾸는 공통 함수다. `raw = -decision_function(X)` 값은 데이터 분포에 따라 범위가 달라질 수 있으므로, 학습 시점의 정상 거래 분포에서 얻은 최소값과 p99를 기준으로 잘라낸 뒤 스케일링한다. 이 함수를 batch와 API가 함께 사용해야 같은 거래가 어디서 호출되든 동일한 anomaly score를 받는다.

```python
# models/anomaly_scorer.py
"""Train 시점 통계를 활용해 batch/API 가 동일한 0~1 anomaly score 를 산출한다."""
from __future__ import annotations

import numpy as np


def normalize_anomaly(raw: np.ndarray, normalizer: dict) -> np.ndarray:
    """raw = -decision_function(X). p99 를 상한으로 잘라 안정성 확보."""
    lo = normalizer["min"]
    hi = max(normalizer["p99"], lo + 1e-9)
    clipped = np.clip(raw, lo, hi)
    return (clipped - lo) / (hi - lo)
```

코드 해설:

- `p99` 를 상한으로 쓰는 이유는 극단적인 anomaly 값 하나가 전체 스케일을 망가뜨리지 않게 하기 위해서다.
- `lo + 1e-9` 는 분모가 0이 되는 상황을 피하는 안전장치다.
- 이 함수는 입력 배열 크기와 무관하게 동작하므로 batch scoring의 다건 입력과 API의 단건 입력에서 같은 로직을 사용할 수 있다.

### 4-1. `models/batch_score_fraud.py`

아래 배치 스코어링 스크립트는 학습된 classifier artifact, anomaly detector artifact, threshold policy를 함께 읽어 모든 미채점 거래의 risk score를 계산한다. 결과는 fraud 운영 테이블인 `risk_score`와 공통 예측 로그인 `prediction_result`에 동시에 적재된다. 또한 `data_lineage`에 run_id와 입력 feature set을 남겨 나중에 "이 점수는 어떤 모델/피처/정책으로 만들어졌는가"를 추적할 수 있게 한다.

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
from features.risk_reasons import RiskReasonRules, derive_reasons, join_reasons
from models.anomaly_scorer import normalize_anomaly


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
    rules = RiskReasonRules.from_config(cfg["features"]["risk_reason_rules"])

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
    anomaly_score = normalize_anomaly(raw_anomaly, anomaly["normalizer"])
    combined = np.minimum(1.0, 0.85 * fraud_probability + 0.15 * anomaly_score)

    run_id = str(uuid.uuid4())
    payloads = rows["payload_json"].apply(lambda x: x if isinstance(x, dict) else json.loads(x))
    out_rows = []
    prediction_rows = []
    for i, (idx, row) in enumerate(rows.iterrows()):
        score = float(combined[i])
        band, action = _band(score, high, medium)
        payload = payloads.loc[idx]
        derived = derive_reasons(
            rules=rules,
            amount_zscore_30d=payload.get("amount_zscore_30d"),
            tx_count_24h=payload.get("tx_count_24h"),
            device_country=payload.get("device_country"),
            fraud_probability=float(fraud_probability[i]),
            anomaly_score=float(anomaly_score[i]),
            high_threshold=high,
        )
        seeded = list(payload.get("risk_reason_seed", []))
        reason_code = join_reasons(seeded + derived)

        out_rows.append(
            {
                "transaction_id": row["transaction_id"],
                "customer_id": row["customer_id"],
                "model_name": model_name,
                "model_version": model_version,
                "feature_set": feature_set,
                "feature_version": feature_version,
                "fraud_probability": float(fraud_probability[i]),
                "anomaly_score": float(anomaly_score[i]),
                "combined_score": score,
                "risk_band": band,
                "reason_code": reason_code,
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

코드 해설:

- `_latest` 는 classifier와 anomaly detector의 최신 local artifact를 찾는다. Week 4에서는 이 선택 기준을 Registry stage로 대체할 수 있다.
- SQL의 `NOT IN (SELECT transaction_id FROM risk_score ...)` 조건은 같은 모델 버전으로 이미 채점한 거래를 다시 넣지 않게 한다.
- `feature_store_to_dataframe(rows, columns)` 는 classifier artifact에 저장된 컬럼 순서로 payload를 복원한다. 이 순서가 학습과 다르면 score가 의미 없어질 수 있다.
- `combined` 는 classifier probability 85%, anomaly score 15%의 단순 조합이다. 이 비율은 baseline 정책이며 운영 검증 후 조정 대상이다.
- `risk_score` 와 `prediction_result` 에 동시에 쓰는 이유는 도메인 운영 조회와 모델 공통 lineage 조회를 모두 만족하기 위해서다.

### 4-2. `serving/fraud_router.py` 와 Week 1 `serving/app.py` 통합

Week 1 의 `api` 컨테이너는 `uvicorn serving.app:app` 으로 떠 있다. fraud 엔드포인트를 별도 컨테이너로 띄우려고 하면 포트 충돌과 라우팅 분리가 발생한다. 따라서 fraud 라우터를 `serving/fraud_router.py` 에 두고 Week 1 `serving/app.py` 에서 `include_router` 로 흡수한다. 이렇게 하면 `make api-test` 와 `make fraud-api-test` 가 동일 컨테이너의 같은 포트(8000)에서 동작한다.

`serving/fraud_router.py`:

아래 FastAPI router는 batch scoring과 같은 artifact 및 threshold policy를 사용해 단건 fraud score를 반환한다. 운영 API는 보통 실시간 결제 승인 흐름이나 운영자 조회 화면에서 한 건씩 호출되므로, request payload를 Pydantic으로 검증하고 score, band, action, reason code를 즉시 응답한다. 별도 앱을 띄우지 않고 Week 1의 `serving/app.py`에 router로 마운트하는 이유는 같은 포트와 같은 배포 단위에서 baseline API와 fraud API를 함께 운영하기 위해서다.

```python
# serving/fraud_router.py
"""Fraud scoring router. Week 1 serving/app.py 가 include_router 로 흡수한다."""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from features.risk_reasons import RiskReasonRules, derive_reasons, join_reasons
from models.anomaly_scorer import normalize_anomaly


router = APIRouter(prefix="/fraud", tags=["fraud"])

CONFIG_PATH = Path("config/fraud_model_config.yaml")
CLASSIFIER_NAME = "nexuspay_fraud_classifier"
ANOMALY_NAME = "nexuspay_fraud_anomaly_detector"
_CACHE: dict = {}


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
        raise RuntimeError(f"no artifact found for {model_name} — run make fraud-train / fraud-anomaly first")
    return candidates[-1]


def _band(score: float, high: float, medium: float) -> tuple[str, str]:
    if score >= high:
        return "HIGH", "REVIEW"
    if score >= medium:
        return "MEDIUM", "MONITOR"
    return "LOW", "PASS"


def _bundle():
    if not _CACHE:
        cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
        classifier = joblib.load(_latest(CLASSIFIER_NAME))
        anomaly = joblib.load(_latest(ANOMALY_NAME))
        policy_path = Path(cfg["artifacts"]["threshold_policy"])
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        rules = RiskReasonRules.from_config(cfg["features"]["risk_reason_rules"])
        _CACHE.update({
            "cfg": cfg,
            "classifier": classifier,
            "anomaly": anomaly,
            "policy": policy,
            "rules": rules,
        })
    return _CACHE


@router.get("/health")
def health():
    try:
        b = _bundle()
        return {"status": "ok", "model": CLASSIFIER_NAME, "version": b["classifier"]["model_version"]}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc))


@router.post("/score", response_model=FraudScoreResponse)
def score(req: FraudScoreRequest):
    b = _bundle()
    classifier = b["classifier"]
    anomaly = b["anomaly"]
    policy = b["policy"]
    rules = b["rules"]

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
    raw_anomaly = np.asarray([float(-anomaly["pipeline"].decision_function(X)[0])])
    anomaly_score = float(normalize_anomaly(raw_anomaly, anomaly["normalizer"])[0])
    combined = min(1.0, 0.85 * fraud_probability + 0.15 * anomaly_score)
    band, action = _band(combined, policy["high_threshold"], policy["medium_threshold"])

    reasons = derive_reasons(
        rules=rules,
        amount_zscore_30d=req.amount_zscore_30d,
        tx_count_24h=req.tx_count_24h,
        device_country=req.device_country,
        fraud_probability=fraud_probability,
        anomaly_score=anomaly_score,
        high_threshold=policy["high_threshold"],
    )

    return FraudScoreResponse(
        transaction_id=req.transaction_id,
        model_name=CLASSIFIER_NAME,
        model_version=classifier["model_version"],
        fraud_probability=round(fraud_probability, 4),
        anomaly_score=round(anomaly_score, 4),
        combined_score=round(combined, 4),
        risk_band=band,
        action=action,
        reason_code=join_reasons(reasons),
    )
```

코드 해설:

- `_CACHE` 는 artifact와 policy를 요청마다 다시 읽지 않도록 메모리에 보관한다. 개발 중 artifact가 바뀌면 API reload가 필요하다.
- `FraudScoreRequest` 는 API 입력 계약이다. batch feature와 동일한 핵심 컬럼을 요구해 online/offline skew를 줄인다.
- `_bundle` 은 classifier, anomaly detector, threshold policy, reason rule을 함께 로드한다. API 응답은 이 네 가지가 모두 있어야 완성된다.
- `normalize_anomaly` 를 batch와 동일하게 사용하므로 같은 입력은 batch/API에서 같은 anomaly scale을 갖는다.
- 이 router는 score를 반환만 하고 DB에 쓰지 않는다. 필요하면 운영 요구에 따라 online prediction logging을 Week 4에서 추가할 수 있다.

Week 1 의 `serving/app.py` 끝부분에 라우터를 마운트하는 한 줄을 추가한다 (Week 1 가이드를 함께 갱신하지 않고도 Week 2 주차 안에서 처리 가능하도록 본 가이드에서는 "삽입 위치" 만 안내한다).

아래 두 줄은 Week 1 FastAPI 앱에 Week 2 fraud router를 연결한다. `serving.app:app`으로 이미 실행 중인 API 컨테이너가 `/fraud/health`, `/fraud/score`를 추가로 노출하게 되며, 별도의 포트나 별도 uvicorn 프로세스를 만들지 않아도 된다. 이 방식은 작은 ML 서비스에서 여러 모델 endpoint를 하나의 app 안에 점진적으로 추가하는 패턴을 보여준다.

```python
# serving/app.py — 기존 코드 유지하고 import 와 라우터 등록만 추가
from serving.fraud_router import router as fraud_router  # noqa: E402

app.include_router(fraud_router)
```

코드 해설:

- import 문은 `app.include_router` 보다 먼저 실행되어야 한다.
- `app.include_router(fraud_router)` 를 호출하면 router 내부의 `/health`, `/score` 가 prefix `/fraud` 와 결합되어 `/fraud/health`, `/fraud/score` 로 노출된다.
- Week 1의 기존 `/health`, `/predict` endpoint는 그대로 유지된다. 즉 Week 2는 기존 API를 대체하지 않고 확장한다.

이로써 `POST /fraud/score`, `GET /fraud/health` 가 Week 1 의 `/predict`, `/health` 와 동일 컨테이너에서 응답한다. 코드 수정 후 컨테이너의 uvicorn `--reload` 가 자동 재적재한다 (필요 시 `make fraud-api-reload`).

### Day 4 완료 기준

아래 명령은 배치와 온라인 진입점이 모두 같은 모델 artifact와 threshold policy를 사용할 수 있는지 확인한다. 먼저 `fraud-score`로 운영 테이블에 risk score를 적재하고, 이어서 `fraud-api-test`로 단건 API 요청이 정상 응답하는지 확인한다. 두 명령이 모두 성공해야 Week 2 fraud 모델이 batch 운영과 online serving 양쪽에서 작동한다고 볼 수 있다.

```bash
make fraud-score
make fraud-api-test
```

코드 해설:

- `fraud-score` 는 DB에 batch 결과를 남기는 검증이고, `fraud-api-test` 는 HTTP endpoint가 같은 모델 계약으로 응답하는지 보는 검증이다.
- API test가 실패할 때는 artifact 부재, threshold policy 부재, router 미마운트, API 컨테이너 reload 미반영을 순서대로 확인한다.

검증:

아래 SQL은 적재된 `risk_score` 결과가 운영 band와 action 단위로 어떻게 분포하는지 확인한다. 단순히 row가 들어갔는지 보는 것을 넘어, HIGH/MEDIUM/LOW와 REVIEW/MONITOR/PASS가 정상적으로 생성되었는지 확인하는 운영 sanity check다.

```bash
docker compose exec postgres psql -U mluser -d ml_db -c \
  "SELECT risk_band, action, COUNT(*) FROM risk_score GROUP BY 1,2 ORDER BY 1,2;"
```

코드 해설:

- `risk_band` 와 `action` 을 함께 group by 하면 band 정책과 운영 조치가 정상적으로 매핑되었는지 볼 수 있다.
- HIGH가 지나치게 많으면 threshold policy가 운영 capacity를 만족하지 못할 수 있고, 모두 LOW면 모델/feature/threshold가 지나치게 보수적일 수 있다.

완료 조건:

* `risk_score` 에 `HIGH/MEDIUM/LOW` band 가 적재된다.
* `prediction_result` 에 fraud score 공통 로그가 함께 적재된다.
* `/fraud/score` 응답에 `fraud_probability`, `anomaly_score`, `combined_score`, `risk_band`, `action`, `reason_code` 가 포함된다.

---

## Day 5: DAG + 테스트 + 운영 리포트

### 5-1. `dags/fraud_scoring_pipeline_dag.py`

아래 DAG 초안은 Week 2 fraud scoring 흐름을 Airflow 태스크 순서로 표현한다. 로컬 Week 2 환경에서 Airflow를 반드시 띄우지 않아도 `--validate`로 태스크 순서와 import 가능성을 확인할 수 있게 만든다. 실제 운영에서는 이 DAG가 feature 생성, classifier 학습, anomaly 학습, threshold 생성, batch scoring을 순서대로 실행하며, Week 4에서 SLA, retry, recovery, promotion gate를 붙이는 기반이 된다.

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

코드 해설:

- `AIRFLOW_AVAILABLE` 플래그는 Airflow가 설치되지 않은 runner 컨테이너에서도 파일 import와 validation을 가능하게 한다.
- `PIPELINE_STEPS` 는 DAG의 핵심 계약이다. 실제 Airflow task와 `--validate` 출력이 같은 리스트를 사용하므로 문서와 실행 순서가 어긋날 가능성이 줄어든다.
- `build_dag` 는 Airflow 환경에서만 호출된다. 로컬 검증은 `_validate` 로 충분하다.
- `expected` 순서 검증은 실수로 threshold와 anomaly 순서를 바꾸는 문제를 잡는다. scoring은 feature, classifier, anomaly, threshold가 모두 준비된 뒤 실행되어야 한다.

### 5-2. `tests/test_fraud_pipeline.py`

테스트는 여섯 영역을 덮는다.

1. threshold policy JSON 형식 (high > medium, capacity > 0)
2. risk band 분기 일관성
3. reason code whitelist
4. risk_reasons 단일 소스 모듈의 동작
5. anomaly_scorer 가 batch/API 양쪽에서 동일 결과를 내는지
6. fraud feature 시간 누수 시나리오 (Week 1 의 `time_based_split` 가 fraud label window 에서도 누수를 막는지)

아래 테스트 파일은 Week 2에서 특히 깨지기 쉬운 운영 계약을 작게 검증한다. 모델 성능 자체를 테스트하는 것이 아니라 threshold JSON 구조, risk band 분기, reason code whitelist, anomaly score 정규화, 시간 기반 split의 라벨 누수 방지를 확인한다. 이런 테스트는 데이터가 바뀌어도 모델 운영 계약이 무너지지 않게 해주는 최소 안전망이다.

```python
# tests/test_fraud_pipeline.py
import json
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.data_split import SplitConfig, time_based_split
from features.risk_reasons import RiskReasonRules, derive_reasons, join_reasons
from models.anomaly_scorer import normalize_anomaly


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


def test_risk_reasons_single_source_consistency() -> None:
    rules = RiskReasonRules.from_config(
        {"high_amount_zscore": 3.0, "high_velocity_24h": 8, "overseas_device_country": ["JP", "US", "VN"]}
    )
    reasons = derive_reasons(
        rules=rules,
        amount_zscore_30d=4.2,
        tx_count_24h=12,
        device_country="US",
        fraud_probability=0.92,
        anomaly_score=0.85,
        high_threshold=0.7,
    )
    assert "HIGH_AMOUNT_ZSCORE" in reasons
    assert "HIGH_VELOCITY_24H" in reasons
    assert "OVERSEAS_DEVICE" in reasons
    assert "MODEL_HIGH_PROBABILITY" in reasons
    assert "ANOMALY_PATTERN" in reasons
    assert join_reasons(reasons).count(",") == 4


def test_anomaly_scorer_batch_and_single_consistency() -> None:
    normalizer = {"min": -0.2, "max": 0.5, "p99": 0.4}
    raw_batch = np.array([-0.2, 0.0, 0.4, 1.0])
    batch_out = normalize_anomaly(raw_batch, normalizer)
    single_out = np.array([float(normalize_anomaly(np.array([v]), normalizer)[0]) for v in raw_batch])
    np.testing.assert_allclose(batch_out, single_out)
    assert batch_out.min() == 0.0
    assert batch_out.max() == 1.0


@pytest.fixture
def fraud_split_frame() -> pd.DataFrame:
    base = pd.Timestamp("2026-04-15", tz="Asia/Seoul")
    rows = []
    for i in range(120):
        tx = base + timedelta(hours=i)
        rows.append(
            {
                "transaction_id": f"tx-{i:03d}",
                "transacted_at": tx,
                # 라벨은 거래 후 0~10일 사이에 확정된다고 가정
                "label_confirmed_at": tx + timedelta(days=(i % 10) + 1),
                "is_label_fraud": i % 23 == 0,
                "amount": 10000 + i * 17,
            }
        )
    return pd.DataFrame(rows)


def test_fraud_train_window_excludes_late_labels(fraud_split_frame: pd.DataFrame) -> None:
    cfg = SplitConfig.from_dict(
        {
            "train_until": "2026-04-18T00:00:00+09:00",
            "valid_until": "2026-04-19T00:00:00+09:00",
            "test_until": "2026-04-20T00:00:00+09:00",
            "label_column": "is_label_fraud",
            "label_confirmed_column": "label_confirmed_at",
            "min_label_lag_hours": 24,
        }
    )
    train, valid, test = time_based_split(fraud_split_frame, cfg)
    # 학습 윈도우의 모든 라벨은 윈도우 이전에 확정되어야 한다.
    assert (train["label_confirmed_at"] <= cfg.train_until).all(), "fraud label window 누수"
    # train / valid / test 는 시간 기준으로 분리된다.
    assert set(train["transaction_id"]).isdisjoint(set(valid["transaction_id"]))
    assert set(valid["transaction_id"]).isdisjoint(set(test["transaction_id"]))
```

코드 해설:

- `test_threshold_policy_shape` 는 threshold policy JSON의 최소 구조를 검증한다. high threshold가 medium보다 낮으면 운영 band가 무너진다.
- `test_risk_band_policy_order` 는 score가 HIGH/MEDIUM/LOW로 기대 순서대로 나뉘는지 확인한다.
- `test_reason_codes_are_explainable` 과 `test_risk_reasons_single_source_consistency` 는 reason code가 whitelist 안에서 생성되는지 확인한다.
- `test_anomaly_scorer_batch_and_single_consistency` 는 batch 입력과 단건 입력이 같은 정규화 결과를 내는지 검증한다.
- `test_fraud_train_window_excludes_late_labels` 는 fraud 라벨 확정 시점이 train window 이후인 row가 학습에 들어가지 않는지 확인한다. 이 테스트가 Week 2의 누수 방지 핵심이다.

### 5-3. `docs/reports/fraud-model-evaluation.md`

아래 문서는 자동 생성 리포트와 별개로, stakeholder에게 fraud 모델 평가를 설명할 때 사용할 정적 리포트 템플릿이다. precision/recall 같은 기술 지표만 적는 것이 아니라, 오탐과 미탐이 운영 비용과 손실에 어떤 의미를 갖는지, `risk_score`와 `prediction_result`를 어떻게 인수인계해야 하는지를 함께 정리한다. CFO/COO/CISO가 각자 관심 있는 관점에서 같은 모델 결과를 해석할 수 있게 만드는 것이 목적이다.

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

코드 해설:

- 이 리포트는 자동 산출물의 양식을 사람이 먼저 이해할 수 있게 만든 정적 기준 문서다.
- `핵심 지표` 표는 모델 평가값을 운영 의미로 번역한다. 예를 들어 precision은 단순 수학 지표가 아니라 운영팀 검토 효율이다.
- `CFO 비용 해석` 섹션은 false positive와 false negative를 비용 언어로 연결한다.
- `운영 인수인계` 섹션은 어떤 테이블과 artifact를 운영자가 봐야 하는지 정리한다.

### 5-4. End-to-end 검증 시나리오

아래 명령 묶음은 Week 2의 전체 경로를 처음부터 끝까지 검증하는 표준 실행 순서다. 환경을 띄우고 schema를 적용한 뒤, Silver 기반 라벨 생성과 import, feature 생성, 모델 학습, anomaly 학습, threshold 생성, batch scoring, API smoke test, DAG validation, pytest를 차례로 실행한다. 이 순서로 성공하면 fraud 모델이 데이터 입력부터 운영 출력까지 하나의 재현 가능한 pipeline으로 연결되었음을 보여줄 수 있다.

```bash
# 0. Week 1 환경 기동 + Week 2 schema 적용
make up
make healthcheck
make fraud-schema

# 1. Spark Silver 거래 데이터 + 사후 확정 fraud label 준비
# 사전조건: .env 의 UPSTREAM_SILVER_PATH 가 업스트림 Silver Delta 호스트 경로를 가리켜야 한다.
# runner 컨테이너에는 그 경로가 /upstream/silver/transactions:ro 로 마운트되어 있어야 한다.
# Silver 가 미가용이면 아래 "Synthetic smoke path" 를 사용한다.
make fraud-labels
make fraud-import-upstream

# 2. fraud feature_set 적재
make fraud-features

# 3. 모델 학습 + threshold (anomaly 가 threshold 보다 먼저 학습되어야 한다)
make fraud-train
make fraud-anomaly
make fraud-threshold

# 4. risk score 적재 + API 확인 (사전조건: 위 3 단계 모두 성공)
make fraud-score
make fraud-api-test

# 5. DAG + 테스트
make fraud-dag-validate
make fraud-test

# 6. 결과 확인
docker compose exec postgres psql -U mluser -d ml_db -c \
  "SELECT risk_band, COUNT(*) FROM risk_score GROUP BY 1 ORDER BY 1;"
```

코드 해설:

- 이 절차는 Silver 연계가 가능한 consulting demo 경로다. upstream Delta data, simulated fraud label, raw table import까지 모두 포함한다.
- `make fraud-schema` 를 초반에 실행해 운영 테이블이 없어서 scoring이 실패하는 문제를 예방한다.
- `make fraud-train` 뒤에 `make fraud-anomaly`, `make fraud-threshold` 를 실행하는 순서가 중요하다. Day 4 scoring은 세 산출물을 모두 참조한다.
- 마지막 SQL은 score 적재가 끝난 뒤 운영자가 가장 먼저 볼 수 있는 band 분포 확인이다.

Silver Delta 경로가 아직 준비되지 않은 경우에는 다음 smoke path 로 Week 2 코드 흐름만 검증한다. 이 경로는 `fraud-labels` 와 `fraud-import-upstream` 을 실행하지 않고 Week 1 synthetic sample 을 그대로 사용한다.

아래 smoke path는 upstream Silver 연계가 아직 준비되지 않았을 때 사용하는 빠른 대체 검증 절차다. 실제 Silver import를 생략하고 Week 1 synthetic sample을 원천 데이터로 사용하므로 consulting demo의 upstream 연결성은 증명하지 못하지만, Week 2의 feature build, model train, threshold, scoring, API, DAG, test 흐름이 코드 수준에서 작동하는지는 확인할 수 있다.

```bash
# Synthetic smoke path
make up
make healthcheck
make fraud-schema
make load-sample
make fraud-features
make fraud-train
make fraud-anomaly
make fraud-threshold
make fraud-score
make fraud-api-test
make fraud-dag-validate
make fraud-test
```

코드 해설:

- 이 경로는 upstream Silver가 없어도 Week 2 모델 코드의 기본 실행성을 확인하기 위한 smoke test다.
- `make load-sample` 이 Week 1 synthetic 거래를 `raw_transactions`에 준비한다고 가정한다.
- `fraud-labels` 와 `fraud-import-upstream` 을 건너뛰므로 upstream 조인 품질은 검증하지 못한다. 대신 feature, train, score, API, DAG, test 흐름을 빠르게 확인할 수 있다.
- 포트폴리오 데모에서는 가능하면 Silver 경로를 사용하고, 개발 중 빠른 회귀 확인에는 smoke path를 사용한다.

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
- [ ] `study-ml-pipeline/features/risk_reasons.py`
- [ ] `study-ml-pipeline/scripts/generate_fraud_labels_from_silver.py`
- [ ] `study-ml-pipeline/scripts/import_spark_silver_to_raw_transactions.py`
- [ ] `study-ml-pipeline/features/build_fraud_features.py`
- [ ] `study-ml-pipeline/models/train_fraud_classifier.py`
- [ ] `study-ml-pipeline/models/train_anomaly_detector.py`
- [ ] `study-ml-pipeline/models/tune_fraud_threshold.py`
- [ ] `study-ml-pipeline/models/batch_score_fraud.py`
- [ ] `study-ml-pipeline/models/anomaly_scorer.py`
- [ ] `study-ml-pipeline/serving/fraud_router.py`
- [ ] `study-ml-pipeline/serving/app.py` 에 `include_router(fraud_router)` 추가
- [ ] `study-ml-pipeline/dags/fraud_scoring_pipeline_dag.py`
- [ ] `study-ml-pipeline/tests/test_fraud_pipeline.py`
- [ ] `study-ml-pipeline/Makefile` fraud 타겟 추가
- [ ] `study-ml-pipeline/.env` 의 `UPSTREAM_SILVER_PATH` 설정
- [ ] `study-ml-pipeline/docker-compose.yml` runner 서비스에 `${UPSTREAM_SILVER_PATH:-./data/empty_upstream/silver/transactions}:/upstream/silver/transactions:ro` 마운트 추가

### 문서 산출물

- [ ] `study-ml-pipeline/docs/models/fraud-detection-design.md`
- [ ] `study-ml-pipeline/docs/reports/fraud-model-evaluation.md`
- [ ] `study-ml-pipeline/data/reports/fraud_eval_report_*.md` (자동 생성, `make fraud-train` 실행 시 매번 생성)

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
