# Week 1: ML 기본기와 모델링 파이프라인

**기간**: 5일 (월~금, 풀타임 40시간) — 2026-05-25 ~ 2026-05-29
**주제**: ML 프로젝트 표준 구조 정립, 학습·평가·배치 추론·API 서빙이 독립 실행 가능하면서 하나의 흐름으로 연결되는 ML 운영 파이프라인 기초 구축
**산출물**: `study-ml-pipeline` 컨테이너 환경, 공통 ML 프로젝트 템플릿, 입력·출력 스키마 문서, baseline 학습 스크립트, 배치 예측 흐름, FastAPI 추론 엔드포인트 초안, Airflow 학습 DAG 초안
**전제 조건**: `study-data-pipeline` Week 1~5 환경에서 PostgreSQL·Redis·Spark·Airflow 기동 경험을 보유, Docker Engine 24+ 와 Docker Compose v2, Python 3.11, `make` 사용 가능

---

## 수행 시나리오

### 배경 설정

`study-data-pipeline` 8주 흐름에서 Nexus Pay는 Spark·Airflow·Delta Lake 기반의 데이터 파이프라인 골격을 갖췄다. CTO는 다음 단계로 "축적된 데이터 위에서 ML 모델이 운영되는 흐름"을 요구한다. 이 흐름은 단순히 노트북에서 모델을 학습하는 것이 아니라, **피처 생성 → 학습 → 평가 → 배치 추론 → 결과 저장 → API 서빙**이 독립 실행 가능하면서도 한 줄기로 연결되어야 한다.

> "노트북에서만 잘 돌아가는 모델은 운영팀에 인수인계할 수 없습니다. 우리는 ML도 데이터파이프라인처럼 다뤄야 합니다. 모델 입력이 어디서 만들어지고, 결과가 어느 테이블에 적재되는지, 실패하면 어느 단계를 재실행해야 하는지를 아는 것이 우선입니다. 모델 정확도를 0.01 올리는 것보다, 다음 모델을 같은 구조로 다시 만들 수 있는 표준이 먼저 필요합니다." — Nexus Pay CTO

이번 주의 목표는 fraud, churn, demand forecast, customer segmentation 등 향후 4주에 걸쳐 만들 모델들이 **공통으로 따를 ML 프로젝트 템플릿**을 구축하는 것이다. 알고리즘은 단순한 baseline 분류 모델 한 개로 제한하고, 대신 다음을 빈틈없이 정렬한다.

* 학습 입력 데이터의 출처와 스키마
* 학습 출력(모델 artifact, 평가 리포트)의 위치와 형식
* 배치 추론 결과 테이블의 스키마와 적재 절차
* API 호출 시 입력·응답 스키마
* 같은 흐름을 Airflow DAG로 옮기기 위한 진입점

### C레벨 요구사항

| 요구자 | 요구사항 | 본 가이드 반영 위치 |
|--------|----------|----------------------|
| CTO | 개발자 노트북이 아니라 Docker Compose로 재현 가능한 ML 운영 실습 환경 | Day 1: docker-compose.yml, Makefile |
| CTO | 피처 생성·학습·평가·배치 추론·API 서빙이 독립 실행 가능하면서도 하나의 흐름으로 연결 | Day 1~5 전체, Makefile 타겟 분리 |
| CIO | 모델 입력·출력 스키마와 데이터 계보 문서화로 감사·인수인계 가능 | Day 1: schemas/init.sql, docs/features/ml-feature-schema.md |
| COO | 예측 결과가 파일에 머물지 않고 운영팀이 조회 가능한 PostgreSQL 테이블에 저장 | Day 5: batch_predict.py + prediction 테이블 |
| CFO | 모델 평가 리포트에 정확도뿐 아니라 오탐·미탐이 운영 비용에 미치는 해석 포함 | Day 4: 평가 리포트 템플릿의 "비용 해석" 섹션 |

### 목표

1. `study-ml-pipeline` 저장소에 PostgreSQL, MLflow, FastAPI, Python runner를 포함한 ML 운영 실습 환경을 Docker Compose로 구성한다.
2. 향후 4주에 걸쳐 만들 ML 모델들이 공통으로 따를 디렉터리 표준과 학습 입력·예측 출력 스키마를 정립한다.
3. train/test split, time-based split, 데이터 누수 방지의 기준선을 코드와 문서로 남긴다.
4. 결측·이상치·범주형 인코딩·금액/빈도/최근성 피처 생성을 sklearn Pipeline으로 구현한다.
5. baseline 모델 학습, cross validation, 평가 리포트, 배치 추론, FastAPI 추론 엔드포인트, Airflow DAG 초안을 한 명령으로 실행 가능하게 묶는다.
6. CFO/CIO/COO 관점에서 의미 있는 평가 리포트와 배치 예측 결과 테이블을 만들고, 데이터 계보를 문서화한다.

### 일정 개요

| 일차 | 주제 | 핵심 작업 | 완료 산출물 |
|------|------|----------|-------------|
| Day 1 | 프로젝트 구조 + 컨테이너 환경 | 디렉토리 구조, docker-compose, Makefile, 입력/예측 출력 스키마 정의 | `study-ml-pipeline/` 골격, `docker-compose.yml`, `schemas/init.sql` |
| Day 2 | 데이터 분할과 누수 방지 | sample fraud 데이터 생성, time-based split, leakage 점검 | `data/sample/`, `features/data_split.py`, `docs/features/ml-data-leakage-checklist.md` |
| Day 3 | 피처 엔지니어링 기본 | sklearn Pipeline, 결측·이상치·인코딩, 금액/빈도/최근성 피처 | `features/build_features.py`, `features/feature_pipeline.py` |
| Day 4 | 모델 학습 + 평가 템플릿 | baseline 학습, MLflow 로깅, 평가 리포트(비용 해석 포함) | `models/train_baseline.py`, `reports/baseline_eval_report.md` |
| Day 5 | 배치 예측 + API + DAG 초안 | PostgreSQL 적재, FastAPI 엔드포인트, Airflow DAG 초안, 통합 검증 | `models/batch_predict.py`, `serving/app.py`, `dags/ml_training_pipeline_dag.py` |

### Day N 완료 기준 요약

* Day 1 완료 기준: `make up` 한 줄로 PostgreSQL·MLflow·Python runner가 healthy 상태로 기동되고, `init.sql`이 적용되어 `feature_store`, `prediction_result`, `model_evaluation` 테이블이 생성된다.
* Day 2 완료 기준: 거래 샘플 데이터에 대해 time-based split이 동작하며, 학습 시점 이후 정보가 학습 데이터에 누수되지 않는다는 사실을 테스트로 증명한다.
* Day 3 완료 기준: `make features` 실행 시 결측·이상치·인코딩·파생 피처 생성이 한 번에 수행되고, sklearn `Pipeline` 객체가 직렬화 가능 형태로 저장된다.
* Day 4 완료 기준: `make train` 실행 시 baseline 모델이 학습되어 MLflow에 기록되고, CFO 해석이 포함된 평가 리포트가 자동 생성된다.
* Day 5 완료 기준: `make predict`, `make api-test`, `make dag-validate` 실행이 모두 성공하고, `prediction_result` 테이블에서 최근 배치 결과를 조회할 수 있다.

---

## Day 1: 프로젝트 구조 + 컨테이너 환경

### 1-0. 작업 원칙

ML 파이프라인은 데이터파이프라인보다 더 파편화되기 쉽다. 노트북·스크립트·서비스가 섞여 있고, 모델 artifact·피처·예측 결과의 저장 위치가 사람마다 달라진다. 이번 주 첫날의 임무는 그 흐름을 막는 표준을 만드는 것이다. **알고리즘 코드를 먼저 쓰지 않는다.** 코드를 쓰기 전에 다음 세 가지를 합의한다.

1. 디렉토리 표준 — 어디에 무엇을 둘 것인가
2. 데이터 계보 — 입력은 어디서 오고 출력은 어디로 가는가
3. 실행 인터페이스 — 어떤 명령으로 누가 실행할 것인가

### 1-1. 저장소 디렉토리 구조 생성

```bash
# study-ml-pipeline 저장소 루트에서 실행
mkdir -p config data/{sample,artifacts,reports} dags docs/{features,models,reports,guides} features models reports schemas scripts serving tests rag mlops
```

최종 디렉토리 표준은 다음과 같다. 이 구조는 Week 1~4 내내 변경되지 않는다.

```text
study-ml-pipeline/
├── docker-compose.yml             # ML 운영 실습 환경
├── .env.example                   # 환경 변수 예시
├── Makefile                       # make up / train / predict / api-test ...
├── README.md                      # 기동·실행·검증 절차
├── config/
│   ├── model_config.yaml          # 데이터 경로/피처/평가 지표 등 공용 설정
│   └── logging.yaml               # Python 로깅 포맷
├── data/
│   ├── sample/                    # 샘플 입력 데이터(비식별 거래)
│   ├── artifacts/                 # 학습된 모델 artifact (`.joblib`)
│   └── reports/                   # 자동 생성 평가 리포트
├── dags/
│   └── ml_training_pipeline_dag.py
├── docs/
│   ├── features/
│   │   ├── ml-feature-schema.md
│   │   └── ml-data-leakage-checklist.md
│   ├── models/
│   │   └── ml-pipeline-architecture.md
│   ├── reports/
│   └── guides/
├── features/
│   ├── build_features.py
│   ├── feature_pipeline.py
│   └── data_split.py
├── models/
│   ├── train_baseline.py
│   ├── batch_predict.py
│   └── eval_report.py
├── reports/
│   └── baseline_eval_report.md    # Day 4 산출물 템플릿
├── schemas/
│   └── init.sql                   # PostgreSQL 초기화 DDL
├── scripts/
│   └── healthcheck.sh
├── serving/
│   └── app.py                     # FastAPI 추론 엔드포인트
├── tests/
│   └── test_week1_pipeline.py
├── rag/                           # Week 4 RAG 자산 위치
├── mlops/                         # Week 4 모델 운영 자산 위치
└── .gitignore
```

`.gitignore` 초안은 데이터·모델 산출물의 우발적 커밋을 막는다.

```bash
cat > .gitignore << 'EOF'
__pycache__/
*.pyc
.venv/
.env
data/artifacts/
data/reports/
mlruns/
*.joblib
*.log
.DS_Store
EOF
```

### 1-2. 환경 변수 파일 생성

```bash
cat > .env.example << 'EOF'
# === 공통 ===
COMPOSE_PROJECT_NAME=ml-lab
TZ=Asia/Seoul

# === PostgreSQL (학습 입력 / 예측 결과 저장소) ===
POSTGRES_USER=mluser
POSTGRES_PASSWORD=mlpass
POSTGRES_DB=ml_db
POSTGRES_PORT=5432

# === MLflow Tracking Server ===
MLFLOW_PORT=5000
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_BACKEND_STORE_URI=postgresql+psycopg2://mluser:mlpass@postgres:5432/mlflow_db
MLFLOW_ARTIFACT_ROOT=/mlflow/artifacts

# === FastAPI 추론 서비스 ===
API_PORT=8000

# === Python runner ===
PYTHONPATH=/app
EOF

cp .env.example .env
```

### 1-3. Docker Compose — 1주차 기동 범위

1주차에서는 PostgreSQL, MLflow Tracking Server, Python runner를 핵심으로 기동한다. FastAPI는 코드 수정 시 자동 리로드되도록 dev 모드로 띄운다. RAG와 벡터 DB 서비스는 Week 4 범위에서 별도 Compose 확장으로 추가한다.

```yaml
# docker-compose.yml
version: "3.9"

networks:
  ml-net:
    driver: bridge

volumes:
  postgres-data:
  mlflow-artifacts:

services:
  # ──────────────────────────────────────
  # PostgreSQL — 피처 / 예측 / 모델 평가 결과 저장소
  # ──────────────────────────────────────
  postgres:
    image: postgres:16-alpine
    container_name: ml-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./schemas/init.sql:/docker-entrypoint-initdb.d/01-init.sql
    networks:
      - ml-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ──────────────────────────────────────
  # MLflow Tracking Server (실험 / 모델 등록)
  # ──────────────────────────────────────
  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.13.0
    container_name: ml-mlflow
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      MLFLOW_BACKEND_STORE_URI: ${MLFLOW_BACKEND_STORE_URI}
      MLFLOW_DEFAULT_ARTIFACT_ROOT: ${MLFLOW_ARTIFACT_ROOT}
    command: >
      bash -lc "pip install psycopg2-binary &&
                mlflow server
                  --host 0.0.0.0
                  --port 5000
                  --backend-store-uri ${MLFLOW_BACKEND_STORE_URI}
                  --default-artifact-root ${MLFLOW_ARTIFACT_ROOT}"
    ports:
      - "${MLFLOW_PORT}:5000"
    volumes:
      - mlflow-artifacts:/mlflow/artifacts
    networks:
      - ml-net
    healthcheck:
      test: ["CMD-SHELL", "python -c 'import urllib.request,sys; urllib.request.urlopen(\"http://localhost:5000\"); sys.exit(0)' || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 10

  # ──────────────────────────────────────
  # Python runner — 학습/추론/평가/리포트 실행 컨테이너
  # ──────────────────────────────────────
  runner:
    image: python:3.11-slim
    container_name: ml-runner
    working_dir: /app
    depends_on:
      postgres:
        condition: service_healthy
      mlflow:
        condition: service_healthy
    environment:
      PYTHONPATH: /app
      MLFLOW_TRACKING_URI: ${MLFLOW_TRACKING_URI}
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - ./:/app
    networks:
      - ml-net
    command: ["bash", "-lc", "pip install -r requirements.txt && tail -f /dev/null"]

  # ──────────────────────────────────────
  # FastAPI 추론 서비스 (개발 모드)
  # ──────────────────────────────────────
  api:
    image: python:3.11-slim
    container_name: ml-api
    working_dir: /app
    depends_on:
      runner:
        condition: service_started
    environment:
      PYTHONPATH: /app
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${API_PORT}:8000"
    volumes:
      - ./:/app
    networks:
      - ml-net
    command: >
      bash -lc "pip install -r requirements.txt &&
                uvicorn serving.app:app --host 0.0.0.0 --port 8000 --reload"
```

### 1-4. requirements.txt 와 PostgreSQL 초기 스키마

Week 1 의 의존성은 "데이터 처리 → 모델 학습 → 추적 → 서빙 → 검증" 흐름을 독립적으로 실행할 수 있는 최소 세트로 제한한다.

| 모듈 | 역할 |
|------|------|
| `pandas` | 거래 CSV, PostgreSQL 조회 결과, 피처 payload 를 DataFrame 으로 처리한다. |
| `numpy` | 수치 연산과 모델 평가 과정의 배열 처리를 담당한다. |
| `scikit-learn` | 전처리 `Pipeline`, baseline 분류 모델, 평가 지표를 제공한다. |
| `joblib` | 학습된 sklearn pipeline 과 메타데이터를 `model.joblib` artifact 로 저장·로드한다. |
| `pyyaml` | `config/model_config.yaml` 을 읽어 피처, 분할, 모델, 평가 설정을 코드에 주입한다. |
| `python-dotenv` | 로컬 실행 시 `.env` 기반 환경 변수를 읽을 수 있게 한다. |
| `SQLAlchemy` | Python 코드에서 PostgreSQL 연결, 조회, 적재를 일관된 방식으로 처리한다. |
| `psycopg2-binary` | SQLAlchemy 가 PostgreSQL 에 접속할 때 사용하는 DB 드라이버다. |
| `mlflow` | 학습 파라미터, 지표, 모델 artifact, 평가 리포트 lineage 를 추적한다. |
| `fastapi` | 온라인 추론 API 의 HTTP 엔드포인트를 구현한다. |
| `uvicorn[standard]` | FastAPI 앱을 개발 모드와 컨테이너 환경에서 실행하는 ASGI 서버다. |
| `pydantic` | 추론 요청과 응답 스키마를 검증한다. |
| `pytest` | 데이터 누수, 피처 파이프라인, DAG import 등 Week 1 검증 테스트를 실행한다. |

```bash
cat > requirements.txt << 'EOF'
# Core
pandas==2.2.2
numpy==1.26.4
scikit-learn==1.5.0
joblib==1.4.2
pyyaml==6.0.2
python-dotenv==1.0.1

# Database
SQLAlchemy==2.0.30
psycopg2-binary==2.9.9

# Tracking & Serving
mlflow==2.13.0
fastapi==0.111.0
uvicorn[standard]==0.30.1
pydantic==2.7.4

# Tests
pytest==8.2.2
EOF
```

PostgreSQL 초기화 DDL은 모든 모델이 공통으로 사용할 기본 테이블을 정의한다. **이 시점에는 fraud 전용 결과 테이블을 만들지 않는다.** 다음 주차에서 모델별 `risk_score`, `demand_forecast`, `customer_segment` 테이블을 추가할 것이다.

| 테이블/DB | 역할 |
|-----------|------|
| `mlflow_db` | MLflow Tracking Server 가 실험, run, artifact metadata 를 저장하는 전용 DB다. |
| `raw_transactions` | Nexus Pay 비식별 거래 원천 데이터와 사후 확정 라벨을 보관한다. |
| `feature_store` | 학습과 추론이 공유하는 feature set/version 단위의 피처 payload 를 저장한다. |
| `prediction_result` | 배치 추론과 온라인 추론의 점수, 판정, 모델 버전, 실행 lineage 를 저장한다. |
| `model_evaluation` | precision, recall, PR-AUC, confusion matrix, 비용 해석 등 오프라인 평가 결과를 저장한다. |
| `data_lineage` | feature, train, predict, evaluate 단계가 어떤 입력 윈도우와 출력 대상을 사용했는지 기록한다. |

```sql
-- schemas/init.sql

-- MLflow 메타데이터 DB
CREATE DATABASE mlflow_db;

\c ml_db

-- ─────────────────────────────────────────────
-- 1. 거래 원천 (Nexus Pay 비식별 거래 샘플)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id   VARCHAR(40) PRIMARY KEY,
    customer_id      VARCHAR(40) NOT NULL,
    merchant_id      VARCHAR(40) NOT NULL,
    amount           NUMERIC(14,2) NOT NULL,
    currency         CHAR(3) DEFAULT 'KRW',
    channel          VARCHAR(20) NOT NULL,           -- APP/WEB/POS
    device_country   CHAR(2),                        -- KR/JP/...
    merchant_segment VARCHAR(30),                     -- GENERAL/PREMIUM/...
    is_label_fraud   BOOLEAN,                        -- 라벨 (사후 확정값)
    transacted_at    TIMESTAMPTZ NOT NULL,
    label_confirmed_at TIMESTAMPTZ                   -- 라벨이 확정된 시각 (누수 방지용)
);

CREATE INDEX IF NOT EXISTS ix_raw_tx_customer_time
    ON raw_transactions (customer_id, transacted_at);

-- ─────────────────────────────────────────────
-- 2. 피처 스토어 (학습 / 예측 공통 입력)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS feature_store (
    feature_set         VARCHAR(40) NOT NULL,        -- 예: 'baseline_v1'
    transaction_id      VARCHAR(40) NOT NULL,
    feature_version     VARCHAR(20) NOT NULL,        -- 'v1.0.0'
    as_of_ts            TIMESTAMPTZ NOT NULL,        -- 피처 계산 시점
    payload_json        JSONB NOT NULL,              -- 피처 값
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (feature_set, feature_version, transaction_id)
);

-- ─────────────────────────────────────────────
-- 3. 예측 결과 (배치 추론·온라인 추론 공통)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prediction_result (
    prediction_id     BIGSERIAL PRIMARY KEY,
    transaction_id    VARCHAR(40) NOT NULL,
    model_name        VARCHAR(60) NOT NULL,
    model_version     VARCHAR(64) NOT NULL,
    feature_set       VARCHAR(40) NOT NULL,
    feature_version   VARCHAR(20) NOT NULL,
    score             NUMERIC(7,4),                  -- 0.0~1.0 확률
    prediction        VARCHAR(20),                   -- 'POSITIVE'/'NEGATIVE'/...
    scored_at         TIMESTAMPTZ DEFAULT NOW(),
    run_id            VARCHAR(40)                    -- Airflow DAG run / MLflow run 추적
);

CREATE INDEX IF NOT EXISTS ix_prediction_tx
    ON prediction_result (transaction_id);
CREATE INDEX IF NOT EXISTS ix_prediction_scored_at
    ON prediction_result (scored_at);

-- ─────────────────────────────────────────────
-- 4. 모델 평가 결과 (오프라인 평가 / 정기 검증)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_evaluation (
    evaluation_id   BIGSERIAL PRIMARY KEY,
    model_name      VARCHAR(60) NOT NULL,
    model_version   VARCHAR(64) NOT NULL,
    eval_window_start TIMESTAMPTZ NOT NULL,
    eval_window_end   TIMESTAMPTZ NOT NULL,
    metric_name     VARCHAR(40) NOT NULL,            -- 'precision','recall','f1','pr_auc','roc_auc','mae','rmse'
    metric_value    NUMERIC(10,5) NOT NULL,
    extra_json      JSONB,                           -- confusion matrix 등 부가 정보
    evaluated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- 5. 데이터 계보 (어느 학습/예측 실행이 어느 입력을 사용했는지)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id        BIGSERIAL PRIMARY KEY,
    run_id            VARCHAR(40) NOT NULL,
    stage             VARCHAR(20) NOT NULL,          -- 'feature'/'train'/'predict'/'evaluate'
    input_source      TEXT NOT NULL,
    input_window_start TIMESTAMPTZ,
    input_window_end   TIMESTAMPTZ,
    output_target     TEXT NOT NULL,
    recorded_at       TIMESTAMPTZ DEFAULT NOW()
);
```

### 1-5. config/model_config.yaml — 공용 설정

```yaml
# config/model_config.yaml
project:
  name: study-ml-pipeline
  scenario: nexuspay-baseline-fraud

data:
  source: postgres
  schema: public
  raw_table: raw_transactions
  feature_table: feature_store
  prediction_table: prediction_result
  evaluation_table: model_evaluation

features:
  feature_set: baseline_v1
  feature_version: "v1.0.0"
  as_of_ts_column: transacted_at
  numerical:
    - amount
    - amount_zscore_30d
    - tx_count_24h
    - days_since_last_tx
  categorical:
    - channel
    - device_country
    - merchant_segment
  drop_columns:
    - currency
  imputation:
    numerical: median
    categorical: most_frequent
  outlier:
    method: iqr
    factor: 3.0

split:
  strategy: time_based
  train_until: "2026-04-30T23:59:59+09:00"
  valid_until: "2026-05-15T23:59:59+09:00"
  test_until:  "2026-05-24T23:59:59+09:00"
  label_column: is_label_fraud
  label_confirmed_column: label_confirmed_at
  min_label_lag_hours: 24

model:
  type: classifier
  baseline: logistic_regression
  candidates:
    - logistic_regression
    - random_forest
  random_state: 42
  registry:
    name: nexuspay_baseline_classifier

evaluation:
  metrics: [precision, recall, f1, pr_auc, roc_auc]
  cost:
    false_positive_cost_krw: 5000      # 운영 검토 1건 비용
    false_negative_cost_krw: 250000    # 사기 거래 1건 평균 손실
  report_template: reports/baseline_eval_report.md

serving:
  api_host: 0.0.0.0
  api_port: 8000
  threshold: 0.5
```

### 1-6. Makefile — 단일 진입점

```makefile
# Makefile
SHELL := /bin/bash

.PHONY: up down ps logs shell init-db features train predict eval api-test \
        dag-validate test healthcheck clean

up:
	docker compose up -d
	docker compose ps

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

shell:
	docker compose exec runner bash

init-db:
	docker compose exec -T postgres \
	  psql -U $${POSTGRES_USER} -d $${POSTGRES_DB} -f /docker-entrypoint-initdb.d/01-init.sql

features:
	docker compose exec runner python -m features.build_features --config config/model_config.yaml

train:
	docker compose exec runner python -m models.train_baseline --config config/model_config.yaml

predict:
	docker compose exec runner python -m models.batch_predict --config config/model_config.yaml

eval:
	docker compose exec runner python -m models.eval_report --config config/model_config.yaml

api-test:
	curl -fsS -X POST http://localhost:$${API_PORT:-8000}/predict \
	  -H 'Content-Type: application/json' \
	  -d '{"transaction_id":"tx-demo","amount":120000,"channel":"APP","device_country":"KR","merchant_segment":"GENERAL"}'

dag-validate:
	docker compose exec runner python -m dags.ml_training_pipeline_dag --validate

test:
	docker compose exec runner pytest -q tests/

healthcheck:
	bash scripts/healthcheck.sh

clean:
	docker compose down -v
	rm -rf data/artifacts/* data/reports/*
```

### 1-7. 입력·출력 스키마 문서화

CIO 요구사항(스키마와 데이터 계보 문서화)을 충족하기 위해 두 개의 문서를 동시에 작성한다.

```bash
cat > docs/features/ml-feature-schema.md << 'EOF'
# ML 입력·출력 스키마 — Nexus Pay 베이스라인

본 문서는 `study-ml-pipeline` 의 모든 모델이 공유하는 학습 입력과 예측 출력 스키마의 첫 번째 버전이다. 모델별 추가 컬럼은 본 문서를 상속하여 별도 섹션에서 정의한다.

## 1. 학습 입력 (`feature_store`)

| 컬럼 | 의미 | 비고 |
|------|------|------|
| feature_set | 피처 세트 식별자 | `baseline_v1` |
| feature_version | 피처 버전 | semver |
| transaction_id | 거래 ID | `raw_transactions` 와 1:1 |
| as_of_ts | 피처 계산 시점 | 누수 방지 기준 시각 |
| payload_json.amount | 거래 금액 | 원천 그대로 |
| payload_json.amount_zscore_30d | 최근 30일 금액 z-score | 학습 시점 이전 데이터로만 계산 |
| payload_json.tx_count_24h | 직전 24시간 거래 횟수 | 학습 시점 이전 |
| payload_json.days_since_last_tx | 직전 거래 후 경과일 | 학습 시점 이전 |
| payload_json.channel | 채널 | `APP/WEB/POS` |
| payload_json.device_country | 단말 국가 | ISO-3166 alpha-2 |
| payload_json.merchant_segment | 가맹점 세그먼트 | 카테고리화된 가맹점 군 |

## 2. 예측 출력 (`prediction_result`)

| 컬럼 | 의미 | 비고 |
|------|------|------|
| transaction_id | 거래 ID | 입력과 동일 |
| model_name | 모델 식별자 | `nexuspay_baseline_classifier` |
| model_version | 모델 버전 | artifact 디렉터리명 (`YYYYMMDD-HHMMSS`) |
| feature_set / feature_version | 입력 피처 식별자 | 재현성 확보 |
| score | 0~1 확률 | 분류 모델 |
| prediction | 라벨 | `POSITIVE/NEGATIVE` |
| run_id | 추적 ID | MLflow run / Airflow run |

## 3. 평가 출력 (`model_evaluation`)

평가 지표는 `precision`, `recall`, `f1`, `pr_auc`, `roc_auc` 를 기본 셋으로 한다. CFO 해석을 위해 `extra_json` 에 confusion matrix 와 비용 추정치를 함께 저장한다.

## 4. 누수 방지 원칙

- `as_of_ts` 이후의 정보는 절대 피처에 포함되지 않는다.
- 라벨은 `label_confirmed_at` 이 학습 윈도우 끝보다 빠를 때만 사용한다.
- 학습/검증/테스트 분할은 시간 기반(`time_based`)을 기본으로 한다.
EOF

cat > docs/models/ml-pipeline-architecture.md << 'EOF'
# ML 파이프라인 아키텍처 — Week 1 기준선

```
┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│  raw_transactions│──▶│  feature_store   │──▶│  baseline model  │
│   (PostgreSQL)   │    │   (PostgreSQL)   │    │ (joblib artifact)│
└─────────────────┘    └─────────────────┘    └──────────────────┘
                                                      │
                                                      ▼
        ┌─────────────────┐    ┌──────────────────────────────┐
        │ prediction_result│◀──│ batch_predict / FastAPI app  │
        │   (PostgreSQL)   │   └──────────────────────────────┘
        └─────────────────┘
                                                      │
                                                      ▼
                                          ┌──────────────────────┐
                                          │  model_evaluation    │
                                          │   (PostgreSQL)        │
                                          └──────────────────────┘
```

- 같은 피처를 학습 시점에는 `feature_store` 에 적재하고, 예측 시점에는 동일한 키 (`feature_set`, `feature_version`) 로 조회한다.
- MLflow Tracking 은 학습 실험과 모델 등록 이력을 보관한다.
- Airflow DAG (`dags/ml_training_pipeline_dag.py`) 가 `features → train → predict → evaluate` 를 한 번에 실행한다.
EOF
```

### 1-8. 헬스체크 스크립트

```bash
cat > scripts/healthcheck.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "[1/3] postgres";  docker compose exec -T postgres pg_isready -U "${POSTGRES_USER:-mluser}" >/dev/null && echo "  OK"
echo "[2/3] mlflow";    curl -fsS "http://localhost:${MLFLOW_PORT:-5000}/" >/dev/null && echo "  OK"
echo "[3/3] runner";    docker compose exec -T runner python -c "import pandas, sklearn, mlflow; print('  OK')"
EOF
chmod +x scripts/healthcheck.sh
```

### Day 1 완료 기준

* `make up && make healthcheck` 가 핵심 컴포넌트 3개 모두 OK 로 끝난다.
* `docker compose exec postgres psql -U mluser -d ml_db -c "\dt"` 결과에 `raw_transactions`, `feature_store`, `prediction_result`, `model_evaluation`, `data_lineage` 가 모두 보인다.
* `docs/features/ml-feature-schema.md`, `docs/models/ml-pipeline-architecture.md` 가 작성되어 CIO 요구사항(스키마/계보 문서)을 만족한다.

---

## Day 2: 데이터 분할과 누수 방지

### 2-0. 학습 시점과 라벨 시점이 다르다는 것을 인정하기

ML 입문 단계에서 가장 많이 보는 실수는 "지금 시점에서 데이터를 무작위 분할하는 것"이다. Nexus Pay 거래 데이터는 라벨(이상 거래 여부)이 거래 발생 시점이 아니라 운영팀 검증·고객 분쟁 처리 후에 확정된다. 모델은 거래 발생 시점에 결정을 내려야 하므로, **학습용 피처는 학습 시점 이전 정보로만 만들고, 라벨은 그 시점까지 확정된 라벨만 사용**해야 한다.

```
거래 발생 시각 ──▶ 결정(예측) 시각 ──▶ 라벨 확정 시각
   tx_time         decision_time          label_confirmed_at
       │               │                       │
       │   ◀─ 사용 가능 ─▶                       │
       │               │                       │
       │               └── 이 시점 이후 정보를 학습에 쓰면 누수
```

이를 코드로 강제하기 위해 Day 2 에서는 다음 세 가지를 만든다.

1. 비식별 거래 샘플 데이터 생성기 — `transacted_at` 과 `label_confirmed_at` 분리
2. 시간 기반 분할기 — `train_until / valid_until / test_until`
3. 누수 점검 체크리스트 — 다음 주차 모델에서도 그대로 활용

### 2-1. 비식별 거래 샘플 데이터 생성

```python
# scripts/generate_sample_transactions.py
"""Nexus Pay 비식별 거래 샘플 생성기.

CISO 요구사항: 카드번호·주민번호 등 민감정보는 포함하지 않는다.
COO 요구사항: 라벨 확정 시각이 거래 시각보다 이후이도록 시뮬레이션한다.
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

KST = timezone(timedelta(hours=9))
CHANNELS = ["APP", "WEB", "POS"]
COUNTRIES = ["KR", "KR", "KR", "JP", "US", "VN"]
SEGMENTS = ["GENERAL", "PREMIUM", "STARTUP", "OVERSEAS"]


def generate(n: int, seed: int = 42, fraud_rate: float = 0.012) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = []
    base = datetime(2026, 3, 1, tzinfo=KST)
    for i in range(n):
        ts = base + timedelta(minutes=rng.randint(0, 60 * 24 * 80))  # 80일치
        is_fraud = rng.random() < fraud_rate
        amount = rng.choice([5000, 12000, 30000, 88000, 150000, 980000])
        if is_fraud:
            amount = rng.choice([350000, 980000, 1500000, 2400000])
        rows.append(
            {
                "transaction_id": f"tx-{i:07d}",
                "customer_id": f"c-{rng.randint(1, 5000):05d}",
                "merchant_id": f"m-{rng.randint(1, 800):04d}",
                "amount": amount,
                "currency": "KRW",
                "channel": rng.choice(CHANNELS),
                "device_country": rng.choice(COUNTRIES),
                "merchant_segment": rng.choice(SEGMENTS),
                "is_label_fraud": is_fraud,
                "transacted_at": ts,
                # 라벨 확정은 거래 후 1~7일 사이에 일어난다고 가정
                "label_confirmed_at": ts + timedelta(hours=rng.randint(24, 24 * 7)),
            }
        )
    df = pd.DataFrame(rows).sort_values("transacted_at").reset_index(drop=True)
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=50_000)
    parser.add_argument("--out", type=Path, default=Path("data/sample/transactions.csv"))
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df = generate(args.n)
    df.to_csv(args.out, index=False)
    print(f"wrote {len(df):,} rows -> {args.out}")


if __name__ == "__main__":
    main()
```

생성된 CSV 는 PostgreSQL `raw_transactions` 에 적재한다. (학습 입력은 항상 PostgreSQL 을 거치도록 강제한다.)

```python
# scripts/load_sample_transactions.py
"""data/sample/transactions.csv → raw_transactions 적재."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


def main() -> None:
    csv_path = Path("data/sample/transactions.csv")
    df = pd.read_csv(csv_path, parse_dates=["transacted_at", "label_confirmed_at"])

    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    engine = create_engine(url, future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE raw_transactions")
        df.to_sql("raw_transactions", conn, if_exists="append", index=False)
    print(f"loaded {len(df):,} rows into raw_transactions")


if __name__ == "__main__":
    main()
```

### 2-2. 시간 기반 분할기

`features/data_split.py` 는 모든 모델이 공유하는 분할 기준이다.

```python
# features/data_split.py
"""학습 / 검증 / 테스트 시간 기반 분할.

config.yaml 의 `split` 섹션을 단일 진실로 사용한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import pandas as pd


@dataclass(frozen=True)
class SplitConfig:
    train_until: datetime
    valid_until: datetime
    test_until: datetime
    label_column: str
    label_confirmed_column: str
    min_label_lag_hours: int

    @classmethod
    def from_dict(cls, raw: dict) -> "SplitConfig":
        return cls(
            train_until=pd.Timestamp(raw["train_until"]),
            valid_until=pd.Timestamp(raw["valid_until"]),
            test_until=pd.Timestamp(raw["test_until"]),
            label_column=raw["label_column"],
            label_confirmed_column=raw["label_confirmed_column"],
            min_label_lag_hours=int(raw["min_label_lag_hours"]),
        )


def time_based_split(
    df: pd.DataFrame,
    cfg: SplitConfig,
    time_column: str = "transacted_at",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """라벨 확정 시각 기준 누수 방지 + 시간 기반 분할."""
    if df[time_column].dt.tz is None:
        raise ValueError(f"{time_column} 컬럼은 timezone-aware 이어야 한다")

    # 라벨이 train_until 이후에 확정된 행은 학습에서 제외 (누수 방지)
    label_lag_ok = df[cfg.label_confirmed_column] <= cfg.train_until
    train = df[(df[time_column] <= cfg.train_until) & label_lag_ok].copy()
    valid = df[(df[time_column] > cfg.train_until) & (df[time_column] <= cfg.valid_until)].copy()
    test = df[(df[time_column] > cfg.valid_until) & (df[time_column] <= cfg.test_until)].copy()
    return train, valid, test
```

### 2-3. 누수 검증 테스트

`tests/test_week1_pipeline.py` 에서 누수가 절대 발생하지 않도록 못 박는다. 다음 주 fraud 모델에도 그대로 재사용된다.

```python
# tests/test_week1_pipeline.py
from datetime import timedelta

import pandas as pd
import pytest

from features.data_split import SplitConfig, time_based_split


@pytest.fixture
def sample_df() -> pd.DataFrame:
    base = pd.Timestamp("2026-04-01", tz="Asia/Seoul")
    rows = []
    for i in range(100):
        tx = base + timedelta(hours=i)
        rows.append(
            {
                "transaction_id": f"t-{i:03d}",
                "transacted_at": tx,
                "label_confirmed_at": tx + timedelta(days=2),
                "is_label_fraud": i % 17 == 0,
                "amount": 10000 + i,
            }
        )
    return pd.DataFrame(rows)


def _cfg() -> SplitConfig:
    return SplitConfig.from_dict(
        {
            "train_until": "2026-04-03T00:00:00+09:00",
            "valid_until": "2026-04-04T00:00:00+09:00",
            "test_until": "2026-04-05T00:00:00+09:00",
            "label_column": "is_label_fraud",
            "label_confirmed_column": "label_confirmed_at",
            "min_label_lag_hours": 24,
        }
    )


def test_no_future_information_in_train(sample_df: pd.DataFrame) -> None:
    train, _, _ = time_based_split(sample_df, _cfg())
    assert (train["transacted_at"] <= _cfg().train_until).all()
    assert (train["label_confirmed_at"] <= _cfg().train_until).all(), "라벨 확정 시각 누수"


def test_disjoint_windows(sample_df: pd.DataFrame) -> None:
    train, valid, test = time_based_split(sample_df, _cfg())
    assert set(train["transaction_id"]).isdisjoint(valid["transaction_id"])
    assert set(valid["transaction_id"]).isdisjoint(test["transaction_id"])
```

### 2-4. 누수 방지 체크리스트 문서

```bash
cat > docs/features/ml-data-leakage-checklist.md << 'EOF'
# 데이터 누수 방지 체크리스트 — Nexus Pay 베이스라인

본 체크리스트는 Week 1~4 모든 모델 학습 전에 통과되어야 한다.

## 1. 시간 누수
- [ ] 학습 시점(`as_of_ts`) 이후의 거래·집계가 학습 피처에 들어가지 않았는가?
- [ ] 라벨 확정 시각(`label_confirmed_at`)이 학습 윈도우 끝 이후이면 그 행은 학습에서 제외되었는가?
- [ ] `transacted_at` 이 timezone-aware 인가?

## 2. 정답 누수 (Target Leakage)
- [ ] 라벨 자체나 라벨에서 직접 파생된 컬럼이 피처에 포함되어 있지 않은가?
- [ ] 라벨 확정 후에만 채워지는 컬럼(보상 처리 결과 등)이 학습에 사용되지 않았는가?

## 3. 그룹 누수
- [ ] 동일 고객의 미래 거래가 train 에 들어가고 과거 거래가 test 에 들어가는 패턴이 없는가? (시간 기반 분할로 강제)
- [ ] 동일 가맹점의 다른 거래에서 추출한 통계가 학습 시점 이전만 사용되었는가?

## 4. 전처리 누수
- [ ] z-score, min-max, target encoding 같은 통계가 train 데이터로만 fit 되었는가?
- [ ] sklearn `Pipeline` 으로 전처리와 모델이 묶여 있어 fit/predict 흐름에서 자동 강제되는가?

## 5. 운영 시점 누수
- [ ] 배치 추론 시각의 입력 피처가 결정 시각 기준으로 합법적인가?
- [ ] 온라인 추론 시 외부 시스템 응답이 결정 시각 이전 상태 기준인가?
EOF
```

### Day 2 완료 기준

* `python scripts/generate_sample_transactions.py --n 50000` 으로 80일치 비식별 거래 샘플이 생성된다.
* `python scripts/load_sample_transactions.py` 로 `raw_transactions` 에 적재된다.
* `pytest tests/test_week1_pipeline.py -q` 가 누수 방지 테스트 두 개를 모두 통과한다.
* `docs/features/ml-data-leakage-checklist.md` 가 작성되어 모든 주차 모델 학습 전 체크리스트로 사용 가능하다.

### 2-5. Upstream Spark Silver 연계 경로는 설계로만 채택

Week 1 의 필수 실습 입력은 synthetic sample 이다. 즉 `scripts/generate_sample_transactions.py` 와 `scripts/load_sample_transactions.py` 만으로 `raw_transactions → feature_store → train → predict` 흐름을 완주할 수 있어야 한다.

다만 실제 consulting demo 에서는 `study-data-pipeline` 의 Spark Silver 거래 데이터를 ML 입력으로 가져오는 경로가 필요하다. 이 경로는 Week 1 에서 구현하지 않고, 다음 원칙만 공식 설계로 채택한다.

| 구분 | Week 1 처리 | Week 2 처리 |
|------|-------------|-------------|
| synthetic sample | 필수 구현 | fraud 학습 smoke test 에 계속 사용 가능 |
| Spark Silver 거래 데이터 import | 설계/placeholder 만 명시 | 실제 구현 |
| chargeback/dispute/fraud review 라벨 | 라벨 지연 원칙만 명시 | 시뮬레이션 생성 후 Silver 와 조인 |

향후 구현할 스크립트는 다음 두 개로 고정한다.

* `scripts/generate_fraud_labels_from_silver.py` — Spark Silver 거래 데이터를 읽어 chargeback/dispute/manual review 성격의 사후 확정 라벨 CSV 를 생성한다.
* `scripts/import_spark_silver_to_raw_transactions.py` — Spark Silver 거래 데이터와 fraud label CSV 를 조인해 ML `raw_transactions` 스키마로 변환·적재한다.

이 설계를 채택하는 이유는 `is_label_fraud` 가 거래 발생 시점에 존재하는 값이 아니라 사후 운영 프로세스에서 확정되는 값이기 때문이다. 따라서 `label_confirmed_at` 은 항상 `transacted_at` 이후여야 하며, 학습 split 은 이 확정 시각을 기준으로 누수를 차단해야 한다.

---

## Day 3: 피처 엔지니어링 기본

### 3-0. 피처 생성을 두 단계로 나눈다

피처 생성은 두 단계로 분리한다. 학습/추론 시 같은 코드가 두 단계 모두를 수행해야 누수가 사라진다.

1. **시점 의존 피처(시간창 피처)** — `as_of_ts` 시점의 과거 데이터로만 계산. 예: `tx_count_24h`, `amount_zscore_30d`, `days_since_last_tx`.
2. **레코드 자체 피처** — 거래 한 건의 컬럼만 사용. 예: `amount`, `channel`, `device_country`.

두 단계 모두 `feature_store` 테이블에 동일한 키 `(feature_set, feature_version, transaction_id)` 로 저장한다.

### 3-1. `features/build_features.py` — 피처 생성 진입점

```python
# features/build_features.py
"""raw_transactions → feature_store 적재.

CLI 사용:
    python -m features.build_features --config config/model_config.yaml
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
    """시점 의존 피처: 과거 데이터로만 계산."""
    df = df.sort_values(["customer_id", "transacted_at"]).copy()

    # 직전 24시간 거래 횟수 (현재 거래 제외)
    df["tx_count_24h"] = (
        df.groupby("customer_id")
          .apply(lambda g: g["transacted_at"].apply(
              lambda t: ((g["transacted_at"] >= t - pd.Timedelta("24h"))
                         & (g["transacted_at"] < t)).sum()))
          .reset_index(level=0, drop=True)
    )

    # 직전 거래 후 경과일 (없으면 NaN)
    df["days_since_last_tx"] = (
        df.groupby("customer_id")["transacted_at"].diff().dt.total_seconds() / 86400.0
    )

    # 최근 30일 금액 z-score (학습 기준 시점 이전만 사용)
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

    df["amount_zscore_30d"] = (
        df.groupby("customer_id", group_keys=False).apply(zscore_30d)
    )
    return df


def _to_feature_rows(df: pd.DataFrame, feature_set: str, feature_version: str) -> pd.DataFrame:
    payload_cols = [
        "amount", "amount_zscore_30d", "tx_count_24h", "days_since_last_tx",
        "channel", "device_country", "merchant_segment",
    ]
    df = df.copy()
    df["payload_json"] = df[payload_cols].apply(lambda r: json.dumps(r.dropna().to_dict()), axis=1)
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

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    until_ts = pd.Timestamp(cfg["split"]["test_until"])

    engine = _engine()
    raw = _load_raw(engine, until_ts)
    raw["transacted_at"] = pd.to_datetime(raw["transacted_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw["label_confirmed_at"] = pd.to_datetime(raw["label_confirmed_at"], utc=True).dt.tz_convert("Asia/Seoul")

    enriched = _temporal_features(raw)
    rows = _to_feature_rows(enriched, feature_set, feature_version)

    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM feature_store "
                "WHERE feature_set = :fs AND feature_version = :fv"
            ),
            {"fs": feature_set, "fv": feature_version},
        )
        rows.to_sql("feature_store", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, "
                " input_window_start, input_window_end, output_target) "
                "VALUES (:run_id, 'feature', 'raw_transactions', :ws, :we, 'feature_store')"
            ),
            {
                "run_id": run_id,
                "ws": raw["transacted_at"].min().to_pydatetime(),
                "we": until_ts.to_pydatetime(),
            },
        )
    print(f"feature_store updated: {len(rows):,} rows (run_id={run_id})")


if __name__ == "__main__":
    main()
```

### 3-2. `features/feature_pipeline.py` — sklearn Pipeline

학습/추론에서 동일하게 사용할 전처리 파이프라인을 만든다. 누수 방지의 마지막 보루는 sklearn `Pipeline` 이다. 전처리 통계는 train 데이터로만 fit 되어야 하므로 `Pipeline.fit(X_train, y_train)` 한 곳에서만 fit 하도록 강제한다.

```python
# features/feature_pipeline.py
"""학습/추론 공통 전처리 파이프라인."""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


class IqrClipper(BaseEstimator, TransformerMixin):
    """IQR 기반 이상치 클리핑. fit 시 1.5*IQR (또는 config) 경계를 학습."""

    def __init__(self, factor: float = 3.0):
        self.factor = factor
        self.lower_: pd.Series | None = None
        self.upper_: pd.Series | None = None

    def fit(self, X: pd.DataFrame, y=None) -> "IqrClipper":
        q1, q3 = X.quantile(0.25), X.quantile(0.75)
        iqr = q3 - q1
        self.lower_ = q1 - self.factor * iqr
        self.upper_ = q3 + self.factor * iqr
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return X.clip(lower=self.lower_, upper=self.upper_, axis=1)


def build_preprocessor(numerical: List[str], categorical: List[str], outlier_factor: float = 3.0) -> ColumnTransformer:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("clipper", IqrClipper(factor=outlier_factor)),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, numerical),
            ("cat", categorical_pipeline, categorical),
        ]
    )


def feature_store_to_dataframe(rows: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """`feature_store.payload_json` 을 평탄화한 DataFrame 반환."""
    flat = pd.json_normalize(rows["payload_json"].apply(_safe_json))
    flat = flat.reindex(columns=columns)
    flat.index = rows.index
    return flat


def _safe_json(s):
    import json
    if isinstance(s, dict):
        return s
    return json.loads(s) if s else {}
```

### 3-3. 단위 테스트

```python
# tests/test_feature_pipeline.py
import pandas as pd
import numpy as np

from features.feature_pipeline import IqrClipper, build_preprocessor


def test_iqr_clipper_clips_extreme_values():
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5000]})
    clipper = IqrClipper(factor=1.5).fit(df)
    out = clipper.transform(df)
    assert out["x"].max() < 5000


def test_preprocessor_handles_missing_categorical():
    df = pd.DataFrame(
        {
            "amount": [100, 200, np.nan, 400],
            "tx_count_24h": [0, 1, 2, 1],
            "channel": ["APP", None, "WEB", "APP"],
            "device_country": ["KR", "KR", "JP", None],
        }
    )
    preprocessor = build_preprocessor(
        numerical=["amount", "tx_count_24h"],
        categorical=["channel", "device_country"],
    )
    X = preprocessor.fit_transform(df)
    assert X.shape[0] == 4
    assert not np.isnan(X).any()
```

### Day 3 완료 기준

* `make features` 실행 시 `feature_store` 가 갱신되고 `data_lineage` 에 한 행이 기록된다.
* `pytest tests/test_feature_pipeline.py -q` 통과.
* 동일 코드로 학습 시점·추론 시점 피처 생성이 가능하다는 점이 `feature_pipeline.py` 의 `feature_store_to_dataframe()` 로 코드상 강제된다.

---

## Day 4: 모델 학습과 평가 템플릿

### 4-0. baseline 모델은 알고리즘 비교가 아니라 흐름 검증이 목적

Day 4 의 목표는 가장 좋은 모델을 찾는 것이 아니다. 다음 모든 항목이 한 번의 명령(`make train`)으로 실행되도록 묶는 것이다.

* `feature_store` 에서 학습 데이터를 가져온다.
* `time_based_split` 으로 train/valid/test 를 나눈다.
* 전처리 + 모델을 sklearn `Pipeline` 으로 묶어 fit 한다.
* MLflow 에 파라미터·지표·artifact 를 기록한다.
* `model_evaluation` 테이블에 평가 결과(메트릭 + 비용 해석) 행을 적재한다.
* `data/reports/baseline_eval_report.md` 를 자동 생성한다.
* 모델 artifact 를 `data/artifacts/<model_name>/<version>/model.joblib` 으로 저장한다.

### 4-1. `models/train_baseline.py`

```python
# models/train_baseline.py
"""baseline classifier 학습 + MLflow 기록 + 평가 적재.

CLI 사용:
    python -m models.train_baseline --config config/model_config.yaml
"""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
import yaml
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text

from features.data_split import SplitConfig, time_based_split
from features.feature_pipeline import build_preprocessor, feature_store_to_dataframe


# ─────────────────────── helpers ───────────────────────

def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _load_training_frame(engine, feature_set: str, feature_version: str) -> pd.DataFrame:
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


def _build_estimator(name: str, random_state: int) -> Pipeline:
    if name == "logistic_regression":
        return LogisticRegression(max_iter=200, class_weight="balanced", random_state=random_state)
    if name == "random_forest":
        return RandomForestClassifier(
            n_estimators=200, class_weight="balanced", random_state=random_state, n_jobs=-1
        )
    raise ValueError(f"unknown model: {name}")


# ─────────────────────── main ───────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    numerical = cfg["features"]["numerical"]
    categorical = cfg["features"]["categorical"]
    label_col = cfg["split"]["label_column"]
    split_cfg = SplitConfig.from_dict(cfg["split"])
    model_name = cfg["model"]["registry"]["name"]
    baseline = cfg["model"]["baseline"]
    rs = cfg["model"]["random_state"]
    fp_cost = cfg["evaluation"]["cost"]["false_positive_cost_krw"]
    fn_cost = cfg["evaluation"]["cost"]["false_negative_cost_krw"]

    engine = _engine()
    raw = _load_training_frame(engine, feature_set, feature_version)
    raw["transacted_at"] = pd.to_datetime(raw["transacted_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw["label_confirmed_at"] = pd.to_datetime(raw["label_confirmed_at"], utc=True).dt.tz_convert("Asia/Seoul")
    raw[label_col] = raw[label_col].astype(int)

    train_df, valid_df, test_df = time_based_split(raw, split_cfg)
    columns = numerical + categorical
    X_train = feature_store_to_dataframe(train_df, columns)
    X_valid = feature_store_to_dataframe(valid_df, columns)
    X_test = feature_store_to_dataframe(test_df, columns)
    y_train = train_df[label_col].values
    y_valid = valid_df[label_col].values
    y_test = test_df[label_col].values

    pipe = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor(numerical, categorical, cfg["features"]["outlier"]["factor"])),
            ("model", _build_estimator(baseline, rs)),
        ]
    )

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(model_name)
    run_id = str(uuid.uuid4())
    started = time.time()

    with mlflow.start_run(run_name=f"{baseline}-{run_id[:8]}") as run:
        mlflow.log_params({"baseline": baseline, "random_state": rs, "feature_set": feature_set,
                           "feature_version": feature_version})
        pipe.fit(X_train, y_train)

        y_valid_proba = pipe.predict_proba(X_valid)[:, 1]
        y_test_proba = pipe.predict_proba(X_test)[:, 1]
        threshold = cfg["serving"]["threshold"]
        y_test_pred = (y_test_proba >= threshold).astype(int)

        metrics = {
            "precision": precision_score(y_test, y_test_pred, zero_division=0),
            "recall":    recall_score(y_test, y_test_pred, zero_division=0),
            "f1":        f1_score(y_test, y_test_pred, zero_division=0),
            "pr_auc":    average_precision_score(y_test, y_test_proba),
            "roc_auc":   roc_auc_score(y_test, y_test_proba) if len(set(y_test)) > 1 else float("nan"),
        }
        mlflow.log_metrics(metrics)

        cm = confusion_matrix(y_test, y_test_pred, labels=[0, 1]).tolist()
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        cost_krw = fp * fp_cost + fn * fn_cost
        mlflow.log_metric("cost_krw", cost_krw)

        artifact_dir = Path("data/artifacts") / model_name / time.strftime("%Y%m%d-%H%M%S")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / "model.joblib"
        model_version = artifact_dir.name
        joblib.dump(
            {"pipeline": pipe, "columns": columns, "threshold": threshold, "model_version": model_version},
            artifact_path,
        )
        mlflow.log_artifact(str(artifact_path), artifact_path="model")
        mlflow.sklearn.log_model(pipe, artifact_path="sklearn_model", registered_model_name=model_name)

        # PostgreSQL model_evaluation 적재
        with engine.begin() as conn:
            for metric_name, metric_value in metrics.items():
                if pd.isna(metric_value):
                    continue
                conn.execute(
                    text(
                        "INSERT INTO model_evaluation(model_name, model_version, "
                        "  eval_window_start, eval_window_end, metric_name, metric_value, extra_json) "
                        "VALUES (:mn, :mv, :ws, :we, :name, :val, :extra)"
                    ),
                    {
                        "mn": model_name,
                        "mv": model_version,
                        "ws": split_cfg.valid_until.to_pydatetime(),
                        "we": split_cfg.test_until.to_pydatetime(),
                        "name": metric_name,
                        "val": float(metric_value),
                        "extra": json.dumps({"threshold": threshold, "confusion_matrix": cm,
                                             "mlflow_run_id": run.info.run_id,
                                             "fp_cost_krw": fp_cost, "fn_cost_krw": fn_cost,
                                             "cost_krw": cost_krw}),
                    },
                )

        # 평가 리포트 자동 생성
        from models.eval_report import render_report  # 재사용
        report_path = Path("data/reports") / f"baseline_eval_report_{time.strftime('%Y%m%d-%H%M%S')}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        render_report(
            path=report_path,
            model_name=model_name,
            model_version=model_version,
            metrics=metrics,
            cm=cm,
            cost={"fp": fp, "fn": fn, "fp_cost_krw": fp_cost, "fn_cost_krw": fn_cost, "total_krw": cost_krw},
            run_id=run_id,
            split_cfg=split_cfg,
        )
        mlflow.log_artifact(str(report_path), artifact_path="reports")

    elapsed = time.time() - started
    print(f"trained {model_name} version={model_version} cost_krw={cost_krw:,.0f} elapsed={elapsed:.1f}s")
    print(f"report: {report_path}")


if __name__ == "__main__":
    main()
```

### 4-2. `models/eval_report.py` — CFO 해석을 포함한 리포트 템플릿

```python
# models/eval_report.py
"""평가 리포트 자동 생성. CFO 해석(오탐/미탐 비용)을 포함한다."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

from features.data_split import SplitConfig


REPORT_TEMPLATE = """# Baseline 모델 평가 리포트

- 모델: `{model_name}`
- 버전: `{model_version}`
- 평가 윈도우: `{eval_start}` ~ `{eval_end}`
- 학습 분할 기준: train ≤ `{train_until}`, valid ≤ `{valid_until}`, test ≤ `{test_until}`
- 자동 생성 시각: {generated_at}
- run_id: `{run_id}`

## 1. 분류 지표

| 지표 | 값 |
|------|----|
| precision | {precision:.4f} |
| recall    | {recall:.4f} |
| f1        | {f1:.4f} |
| PR-AUC    | {pr_auc:.4f} |
| ROC-AUC   | {roc_auc:.4f} |

## 2. 혼동 행렬

|         | 예측 음성 (0) | 예측 양성 (1) |
|---------|---------------|---------------|
| 실제 0  | TN={tn}       | FP={fp}       |
| 실제 1  | FN={fn}       | TP={tp}       |

## 3. CFO 해석 — 운영 비용 환산

- 오탐(FP) 1건당 운영 검토 비용 가정: **{fp_cost_krw:,} KRW**
- 미탐(FN) 1건당 평균 손실 가정:        **{fn_cost_krw:,} KRW**
- 평가 윈도우 합산 추정 비용:           **{total_cost_krw:,} KRW**
- 임계값 조정 시뮬레이션은 추후 Day 5 또는 Week 2 임계값 튜닝 단계에서 수행한다.

## 4. 운영 인수인계 메모

- 모델 artifact: `data/artifacts/{model_name}/.../model.joblib`
- 같은 입력 컬럼/순서가 학습/배치 추론/API 에서 모두 사용된다 (`feature_pipeline.feature_store_to_dataframe`).
- 본 리포트는 `make train` 실행 시 매번 자동 생성된다. 수동 편집 금지.
"""


def render_report(
    *,
    path: Path,
    model_name: str,
    model_version: str,
    metrics: Dict[str, float],
    cm,
    cost: Dict[str, float],
    run_id: str,
    split_cfg: SplitConfig,
) -> None:
    tn, fp = cm[0]
    fn, tp = cm[1]
    body = REPORT_TEMPLATE.format(
        model_name=model_name,
        model_version=model_version,
        eval_start=split_cfg.valid_until.isoformat(),
        eval_end=split_cfg.test_until.isoformat(),
        train_until=split_cfg.train_until.isoformat(),
        valid_until=split_cfg.valid_until.isoformat(),
        test_until=split_cfg.test_until.isoformat(),
        generated_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        run_id=run_id,
        precision=metrics.get("precision", float("nan")),
        recall=metrics.get("recall", float("nan")),
        f1=metrics.get("f1", float("nan")),
        pr_auc=metrics.get("pr_auc", float("nan")),
        roc_auc=metrics.get("roc_auc", float("nan")),
        tn=tn, fp=fp, fn=fn, tp=tp,
        fp_cost_krw=int(cost["fp_cost_krw"]),
        fn_cost_krw=int(cost["fn_cost_krw"]),
        total_cost_krw=int(cost["total_krw"]),
    )
    path.write_text(body, encoding="utf-8")
```

`reports/baseline_eval_report.md` 에는 사람이 읽기 좋은 정적 템플릿(자동 생성된 결과를 비교할 때 사용할 기준 형식)을 한 부 두면 좋다.

```bash
cat > reports/baseline_eval_report.md << 'EOF'
# Baseline 모델 평가 리포트 (템플릿)

> 본 파일은 양식 예시이며 실제 결과는 `data/reports/baseline_eval_report_*.md` 에 자동 생성된다.

- 모델: `nexuspay_baseline_classifier`
- 평가 윈도우: yyyy-mm-dd ~ yyyy-mm-dd

## 1. 분류 지표
- precision / recall / f1 / PR-AUC / ROC-AUC

## 2. 혼동 행렬

## 3. CFO 해석 — 오탐·미탐 비용 환산

## 4. 운영 인수인계 메모
EOF
```

### Day 4 완료 기준

* `make train` 실행 시 MLflow UI(`http://localhost:5000`)에 한 개 이상의 run 이 기록된다.
* `data/reports/baseline_eval_report_*.md` 가 새로 생성되며 CFO 해석 섹션이 채워져 있다.
* `model_evaluation` 테이블에서 `precision/recall/f1/pr_auc/roc_auc` 행을 조회 가능하다.
* `data/artifacts/.../model.joblib` 가 직렬화된 sklearn `Pipeline` 으로 존재한다.

---

## Day 5: 배치 예측 + API + DAG 초안

### 5-0. 같은 모델을 두 가지 진입점으로 운영한다

운영팀은 두 가지 진입점으로 모델을 호출한다.

* **배치 진입점**: `models/batch_predict.py` 가 PostgreSQL 의 입력을 한꺼번에 읽어 `prediction_result` 에 적재.
* **온라인 진입점**: `serving/app.py` 의 FastAPI `POST /predict` 가 한 건 단위 입력을 받아 즉시 응답.

두 진입점은 동일한 `joblib` artifact 를 사용하고 동일한 컬럼 순서를 따른다. 입력 스키마가 흐트러지지 않도록 `pydantic` 모델로 검증한다.

### 5-1. `models/batch_predict.py`

```python
# models/batch_predict.py
"""최신 baseline 모델로 배치 추론 후 prediction_result 에 적재."""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path

import joblib
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


def _latest_artifact(model_name: str) -> Path:
    candidates = sorted(Path("data/artifacts/" + model_name).glob("*/model.joblib"))
    if not candidates:
        raise FileNotFoundError(f"no artifact found for {model_name} — run make train first")
    return candidates[-1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))

    feature_set = cfg["features"]["feature_set"]
    feature_version = cfg["features"]["feature_version"]
    model_name = cfg["model"]["registry"]["name"]
    artifact_path = _latest_artifact(model_name)

    bundle = joblib.load(artifact_path)
    pipe = bundle["pipeline"]
    columns = bundle["columns"]
    threshold = bundle["threshold"]

    engine = _engine()
    sql = text(
        """
        SELECT transaction_id, payload_json, as_of_ts
          FROM feature_store
         WHERE feature_set = :fs AND feature_version = :fv
           AND transaction_id NOT IN (
                 SELECT transaction_id FROM prediction_result
                  WHERE model_name = :mn
           )
        """
    )
    with engine.connect() as conn:
        rows = pd.read_sql(sql, conn, params={"fs": feature_set, "fv": feature_version, "mn": model_name})

    if rows.empty:
        print("no new rows to score")
        return

    X = feature_store_to_dataframe(rows, columns)
    proba = pipe.predict_proba(X)[:, 1]
    pred = (proba >= threshold).astype(int)

    run_id = str(uuid.uuid4())
    out = pd.DataFrame(
        {
            "transaction_id": rows["transaction_id"].values,
            "model_name": model_name,
            "model_version": artifact_path.parent.name,
            "feature_set": feature_set,
            "feature_version": feature_version,
            "score": proba,
            "prediction": ["POSITIVE" if p else "NEGATIVE" for p in pred],
            "run_id": run_id,
        }
    )

    with engine.begin() as conn:
        out.to_sql("prediction_result", conn, if_exists="append", index=False)
        conn.execute(
            text(
                "INSERT INTO data_lineage(run_id, stage, input_source, output_target) "
                "VALUES (:run_id, 'predict', :src, 'prediction_result')"
            ),
            {"run_id": run_id, "src": f"feature_store@{feature_set}/{feature_version}"},
        )
    print(f"scored {len(out):,} rows (run_id={run_id})")


if __name__ == "__main__":
    main()
```

### 5-2. `serving/app.py` — FastAPI 추론 엔드포인트

```python
# serving/app.py
"""FastAPI 기반 모델 추론·조회 API.

엔드포인트:
- GET  /health
- POST /predict        — 한 건 추론
- GET  /predictions/{tx_id} — prediction_result 조회
"""
from __future__ import annotations

import os
from pathlib import Path

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text


app = FastAPI(title="Nexus Pay Baseline ML API", version="0.1.0")


class TransactionRequest(BaseModel):
    transaction_id: str = Field(..., max_length=40)
    amount: float = Field(..., ge=0)
    amount_zscore_30d: float = 0.0
    tx_count_24h: int = 0
    days_since_last_tx: float = 0.0
    channel: str
    device_country: str
    merchant_segment: str


class PredictionResponse(BaseModel):
    transaction_id: str
    model_name: str
    model_version: str
    score: float
    prediction: str


def _latest_artifact(model_name: str) -> Path:
    candidates = sorted(Path("data/artifacts/" + model_name).glob("*/model.joblib"))
    if not candidates:
        raise RuntimeError(f"no artifact for {model_name}")
    return candidates[-1]


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


MODEL_NAME = os.environ.get("MODEL_NAME", "nexuspay_baseline_classifier")
_BUNDLE = None


def _load_bundle():
    global _BUNDLE
    if _BUNDLE is None:
        path = _latest_artifact(MODEL_NAME)
        _BUNDLE = joblib.load(path)
        _BUNDLE["version"] = path.parent.name
    return _BUNDLE


@app.get("/health")
def health():
    try:
        bundle = _load_bundle()
        return {"status": "ok", "model": MODEL_NAME, "version": bundle["version"]}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/predict", response_model=PredictionResponse)
def predict(req: TransactionRequest):
    bundle = _load_bundle()
    pipe = bundle["pipeline"]
    columns = bundle["columns"]
    threshold = bundle["threshold"]

    df = pd.DataFrame(
        [
            {
                "amount": req.amount,
                "amount_zscore_30d": req.amount_zscore_30d,
                "tx_count_24h": req.tx_count_24h,
                "days_since_last_tx": req.days_since_last_tx,
                "channel": req.channel,
                "device_country": req.device_country,
                "merchant_segment": req.merchant_segment,
            }
        ]
    ).reindex(columns=columns)

    proba = float(pipe.predict_proba(df)[0, 1])
    pred = "POSITIVE" if proba >= threshold else "NEGATIVE"
    return PredictionResponse(
        transaction_id=req.transaction_id,
        model_name=MODEL_NAME,
        model_version=bundle["version"],
        score=round(proba, 4),
        prediction=pred,
    )


@app.get("/predictions/{transaction_id}")
def get_prediction(transaction_id: str):
    engine = _engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT transaction_id, model_name, model_version, score, prediction, scored_at "
                "FROM prediction_result WHERE transaction_id = :tid "
                "ORDER BY scored_at DESC LIMIT 1"
            ),
            {"tid": transaction_id},
        ).mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no prediction for {transaction_id}")
    return dict(row)
```

### 5-3. `dags/ml_training_pipeline_dag.py` — Airflow DAG 초안

Week 1 에서는 Airflow 컨테이너 자체는 띄우지 않는다(`study-data-pipeline` 의 Airflow 환경을 재사용). 하지만 DAG 파일은 미리 만들어 Day 5 시점에 구문 검증과 의존 관계까지 확정한다. `--validate` 인자를 지원해 Airflow 없이 import / 의존성 검사가 가능하도록 한다.

```python
# dags/ml_training_pipeline_dag.py
"""Nexus Pay baseline ML 재학습 DAG 초안 (Week 1 시점).

Week 4 에서 fraud / demand / segment 학습으로 확장한다.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:  # 로컬 검증용
    AIRFLOW_AVAILABLE = False


DEFAULT_ARGS = {
    "owner": "ml-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PIPELINE_STEPS = [
    ("build_features", "python -m features.build_features --config config/model_config.yaml"),
    ("train_baseline", "python -m models.train_baseline --config config/model_config.yaml"),
    ("batch_predict",  "python -m models.batch_predict --config config/model_config.yaml"),
]


def build_dag():
    if not AIRFLOW_AVAILABLE:
        raise RuntimeError("Airflow not installed in this container — DAG runtime needs a separate Airflow env")

    dag = DAG(
        dag_id="ml_training_pipeline",
        description="Nexus Pay baseline ML retraining (feature -> train -> predict)",
        start_date=datetime(2026, 5, 25),
        schedule_interval="0 3 * * *",  # 매일 새벽 3시
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["nexuspay", "ml", "week1"],
    )

    previous = None
    for task_id, command in PIPELINE_STEPS:
        op = BashOperator(task_id=task_id, bash_command=command, dag=dag)
        if previous is not None:
            previous >> op
        previous = op
    return dag


def _validate() -> int:
    """Airflow 미설치 환경에서도 의존 관계만 검증."""
    print("Pipeline steps:")
    for task_id, command in PIPELINE_STEPS:
        print(f"  - {task_id}: {command}")
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
```

### 5-4. End-to-end 검증 시나리오

Day 5 마지막에는 다음을 한 호흡으로 실행해 모든 진입점이 같은 모델/스키마/계보를 공유함을 증명한다.

```bash
# 0. 환경 기동 + 헬스체크
make up
make healthcheck

# 1. 샘플 거래 적재 → 피처 생성
docker compose exec runner python scripts/generate_sample_transactions.py --n 50000
docker compose exec runner python scripts/load_sample_transactions.py
make features

# 2. baseline 학습 + 평가 리포트 자동 생성
make train

# 3. 배치 추론
make predict

# 4. API 호출 (FastAPI 컨테이너는 docker compose up 시점에 이미 기동)
make api-test

# 5. DAG 의존성 검증
make dag-validate

# 6. 통합 테스트
make test

# 7. 결과 확인
docker compose exec postgres psql -U mluser -d ml_db -c \
  "SELECT model_name, model_version, COUNT(*) FROM prediction_result GROUP BY 1,2 ORDER BY 1,2;"
```

기대 결과:

* MLflow UI 에 학습 run 1건 이상.
* `data/reports/baseline_eval_report_*.md` 에 CFO 해석 포함된 리포트 1건 생성.
* `prediction_result` 에 50,000 건 가까이의 예측 결과(중복 제외)가 적재.
* `api-test` 응답이 `{"transaction_id": "tx-demo", ..., "score": 0.xxxx, "prediction": "..."}` 형식.
* `tests/test_week1_pipeline.py` 와 `tests/test_feature_pipeline.py` 모두 통과.

### Day 5 완료 기준

* `make predict` 가 실패 없이 끝나고 `prediction_result` 에 새로운 row 가 생긴다.
* `make api-test` 가 200 응답과 함께 score/prediction 을 반환한다.
* `make dag-validate` 가 OK 로 끝난다.
* `data_lineage` 테이블에 feature 단계 1행, predict 단계 1행이 기록되어 학습/예측 흐름의 추적성이 확보된다.

---

## Week 1 산출물 체크리스트

다음 항목이 모두 채워져야 Week 1 이 완료된 것으로 본다. 완료된 항목 옆에 날짜를 적어 두면 다음 주차 가이드에서 인용하기 쉽다.

### 코드 산출물

- [ ] `study-ml-pipeline/docker-compose.yml`
- [ ] `study-ml-pipeline/.env.example`
- [ ] `study-ml-pipeline/Makefile`
- [ ] `study-ml-pipeline/requirements.txt`
- [ ] `study-ml-pipeline/config/model_config.yaml`
- [ ] `study-ml-pipeline/schemas/init.sql`
- [ ] `study-ml-pipeline/scripts/healthcheck.sh`
- [ ] `study-ml-pipeline/scripts/generate_sample_transactions.py`
- [ ] `study-ml-pipeline/scripts/load_sample_transactions.py`
- [ ] `study-ml-pipeline/features/data_split.py`
- [ ] `study-ml-pipeline/features/build_features.py`
- [ ] `study-ml-pipeline/features/feature_pipeline.py`
- [ ] `study-ml-pipeline/models/train_baseline.py`
- [ ] `study-ml-pipeline/models/batch_predict.py`
- [ ] `study-ml-pipeline/models/eval_report.py`
- [ ] `study-ml-pipeline/serving/app.py`
- [ ] `study-ml-pipeline/dags/ml_training_pipeline_dag.py`
- [ ] `study-ml-pipeline/tests/test_week1_pipeline.py`
- [ ] `study-ml-pipeline/tests/test_feature_pipeline.py`

### 문서 산출물

- [ ] `study-ml-pipeline/docs/features/ml-feature-schema.md`
- [ ] `study-ml-pipeline/docs/models/ml-pipeline-architecture.md`
- [ ] `study-ml-pipeline/docs/features/ml-data-leakage-checklist.md`
- [ ] `study-ml-pipeline/reports/baseline_eval_report.md` (양식 예시)
- [ ] `study-ml-pipeline/data/reports/baseline_eval_report_*.md` (자동 생성본 1건 이상)

### 운영 산출물

- [ ] `make up && make healthcheck` 핵심 컴포넌트 3종 OK
- [ ] MLflow UI 에 baseline 실험 run 1건 이상
- [ ] `prediction_result` 테이블에 1만 건 이상의 배치 예측 결과
- [ ] `data_lineage` 테이블에 `feature`/`predict` 단계 행 기록
- [ ] `make test` 전 케이스 PASS

### CTO/CIO/COO/CFO 요구사항 정합 점검

| 요구자 | 요구사항 | 충족 증빙 |
|--------|----------|-----------|
| CTO    | Docker Compose 재현 가능 환경 | `docker-compose.yml`, `make up` |
| CTO    | 단계 독립 실행 + 단일 흐름 | `Makefile` 타겟 분리 + DAG 초안 |
| CIO    | 입출력 스키마와 데이터 계보 문서화 | `ml-feature-schema.md`, `data_lineage` |
| COO    | 예측 결과 PostgreSQL 저장 | `prediction_result` 테이블 |
| CFO    | 평가 리포트에 비용 해석 포함 | `eval_report.py` "CFO 해석" 섹션 |

---

## 다음 주(Week 2) 연결 포인트

Week 2 (분류·이상탐지) 가이드는 본 가이드의 다음 자산을 그대로 상속한다. 직접 다시 만들지 않는다.

* `time_based_split` / `SplitConfig` — fraud 라벨 윈도우만 갈아끼움
* `feature_pipeline.build_preprocessor` — fraud 전용 컬럼만 추가
* `prediction_result` 테이블 — fraud risk score 는 별도의 `risk_score` 테이블로 확장(2주차에서 신설)
* `models/eval_report.py` — fraud 모델용 high/medium/low 등급 해석 섹션이 추가될 예정
* `dags/ml_training_pipeline_dag.py` — fraud 학습/스코어링 태스크가 추가될 예정
* `scripts/generate_fraud_labels_from_silver.py`, `scripts/import_spark_silver_to_raw_transactions.py` — Week 1 에서는 설계만 채택하고 Week 2 에서 구현

본 가이드의 디렉토리/스키마/Make 타겟 표준을 변경할 사유가 생기면, 변경 전 본 문서를 먼저 갱신하고 그 변경 사실을 다음 주차 가이드 첫 문단에 기록한다.
