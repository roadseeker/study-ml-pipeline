# Week 4: MLOps 운영화, API Hardening, RAG/LLM 피드백 루프

**기간**: 5일 (월~금, 풀타임 40시간) — 2026-06-15 ~ 2026-06-21
**주제**: Week 1~3 의 Nexus Pay 모델 자산을 MLflow Registry, production API, Airflow retraining, task-aware monitoring, RAG assistant + feedback loop 로 묶어 CTO/CISO/CEO/CMO 수준의 운영 인수인계 패키지로 완성한다.

**산출물**

| 구분 | 산출물 |
|------|--------|
| Registry / promotion | `mlops/promotion_gates.py`, `mlops/register_model.py`, `mlops/promote_model.py`, `schemas/model_promotion_log.sql` |
| 후보 모델 평가 | `mlops/evaluate_model_candidates.py`, `docs/mlops/model-candidate-evaluation.md` |
| API hardening | Week 1 `serving/app.py` production hardening, `docs/mlops/api-hardening-runbook.md` |
| 재학습 오케스트레이션 | `dags/ml_retraining_dag.py`, `mlops/data_quality_check.py`, `mlops/gate_check.py` |
| Monitoring | `mlops/drift_metrics.py`, `mlops/monitor_predictions.py`, `schemas/model_monitoring.sql`, `data/reports/model_monitoring_report_*.md` |
| RAG | `rag/pii_filter.py`, `rag/ingest_documents.py`, `rag/retrieve_context.py`, `rag/query_assistant.py`, `docs/rag/ml-ops-assistant-design.md` |
| LLM feedback loop | `schemas/llm_feedback.sql`, `docs/llm-feedback-loop-design.md` |
| 운영 준비도 리포트 | `reports/mlops_operational_readiness_report.py`, `data/reports/mlops_operational_readiness_report_*.md` |
| 테스트 | `tests/test_mlops_end_to_end.py` |
| 실행 타겟 | `Makefile` registry / API / retraining / monitoring / RAG / feedback 타겟 |

**전제 조건**

| 구분 | 필요 조건 |
|------|-----------|
| Week 1 환경 | PostgreSQL, MLflow, FastAPI, Qdrant 가 Docker Compose 로 기동 가능하고 `make up && make healthcheck` 가 성공한다. |
| Week 1 baseline 자산 | `serving/app.py`, `prediction_result`, `model_evaluation`, `data_lineage`, `nexuspay_baseline_classifier` artifact 가 준비되어 있다. |
| Week 2 fraud 자산 | `features/risk_reasons.py`, `models/anomaly_scorer.py`, `serving/fraud_router.py`, `risk_score`, `nexuspay_fraud_classifier` 가 정상 작동한다. |
| Week 3 business insight 자산 | `models/forecast_intervals.py`, `features/segment_actions.py`, `serving/business_insight_router.py`, `revenue_forecast`, `customer_segment`, `segment_action_recommendation` 이 정상 작동한다. |
| MLflow Registry | `nexuspay_baseline_classifier`, `nexuspay_fraud_classifier`, `nexuspay_revenue_forecast`, `nexuspay_customer_segment` 중 최소 1개가 Registered Model 로 등록되어 있다. |
| RAG 의존성 | `qdrant-client`, `sentence-transformers`, Pydantic 기반 응답 검증을 설치할 수 있고 Qdrant `6333` 포트에 접근 가능하다. |
| 운영 문서 | Week 1~4 guide, runbook, schema, report 문서가 RAG ingestion 대상으로 존재하며 raw PII / secret 이 포함되어 있지 않다. |

> 강화학습 범위 안내: 본 가이드는 plan 의 "강화학습 참고" 절을 그대로 준용한다. PPO 기반 RL 모델을 직접 학습하지 않고, **답변 평가 데이터셋·preference pair·human feedback loop·평가 지표 설계** 까지를 Day 5 의 학습 목표로 한다. Week 4 의 산출물은 운영 인수인계 + 향후 RLHF/RLAIF 프로젝트의 진입점이라는 두 역할을 동시에 한다.

---

## 수행 시나리오

### 배경 설정

Week 1~3에서 Nexus Pay는 여러 ML 모델을 만들었다. 하지만 consulting deliverable로 더 중요한 질문은 이제부터다. 어떤 모델을 운영에 올릴 것인가? 성능이 나빠지면 누가 알아차릴 것인가? API가 잘못된 입력을 받으면 어떻게 실패할 것인가? 운영팀이 "지난주 fraud 모델 기준이 뭐였지?"라고 물을 때 문서와 실행 흔적을 어디서 찾을 것인가?

> "모델을 만드는 프로젝트는 많습니다. 우리가 필요한 것은 운영팀이 모델을 믿고 넘겨받을 수 있는 체계입니다. registry, promotion, monitoring, retraining, runbook, 그리고 질문에 답할 수 있는 지식 기반까지 갖춰야 진짜 ML 플랫폼이라고 부를 수 있습니다." - Nexus Pay CTO

Week 4는 새로운 예측 문제를 추가하지 않는다. 대신 기존 모델들을 운영 가능한 자산으로 정리한다.

### C레벨 요구사항

| 요구자 | 요구사항 | 본 가이드 반영 위치 |
|--------|----------|----------------------|
| CTO | 모델 registry 와 promotion gate 로 운영 승격 절차 확립 + Week 1~3 라우터 통합 유지 | Day 1, Day 2 |
| CISO | API 입력 검증, 민감정보 없는 로그, 감사 가능한 접근 경계, RAG 적재 PII 차단 | Day 2, Day 5 |
| CIO | retraining, monitoring, lineage, 장애 복구 절차 문서화 | Day 3~4 |
| COO | 모델 장애 시 fallback 과 운영 handoff 절차 | Day 2~4 |
| CFO | 운영 비용과 모델 성능 저하의 재학습 판단 기준 | Day 4 |
| CEO/CMO | 생성형 AI 답변 품질을 지속 개선할 수 있는 feedback loop (preference pair, human review) 설계 | Day 5 |

### 목표

1. MLflow Registry + 추가 metadata 로 모델 promotion gate 와 approval 흐름을 운영 가능하게 만든다.
2. Week 2~3 에서 보류한 XGBoost/LightGBM/Prophet/GMM/HDBSCAN 후보를 운영 후보 평가 항목으로 정리한다.
3. Week 1 `serving/app.py` 를 production-grade 로 진화시키되 fraud_router / business_insight_router 는 그대로 유지한다.
4. Airflow retraining DAG 에 SLA, retry, validation gate, rollback hook, lineage 기록을 추가한다.
5. task-aware (binary classifier / multi-output regression / clustering) prediction monitoring 테이블과 drift/quality report 를 만든다.
6. Week 1~4 문서를 RAG 지식 기반으로 ingest 하고 (PII 필터 포함) 운영 질문에 출처와 함께 답하는 assistant prototype 을 만든다.
7. **답변 평가 데이터셋 + preference pair + human review 의 feedback loop 를 설계**하고 RLHF/RLAIF 개념을 운영 개선 루프로 매핑한다.
8. CTO/CISO/CIO/CEO/CMO 수준의 인수인계 runbook 과 operational readiness report 를 완성한다.

### 일정 개요

| 일차 | 주제 | 핵심 작업 | 완료 산출물 |
|------|------|----------|-------------|
| Day 1 | MLflow Registry workflow + promotion gate | promotion_gates 단일 소스 모듈, register_model / promote_model 전체 코드, registry runbook | `mlops/promotion_gates.py`, `mlops/register_model.py`, `mlops/promote_model.py`, registry runbook |
| Day 2 | 후보 모델 평가 + serving/app.py 진화 (production hardening) | XGBoost/LightGBM/Prophet/GMM/HDBSCAN 후보 평가 기준, request_id 미들웨어, structured error, /health/live, /health/ready, fallback | `mlops/evaluate_model_candidates.py`, candidate report, `serving/app.py` 갱신, API runbook |
| Day 3 | Retraining DAG | gate 함수 호출하는 Airflow retraining, SLA, retry, lineage | `dags/ml_retraining_dag.py`, gate 통과/실패 테스트 |
| Day 4 | Task-aware Monitoring | 모델 task 별 metric, drift_metrics(PSI), ON CONFLICT, 자동 리포트 | `mlops/drift_metrics.py`, `mlops/monitor_predictions.py`, `schemas/model_monitoring.sql`, monitoring runbook |
| Day 5 | RAG + Feedback Loop + Handoff | PII filter, ingest_documents, query_assistant, llm_feedback 스키마, preference pair, RLHF 매핑, operational readiness report | RAG assets, llm-feedback-loop-design, operational_readiness_report |

### Day N 완료 기준 요약

* Day 1 완료 기준: `make register-model` / `make promote-model` 실행 시 promotion_gates 가 통과하고 MLflow Registered Model 의 stage 가 변경되며 approval metadata 가 기록된다.
* Day 2 완료 기준: 후보 모델 평가표가 작성되고, `serving/app.py` 가 `/health/live` (process), `/health/ready` (artifact + DB ready) 를 분리 응답하며 invalid payload 가 정의된 error response (422 + request_id) 로 반환된다.
* Day 3 완료 기준: `make retraining-dag-validate` 가 통과하고 metric gate 실패 시 promotion 단계가 중단되는 동작이 테스트로 검증된다.
* Day 4 완료 기준: `make monitoring-snapshot` 실행 시 fraud / forecast / segment task 별 snapshot 이 task-aware 컬럼으로 적재되고 drift 상태가 PSI 기반으로 판정되며 monitoring report 가 자동 생성된다.
* Day 5 완료 기준: `make rag-ingest`, `make rag-query` 가 PII 필터를 통과한 문서로 답변하고 `llm_feedback` 테이블에 평가/선호 쌍 데이터가 적재되며 `data/reports/mlops_operational_readiness_report_*.md` 가 모든 component 의 ready 여부를 한 페이지로 정리한다.

---

## Week 1~3 자산 상속 기준

| 기존 자산 | Week 4 사용 방식 |
|-----------|------------------|
| Week 1 `serving/app.py` | production hardening 으로 진화. **별도 production_app.py 를 만들지 않는다.** |
| Week 1 `models/eval_report.py` | Day 4 monitoring report 의 시그니처 패턴 재사용 |
| Week 2 `features/risk_reasons.py` | 변경 금지. RAG 답변 시 인용 근거 |
| Week 2 `models/anomaly_scorer.py` | "단일 소스 헬퍼" 패턴의 선례. Week 4 의 `promotion_gates.py`, `drift_metrics.py`, `pii_filter.py` 가 같은 패턴 |
| Week 2 `serving/fraud_router.py` | Week 4 app.py 진화 후에도 그대로 mount |
| Week 3 `models/forecast_intervals.py` | 동일 패턴 인용 |
| Week 3 `features/segment_actions.py` | 동일 패턴 인용 |
| Week 3 `serving/business_insight_router.py` | Week 4 app.py 진화 후에도 그대로 mount |
| Week 1 `feature_store`, Week 3 `feature_store_forecast`, `feature_store_segment` | retraining DAG 의 입력 |
| `prediction_result` | task-aware monitoring 입력 (forecast/segment 의 인공 ID 포함) |
| `model_evaluation` | promotion gate 의 metric 입력 |
| Week 2 `risk_score`, Week 3 `revenue_forecast`, `customer_segment`, `segment_action_recommendation` | task-aware monitoring 의 도메인 입력 |
| `data_lineage` | retraining lineage + rollback 설명 근거 + RAG 답변 근거 |
| MLflow Registered Models | Day 1 promotion gate 의 후보 출처 |
| `docs/guides`, `docs/models`, `docs/reports`, `docs/mlops` | RAG ingestion 소스 (PII 필터 후) |
| `.env` 의 `UPSTREAM_SILVER_PATH` | Week 4 에서는 사용하지 않음 (변경 없음) |

Week 4 에서는 Week 1~3 모델 로직과 라우터 코드를 절대 수정하지 않는다. 운영화 레이어만 추가한다.

---

## Day 1: MLflow Registry Workflow + Promotion Gate

### 1-0. Promotion 정책과 Stage 매핑

운영 승격은 성능이 좋아 보인다는 이유만으로 하지 않는다. 최소한 다음 gate 를 모두 통과해야 한다.

| Gate | 정의 | 검증 방법 |
|------|------|----------|
| Artifact 존재 | fitted pipeline, columns, threshold/action policy 등 metadata 가 joblib 안에 있음 | `data/artifacts/<model>/<version>/model.joblib` 로드 후 키 확인 |
| Evaluation 존재 | `model_evaluation` 에 task-specific metric 적재 | task 별 필수 metric 목록 (아래 1-1) |
| Lineage 존재 | `data_lineage` 에 train/predict 단계 run 기록 | 같은 model_version 으로 stage in (`train`, `feature`, `mart`) |
| Metric Gate | task 별 임계값 통과 | fraud: precision ≥ 0.4 & PR-AUC ≥ 0.5; forecast: amount_weighted_mape ≤ 0.20; segment: silhouette ≥ 0.20 & stability_overlap ≥ 0.55 |
| Approval Metadata | approver / reason / target stage 기록 | `model_promotion_log` 테이블 (Day 1 신설) |

Stage 명명: 본 가이드는 MLflow Registry 표준에 맞추어 **`None / Staging / Production / Archived`** 4단계를 사용한다. 가이드 본문에서 "dev / staging / production / archived" 표기를 보더라도 실제 호출은 MLflow 표준 (대문자 시작) 으로 한다.

### 1-1. `mlops/promotion_gates.py` — 단일 소스 gate 룰

Week 2 의 `risk_reasons.py`, Week 3 의 `segment_actions.py` 와 같은 단일 소스 헬퍼 패턴이다.

```python
# mlops/promotion_gates.py
"""모델 promotion 의 task-별 gate 를 단일 모듈에서 관리한다."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import joblib
import pandas as pd
from sqlalchemy import text


@dataclass(frozen=True)
class GateResult:
    name: str
    passed: bool
    detail: str


# 모델별 metric 임계값 정의. 새 모델이 추가되면 여기 한 곳만 수정한다.
METRIC_GATES: Mapping[str, list[tuple[str, str, float]]] = {
    # (metric_name, comparator, threshold) — comparator: ">=", "<="
    "nexuspay_baseline_classifier": [
        ("precision", ">=", 0.30),
        ("pr_auc", ">=", 0.40),
    ],
    "nexuspay_fraud_classifier": [
        ("precision", ">=", 0.40),
        ("pr_auc", ">=", 0.50),
        ("recall", ">=", 0.30),
    ],
    "nexuspay_revenue_forecast": [
        ("amount_weighted_mape", "<=", 0.20),
        ("cv_mae_mean", "<=", 5_000_000.0),
    ],
    "nexuspay_customer_segment": [
        ("silhouette", ">=", 0.20),
        ("stability_overlap", ">=", 0.55),
    ],
}

REQUIRED_ARTIFACT_KEYS: Mapping[str, list[str]] = {
    "nexuspay_baseline_classifier":  ["pipeline", "columns", "threshold"],
    "nexuspay_fraud_classifier":     ["pipeline", "columns", "threshold", "model_name", "model_version"],
    "nexuspay_revenue_forecast":     ["pipeline", "numerical", "categorical", "targets", "amount_idx", "pi_cfg"],
    "nexuspay_customer_segment":     ["pipeline", "columns", "n_clusters", "policy", "centroids", "stability"],
}


def check_artifact(model_name: str, version: str, model_root: str = "data/artifacts") -> GateResult:
    path = Path(model_root) / model_name / version / "model.joblib"
    if not path.exists():
        return GateResult("artifact_exists", False, f"missing: {path}")
    try:
        bundle = joblib.load(path)
    except Exception as exc:  # noqa: BLE001
        return GateResult("artifact_exists", False, f"load failed: {exc}")
    required = REQUIRED_ARTIFACT_KEYS.get(model_name, ["pipeline"])
    missing = [k for k in required if k not in bundle]
    if missing:
        return GateResult("artifact_keys", False, f"missing keys: {missing}")
    return GateResult("artifact_keys", True, f"keys ok: {required}")


def check_metrics(engine, model_name: str, version: str) -> list[GateResult]:
    rules = METRIC_GATES.get(model_name)
    if not rules:
        return [GateResult("metric_gate", False, f"no METRIC_GATES rule for {model_name}")]
    sql = text(
        "SELECT metric_name, metric_value FROM model_evaluation "
        "WHERE model_name = :mn AND model_version = :mv"
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"mn": model_name, "mv": version}).mappings().all()
    by_metric = {r["metric_name"]: float(r["metric_value"]) for r in rows}
    results: list[GateResult] = []
    for metric, op, threshold in rules:
        if metric not in by_metric:
            results.append(GateResult(f"metric:{metric}", False, f"missing metric in model_evaluation"))
            continue
        v = by_metric[metric]
        ok = (v >= threshold) if op == ">=" else (v <= threshold)
        results.append(GateResult(f"metric:{metric}", ok, f"{v:.4f} {op} {threshold}"))
    return results


def check_lineage(engine, run_id: str | None, model_name: str) -> GateResult:
    if not run_id:
        return GateResult("lineage", False, "run_id missing")
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT COUNT(*) AS c FROM data_lineage WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar()
    return GateResult("lineage", int(row or 0) > 0, f"data_lineage rows={row} for run_id={run_id}")


def evaluate_all(engine, model_name: str, version: str, run_id: str | None = None) -> list[GateResult]:
    out = [check_artifact(model_name, version)]
    out += check_metrics(engine, model_name, version)
    out.append(check_lineage(engine, run_id, model_name))
    return out


def gates_passed(results: list[GateResult]) -> bool:
    return all(r.passed for r in results)
```

### 1-2. `schemas/model_promotion_log.sql` — approval metadata 영속화

```sql
-- schemas/model_promotion_log.sql
CREATE TABLE IF NOT EXISTS model_promotion_log (
    id              BIGSERIAL PRIMARY KEY,
    model_name      VARCHAR(80) NOT NULL,
    model_version   VARCHAR(64) NOT NULL,
    from_stage      VARCHAR(20),
    to_stage        VARCHAR(20) NOT NULL,
    approver        VARCHAR(80) NOT NULL,
    reason          TEXT,
    gate_results    JSONB,
    promoted_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_promotion_log_model
    ON model_promotion_log (model_name, model_version);
```

### 1-3. `mlops/register_model.py` — Registered Model 후보 보장

Week 1~3 학습 스크립트는 이미 `mlflow.sklearn.log_model(registered_model_name=...)` 로 모델을 등록한다. 본 스크립트는 누락된 케이스를 보강하고 metadata 를 채우는 보조 진입점이다.

```python
# mlops/register_model.py
"""기존 MLflow run 의 artifact 를 Registered Model 후보로 보강한다."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import mlflow
from mlflow.exceptions import RestException
from mlflow.tracking import MlflowClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--run-id", required=True, help="MLflow run id 가진 sklearn_model artifact")
    parser.add_argument("--description", default="")
    args = parser.parse_args()

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    client = MlflowClient()

    # Registered Model 가 없으면 만든다 (이미 있으면 통과)
    try:
        client.create_registered_model(args.model_name, description=args.description or args.model_name)
    except RestException as exc:
        if "RESOURCE_ALREADY_EXISTS" not in str(exc):
            raise

    # 새 버전 생성 (run 의 sklearn_model artifact 기반)
    src = f"runs:/{args.run_id}/sklearn_model"
    mv = client.create_model_version(name=args.model_name, source=src, run_id=args.run_id)
    print(f"registered {args.model_name} version={mv.version} (stage={mv.current_stage})")


if __name__ == "__main__":
    main()
```

### 1-4. `mlops/promote_model.py` — Gate + Stage 전환 + Approval

```python
# mlops/promote_model.py
"""promotion_gates 를 통과한 모델을 MLflow stage 전환하고 approval 을 영속화한다."""
from __future__ import annotations

import argparse
import json
import os

import mlflow
from mlflow.tracking import MlflowClient
from sqlalchemy import create_engine, text

from mlops.promotion_gates import evaluate_all, gates_passed


VALID_STAGES = {"None", "Staging", "Production", "Archived"}


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--version", required=True, help="MLflow Registered Model version (예: 3) 또는 artifact 폴더명")
    parser.add_argument("--stage", required=True, choices=sorted(VALID_STAGES))
    parser.add_argument("--approver", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--run-id", help="data_lineage 검증용 MLflow run id")
    parser.add_argument("--dry-run", action="store_true", help="gate 만 평가하고 stage 전환은 안 함")
    args = parser.parse_args()

    engine = _engine()
    results = evaluate_all(engine, args.model_name, args.version, args.run_id)

    print("Gate evaluation:")
    for r in results:
        print(f"  [{ 'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    if not gates_passed(results):
        print(">>> gate failed — promotion aborted")
        raise SystemExit(2)

    if args.dry_run:
        print(">>> dry-run: gate passed, skipping MLflow stage transition")
        return

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    client = MlflowClient()
    current = client.get_model_version(name=args.model_name, version=args.version)
    from_stage = current.current_stage
    client.transition_model_version_stage(
        name=args.model_name,
        version=args.version,
        stage=args.stage,
        archive_existing_versions=(args.stage == "Production"),
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO model_promotion_log(model_name, model_version, from_stage, to_stage, "
                "approver, reason, gate_results) "
                "VALUES (:mn, :mv, :fs, :ts, :ap, :rs, :gr)"
            ),
            {
                "mn": args.model_name,
                "mv": args.version,
                "fs": from_stage,
                "ts": args.stage,
                "ap": args.approver,
                "rs": args.reason,
                "gr": json.dumps([r.__dict__ for r in results]),
            },
        )

    print(f">>> promoted {args.model_name} v{args.version} {from_stage} -> {args.stage} (approver={args.approver})")


if __name__ == "__main__":
    main()
```

### 1-5. Runbook 자동 생성용 시드

```bash
mkdir -p docs/mlops
cat > docs/mlops/model-registry-runbook.md << 'EOF'
# Nexus Pay Model Registry Runbook — Week 4

## 1. 등록 대상 모델

| 모델 | 책임 | 필수 metric gate |
|------|------|-----------------|
| nexuspay_baseline_classifier | Week 1 baseline | precision ≥ 0.30, pr_auc ≥ 0.40 |
| nexuspay_fraud_classifier    | Week 2 fraud   | precision ≥ 0.40, pr_auc ≥ 0.50, recall ≥ 0.30 |
| nexuspay_revenue_forecast    | Week 3 forecast | amount_weighted_mape ≤ 0.20, cv_mae_mean ≤ 5,000,000 |
| nexuspay_customer_segment    | Week 3 segment | silhouette ≥ 0.20, stability_overlap ≥ 0.55 |

## 2. Stage 정의

| Stage | 의미 |
|-------|------|
| None | 학습만 끝난 후보 |
| Staging | gate 통과, 운영 전 검증 단계 |
| Production | 운영 배포된 단일 버전 (transition 시 동일 모델 기존 Production 자동 archive) |
| Archived | 더 이상 사용하지 않는 버전 |

## 3. Promotion Gate

`mlops/promotion_gates.py` 의 `evaluate_all` 결과가 모두 PASS 여야 한다. 룰 변경 시 본 모듈 단일 위치만 수정한다.

## 4. 명령 예시

```bash
make register-model MODEL=nexuspay_fraud_classifier RUN_ID=<mlflow-run-id>
make promote-model  MODEL=nexuspay_fraud_classifier VERSION=3 STAGE=Staging \
                    APPROVER=ml-platform-lead REASON="Week 2 gate passed"
```

## 5. Rollback

`Production` 으로 올린 버전이 monitoring snapshot 에서 `FAIL` 을 띄우면 직전 버전을 다시 `Staging→Production` 으로 transition 한다. 모든 transition 은 `model_promotion_log` 에 남는다.
EOF
```

### 1-6. Makefile 타겟

Week 1 `Makefile` 끝에 다음을 추가한다.

```makefile
.PHONY: registry-schema register-model promote-model registry-check candidate-evaluation

registry-schema:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-mluser} -d $${POSTGRES_DB:-ml_db} < schemas/model_promotion_log.sql

register-model:
	docker compose exec runner python -m mlops.register_model --model-name $${MODEL} --run-id $${RUN_ID}

promote-model:
	docker compose exec runner python -m mlops.promote_model \
	  --model-name $${MODEL} --version $${VERSION} --stage $${STAGE} \
	  --approver $${APPROVER} --reason $${REASON} $${EXTRA_ARGS}

# 등록·승격 흐름이 깨지지 않았는지 점검 (gate 만 dry-run)
registry-check:
	docker compose exec runner python -m mlops.promote_model \
	  --model-name $${MODEL} --version $${VERSION} --stage Staging \
	  --approver gate-check --reason "dry-run gate evaluation" --dry-run

candidate-evaluation:
	docker compose exec runner python -m mlops.evaluate_model_candidates \
	  --output docs/mlops/model-candidate-evaluation.md
```

### Day 1 완료 기준

* `make registry-schema` 후 `\d model_promotion_log` 가 성공한다.
* `mlops/promotion_gates.py`, `mlops/register_model.py`, `mlops/promote_model.py` 가 모두 import 가능하다.
* `make registry-check MODEL=... VERSION=...` 가 gate 결과를 PASS/FAIL 단위로 출력한다.
* `make candidate-evaluation` 이 Week 2~3 baseline 과 Week 4 후속 후보를 비교한 markdown report 를 생성한다.
* gate 가 모두 PASS 인 모델 1개에 대해 `make promote-model ... STAGE=Staging` 이 성공하고 MLflow UI 에 stage 가 변경되며 `model_promotion_log` 에 row 가 남는다.
* `docs/mlops/model-registry-runbook.md` 가 모델별 gate 와 명령 예시를 포함한다.

---

## Day 2: serving/app.py 진화 (Production Hardening)

### 2-0. 후속 후보 모델 평가 항목

Week 2/3 에서는 학습 부하와 sklearn Pipeline 일관성을 우선해 모델 후보를 좁혔다. Week 4 에서는 새 모델을 무조건 production 으로 올리는 것이 아니라, 운영 후보로 비교할 기준과 실험 진입점을 만든다.

| 과제 | Week 1~3 기준선 | Week 4 후속 후보 | 평가 기준 |
|------|----------------|------------------|-----------|
| fraud classifier | Logistic Regression / Random Forest | XGBoost, LightGBM | PR-AUC, recall@alert_capacity, latency, artifact size |
| revenue forecast | MultiOutput RandomForest | LightGBM forecaster, Prophet | amount-weighted MAPE, 피크 시간대 recall, 재학습 시간 |
| customer segment | KMeans | GMM, HDBSCAN | silhouette, stability_overlap, segment size balance, action 해석 가능성 |

`mlops/evaluate_model_candidates.py` 는 후보 모델을 직접 모두 학습하기보다, registry/evaluation/monitoring 기준으로 후보 실험을 비교할 수 있는 입력 양식을 만든다. XGBoost/LightGBM/Prophet/GMM/HDBSCAN 은 optional dependency 로 두며, 설치되지 않았으면 해당 후보를 `SKIPPED` 로 기록한다.

`docs/mlops/model-candidate-evaluation.md` 에는 다음을 포함한다.

* 후보 모델별 도입 목적
* 추가 의존성 및 운영 비용
* baseline 대비 기대 개선 지표
* latency / memory / retraining time 위험
* promotion gate 를 통과하기 위한 최소 기준

#### `mlops/evaluate_model_candidates.py`

이 스크립트는 Week 4 에서 후보 모델을 바로 production 으로 올리기 전에, 어떤 후보가 어떤 조건에서 다음 실험으로 넘어갈 수 있는지 정리한다. 실제 학습은 각 후보 dependency 와 데이터 준비가 끝난 뒤 별도 실험으로 수행하되, 이 단계에서는 CTO/ML lead 가 승인할 수 있는 후보 평가 기준표를 먼저 만든다.

```python
# mlops/evaluate_model_candidates.py
"""Week 2~3 baseline 대비 Week 4 후속 모델 후보 평가표를 생성한다.

이 파일은 XGBoost, LightGBM, Prophet, GMM, HDBSCAN 을 즉시 production 모델로
학습시키는 코드가 아니다. 운영 도입 전에 dependency, metric, latency, 재학습 비용,
promotion gate 를 한 표로 정리해 후속 실험의 진입 조건을 명확히 한다.
"""

from __future__ import annotations

import argparse
import importlib.util
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CandidateSpec:
    task: str
    baseline: str
    candidate: str
    package: str
    metric: str
    promotion_gate: str
    operation_risk: str


CANDIDATES = [
    CandidateSpec(
        task="fraud_classifier",
        baseline="Logistic Regression / Random Forest",
        candidate="XGBoost",
        package="xgboost",
        metric="PR-AUC, recall@alert_capacity, p95 latency",
        promotion_gate="PR-AUC +0.02 이상 또는 동일 PR-AUC 에서 p95 latency 20% 이내",
        operation_risk="트리 수 증가에 따른 artifact 크기와 online scoring latency",
    ),
    CandidateSpec(
        task="fraud_classifier",
        baseline="Logistic Regression / Random Forest",
        candidate="LightGBM",
        package="lightgbm",
        metric="PR-AUC, recall@alert_capacity, p95 latency",
        promotion_gate="alert capacity 고정 조건에서 false negative cost 감소",
        operation_risk="카테고리 feature 처리 방식과 wheel 설치 호환성",
    ),
    CandidateSpec(
        task="revenue_forecast",
        baseline="MultiOutput RandomForest",
        candidate="LightGBM forecaster",
        package="lightgbm",
        metric="amount-weighted MAPE, peak recall, retraining time",
        promotion_gate="MAPE 10% 이상 개선 또는 피크 시간대 recall 개선",
        operation_risk="horizon 별 모델 수 증가와 재학습 batch 시간",
    ),
    CandidateSpec(
        task="revenue_forecast",
        baseline="MultiOutput RandomForest",
        candidate="Prophet",
        package="prophet",
        metric="MAPE, holiday uplift error, retraining time",
        promotion_gate="명절/월말 패턴 설명력이 baseline 보다 명확할 것",
        operation_risk="추가 dependency 무게와 컨테이너 빌드 시간",
    ),
    CandidateSpec(
        task="customer_segment",
        baseline="KMeans",
        candidate="GMM",
        package="sklearn",
        metric="silhouette, stability_overlap, segment size balance",
        promotion_gate="segment stability 를 유지하면서 action 해석 가능성 개선",
        operation_risk="확률 기반 segment 설명을 stakeholder 문구로 전환해야 함",
    ),
    CandidateSpec(
        task="customer_segment",
        baseline="KMeans",
        candidate="HDBSCAN",
        package="hdbscan",
        metric="cluster persistence, noise ratio, action coverage",
        promotion_gate="noise 고객군이 운영 action 으로 분리 가능할 것",
        operation_risk="optional dependency 설치와 작은 sample 에서의 불안정성",
    ),
]


def dependency_status(package: str) -> str:
    return "AVAILABLE" if importlib.util.find_spec(package) else "SKIPPED"


def render_markdown(specs: list[CandidateSpec]) -> str:
    lines = [
        "# Nexus Pay Week 4 Model Candidate Evaluation",
        "",
        "Week 2~3 baseline 모델을 production 후보로 유지하면서, Week 4 에서 검토할 후속 후보의 실험 진입 기준을 정리한다.",
        "",
        "| Task | Baseline | Candidate | Dependency | Metric | Promotion gate | Operation risk |",
        "|------|----------|-----------|------------|--------|----------------|----------------|",
    ]
    for spec in specs:
        lines.append(
            "| "
            + " | ".join(
                [
                    spec.task,
                    spec.baseline,
                    spec.candidate,
                    dependency_status(spec.package),
                    spec.metric,
                    spec.promotion_gate,
                    spec.operation_risk,
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Handoff Notes",
            "",
            "- `SKIPPED` 후보는 dependency 를 설치하지 않았다는 뜻이며, 후보 자체를 기각했다는 뜻이 아니다.",
            "- 후보 모델은 동일 feature contract, 동일 split policy, 동일 evaluation window 에서만 baseline 과 비교한다.",
            "- promotion 은 metric 개선만으로 승인하지 않고 latency, artifact size, retraining time, rollback 가능성을 함께 본다.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="docs/mlops/model-candidate-evaluation.md")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(CANDIDATES), encoding="utf-8")
    print(f"candidate evaluation report written: {output_path}")


if __name__ == "__main__":
    main()
```

코드 해설:

* `CandidateSpec` 은 후보 모델의 목적, dependency, 평가 metric, promotion gate, 운영 위험을 한 곳에서 관리한다.
* `dependency_status()` 는 optional dependency 설치 여부를 확인한다. 설치되지 않은 후보는 실패로 중단하지 않고 `SKIPPED` 로 기록한다.
* `render_markdown()` 은 CTO/ML lead 가 검토할 수 있는 markdown 표를 생성한다.
* `main()` 은 기본 출력 경로를 `docs/mlops/model-candidate-evaluation.md` 로 두어 Week 4 산출물 체크리스트와 바로 연결한다.

### 2-1. 별도 production_app 을 만들지 않는 이유

Week 2 에서 fraud_router 통합으로 라우터 계층을 정리했고, Week 3 에서 business_insight_router 도 같은 방식으로 mount 했다. Week 4 가 새 `production_app.py` 를 만들면 fraud_router/business_insight_router 의 mount 지점이 둘로 갈라지고, docker-compose 의 `serving.app:app` 명령과 일관성이 깨진다.

따라서 Week 4 는 **`serving/app.py` 자체에 production hardening 을 추가**한다. 라우터들은 그대로 mount 되고, 컨테이너 / 포트 / Makefile 도 그대로다.

### 2-2. 추가될 production hardening

| 영역 | 추가 내용 |
|------|-----------|
| Liveness/Readiness 분리 | `/health/live` (프로세스 살아있음만 확인), `/health/ready` (artifact 로딩 + DB 연결 확인) |
| request_id 미들웨어 | 모든 응답에 `X-Request-ID` 헤더 + 응답 body 의 `request_id` 필드 |
| structured error handler | `RequestValidationError`, `HTTPException` 모두 동일 JSON 스키마로 반환 |
| sensitive log 차단 | request body 의 `amount`, `customer_id`, `transaction_id` 는 hash 만 로그 |
| fallback 정책 | artifact 미존재 시 503 + `Retry-After` 힌트 |

### 2-3. `serving/app.py` 변경 (전체)

Week 1 의 baseline endpoint(`POST /predict`, `GET /predictions/{id}`)와 Week 2/3 의 라우터 mount 는 그대로 유지하고, 상단에 hardening 코드만 추가한다.

```python
# serving/app.py — Week 4 production hardening 후 전체 모습
from __future__ import annotations

import hashlib
import logging
import os
import time
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text


app = FastAPI(title="Nexus Pay ML API", version="1.0.0")

logger = logging.getLogger("nexuspay.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")


# ──────────────────────────────────────
# Middleware: request_id, structured logging
# ──────────────────────────────────────
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    _request_id_ctx.set(req_id)
    started = time.time()
    response = await call_next(request)
    elapsed = (time.time() - started) * 1000
    response.headers["X-Request-ID"] = req_id
    logger.info(
        "request_id=%s method=%s path=%s status=%d elapsed_ms=%.1f",
        req_id, request.method, request.url.path, response.status_code, elapsed,
    )
    return response


# ──────────────────────────────────────
# Structured error handlers
# ──────────────────────────────────────
def _error_body(code: str, detail: Any, status: int) -> dict:
    return {
        "request_id": _request_id_ctx.get(),
        "code": code,
        "status": status,
        "detail": detail,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content=_error_body("invalid_request", exc.errors(), 422))


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(f"http_{exc.status_code}", exc.detail, exc.status_code),
    )


# ──────────────────────────────────────
# Liveness / Readiness
# ──────────────────────────────────────
@app.get("/health/live")
def live():
    return {"status": "live", "request_id": _request_id_ctx.get()}


def _check_db() -> bool:
    try:
        url = (
            f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
            f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
        )
        engine = create_engine(url, future=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


def _has_any_artifact() -> bool:
    root = Path("data/artifacts")
    if not root.exists():
        return False
    return any(root.glob("*/*/model.joblib"))


@app.get("/health/ready")
def ready():
    components = {
        "process": True,
        "database": _check_db(),
        "model_artifact": _has_any_artifact(),
    }
    if not all(components.values()):
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "components": components,
                "request_id": _request_id_ctx.get(),
                "retry_after_seconds": 30,
            },
            headers={"Retry-After": "30"},
        )
    return {"status": "ready", "components": components, "request_id": _request_id_ctx.get()}


# ──────────────────────────────────────
# Sensitive value hashing helper
# ──────────────────────────────────────
def _hash_sensitive(value: str) -> str:
    if value is None:
        return "-"
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:10]


# ──────────────────────────────────────
# Week 1 baseline /predict 유지 (logging 만 보강)
# ──────────────────────────────────────
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
    request_id: str
    transaction_id: str
    model_name: str
    model_version: str
    score: float
    prediction: str
    scored_at: datetime


MODEL_NAME = os.environ.get("MODEL_NAME", "nexuspay_baseline_classifier")
_BASELINE_BUNDLE = None


def _latest(model_name: str) -> Path:
    candidates = sorted((Path("data/artifacts") / model_name).glob("*/model.joblib"))
    if not candidates:
        raise HTTPException(status_code=503, detail=f"no artifact for {model_name}")
    return candidates[-1]


def _baseline_bundle():
    global _BASELINE_BUNDLE
    if _BASELINE_BUNDLE is None:
        path = _latest(MODEL_NAME)
        bundle = joblib.load(path)
        bundle["version"] = path.parent.name
        _BASELINE_BUNDLE = bundle
    return _BASELINE_BUNDLE


@app.post("/predict", response_model=PredictionResponse)
def predict(req: TransactionRequest):
    bundle = _baseline_bundle()
    pipe = bundle["pipeline"]
    columns = bundle["columns"]
    threshold = bundle["threshold"]

    df = pd.DataFrame([{
        "amount": req.amount,
        "amount_zscore_30d": req.amount_zscore_30d,
        "tx_count_24h": req.tx_count_24h,
        "days_since_last_tx": req.days_since_last_tx,
        "channel": req.channel,
        "device_country": req.device_country,
        "merchant_segment": req.merchant_segment,
    }]).reindex(columns=columns)

    proba = float(pipe.predict_proba(df)[0, 1])
    pred = "POSITIVE" if proba >= threshold else "NEGATIVE"

    logger.info(
        "predict request_id=%s tx=%s customer_hash=- amount_band=%s",
        _request_id_ctx.get(),
        _hash_sensitive(req.transaction_id),
        "high" if req.amount > 100000 else "normal",
    )
    return PredictionResponse(
        request_id=_request_id_ctx.get(),
        transaction_id=req.transaction_id,
        model_name=MODEL_NAME,
        model_version=bundle["version"],
        score=round(proba, 4),
        prediction=pred,
        scored_at=datetime.now(timezone.utc),
    )


# ──────────────────────────────────────
# Week 2/3 라우터 mount (변경 없음)
# ──────────────────────────────────────
from serving.fraud_router import router as fraud_router  # noqa: E402
from serving.business_insight_router import router as business_insight_router  # noqa: E402

app.include_router(fraud_router)
app.include_router(business_insight_router)
```

> 호환성 노트: 기존 `/health` 가 baseline 테스트에서 사용되고 있다면 별칭으로 `/health/live` 를 default `/health` 로 alias 할 수 있다 (`@app.get("/health")(live)` 또는 라우트 두 개 등록). 본 가이드는 **두 개 모두 등록** 권장.

### 2-4. `docs/mlops/api-hardening-runbook.md`

```bash
cat > docs/mlops/api-hardening-runbook.md << 'EOF'
# Nexus Pay API Hardening Runbook — Week 4

## 1. Endpoint Contract

| Endpoint | 책임 | 실패 응답 |
|----------|------|----------|
| GET  /health/live  | 프로세스 alive | 항상 200 |
| GET  /health/ready | DB + artifact ready | 503 + Retry-After |
| POST /predict      | Week 1 baseline | 422 invalid / 503 missing artifact |
| POST /fraud/score  | Week 2 fraud | 동일 |
| GET  /business/forecast?date=... | Week 3 forecast 조회 | 동일 |
| GET  /business/segments/{id}     | Week 3 segment 조회 | 404 / 422 |

## 2. Readiness 실패 유형

- `database=false` : PostgreSQL 연결 실패 → 인프라 점검
- `model_artifact=false` : `data/artifacts/` 비어 있음 → 학습 작업 선행
- `process=false` : 프로세스 자체 이상 (관측 불가; liveness 와 동시 실패)

## 3. Error Response 카탈로그

```json
{
  "request_id": "uuid",
  "code": "invalid_request | http_404 | http_503 | ...",
  "status": 422,
  "detail": "...",
  "occurred_at": "ISO-8601 UTC"
}
```

## 4. Sensitive Logging 정책

- `transaction_id`, `customer_id` 는 SHA1 prefix 10자만 로그.
- `amount` 는 절대값 대신 band(`high`/`normal`)로 기록.
- request body 전체를 그대로 로깅하지 않는다.

## 5. Fallback / Rollback

- `/health/ready` 가 503 인 동안 클라이언트는 30초 backoff.
- 모델 promotion 직후 `/health/ready=503` 이 지속되면 Week 4 promotion log 의 직전 Production 버전을 다시 transition 한다.
EOF
```

### Day 2 완료 기준

* `serving/app.py` 가 별도 파일을 만들지 않고 hardening 을 흡수한다 (request_id 미들웨어 + 분리된 health endpoint + structured error).
* `make candidate-evaluation` 실행 결과 `docs/mlops/model-candidate-evaluation.md` 에 XGBoost/LightGBM/Prophet/GMM/HDBSCAN 후보별 도입 목적, 평가 metric, optional dependency 상태, promotion gate 가 정리된다.
* 컨테이너 재기동 후 `curl -fsS http://localhost:8000/health/live` 가 200 OK 로 돌아오고 `/health/ready` 는 DB + artifact 존재 여부에 따라 200 또는 503 을 반환한다.
* `POST /predict` 에 잘못된 payload 를 보낼 때 422 + `request_id` 가 포함된 JSON 응답이 나온다.
* `make api-hardening-test` 가 health/live, health/ready, fraud/score, business/segments/summary 모두 200 으로 응답하는지 확인한다.

---

## Day 3: Airflow Retraining Pipeline

### 3-0. Retraining 정책

재학습은 매번 Production 승격을 의미하지 않는다. Week 4 DAG 는 **학습과 승격을 분리** 한다.

1. 데이터 품질 점검 (raw_transactions row 수, fraud_rate band)
2. feature build (모델별)
3. train + evaluate (Week 1~3 학습 스크립트 그대로 호출)
4. validation gate (Day 1 의 `promotion_gates.evaluate_all`)
5. gate PASS 시 registry candidate 등록
6. 별도 promotion 요청은 manual `make promote-model` 로 분리

DAG 는 모델 종류를 인자로 받는다 (`MODEL=baseline|fraud|forecast|segment`).

### 3-1. `dags/ml_retraining_dag.py`

```python
# dags/ml_retraining_dag.py
"""Nexus Pay 재학습 DAG. 모델 종류는 환경 변수 MODEL 로 분기."""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


DEFAULT_ARGS = {
    "owner": "ml-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}

# 모델별 DAG step 정의. 각 step 은 ("task_id", "command", [upstreams])
PIPELINE_BY_MODEL: dict[str, list[tuple[str, str, list[str]]]] = {
    "fraud": [
        ("data_quality_check", "python -m mlops.data_quality_check --domain fraud", []),
        ("fraud_features",     "python -m features.build_fraud_features --config config/fraud_model_config.yaml", ["data_quality_check"]),
        ("fraud_train",        "python -m models.train_fraud_classifier --config config/fraud_model_config.yaml", ["fraud_features"]),
        ("fraud_anomaly",      "python -m models.train_anomaly_detector --config config/fraud_model_config.yaml", ["fraud_features"]),
        ("fraud_threshold",    "python -m models.tune_fraud_threshold --config config/fraud_model_config.yaml", ["fraud_train"]),
        ("validate_metric_gate", "python -m mlops.gate_check --model nexuspay_fraud_classifier --version-from-latest", ["fraud_threshold"]),
        ("register_candidate", "python -m mlops.register_model --model-name nexuspay_fraud_classifier --run-id $(cat /tmp/last_run_id)", ["validate_metric_gate"]),
    ],
    "forecast": [
        ("data_quality_check", "python -m mlops.data_quality_check --domain forecast", []),
        ("forecast_features",  "python -m features.build_forecast_features --config config/forecast_model_config.yaml", ["data_quality_check"]),
        ("forecast_train",     "python -m models.train_revenue_forecast --config config/forecast_model_config.yaml", ["forecast_features"]),
        ("validate_metric_gate", "python -m mlops.gate_check --model nexuspay_revenue_forecast --version-from-latest", ["forecast_train"]),
        ("register_candidate", "python -m mlops.register_model --model-name nexuspay_revenue_forecast --run-id $(cat /tmp/last_run_id)", ["validate_metric_gate"]),
    ],
    "segment": [
        ("data_quality_check", "python -m mlops.data_quality_check --domain segment", []),
        ("segment_features",   "python -m features.build_segment_features --config config/segment_model_config.yaml", ["data_quality_check"]),
        ("segment_train",      "python -m models.train_customer_segments --config config/segment_model_config.yaml", ["segment_features"]),
        ("validate_metric_gate", "python -m mlops.gate_check --model nexuspay_customer_segment --version-from-latest", ["segment_train"]),
        ("register_candidate", "python -m mlops.register_model --model-name nexuspay_customer_segment --run-id $(cat /tmp/last_run_id)", ["validate_metric_gate"]),
    ],
}


def build_dag(model_key: str):
    if not AIRFLOW_AVAILABLE:
        raise RuntimeError("Airflow not installed; use --validate")
    if model_key not in PIPELINE_BY_MODEL:
        raise ValueError(f"unknown MODEL={model_key}; allowed: {list(PIPELINE_BY_MODEL)}")

    dag = DAG(
        dag_id=f"ml_retraining_{model_key}",
        description=f"Nexus Pay {model_key} model retraining",
        start_date=datetime(2026, 6, 15),
        schedule_interval="0 2 * * 1",   # 매주 월요일 새벽 2시
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["nexuspay", "retrain", model_key, "week4"],
    )

    ops = {}
    for task_id, command, _ in PIPELINE_BY_MODEL[model_key]:
        ops[task_id] = BashOperator(task_id=task_id, bash_command=command, dag=dag)
    for task_id, _, upstreams in PIPELINE_BY_MODEL[model_key]:
        for u in upstreams:
            ops[u] >> ops[task_id]
    return dag


def _validate(model_key: str | None = None) -> int:
    keys = [model_key] if model_key else list(PIPELINE_BY_MODEL)
    for k in keys:
        print(f"\nDAG ml_retraining_{k}:")
        for task_id, command, upstreams in PIPELINE_BY_MODEL[k]:
            ups = ", ".join(upstreams) if upstreams else "(root)"
            print(f"  - {task_id}  upstreams={ups}")
        last = PIPELINE_BY_MODEL[k][-1][0]
        if last != "register_candidate":
            print(f"unexpected last task for {k}: {last}", file=sys.stderr)
            return 1
    print("\nvalidation OK")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--model", help="검증 대상 단일 모델 (생략 시 전체)")
    args = parser.parse_args()
    if args.validate:
        sys.exit(_validate(args.model))

    if AIRFLOW_AVAILABLE:
        for key in PIPELINE_BY_MODEL:
            globals()[f"dag_{key}"] = build_dag(key)
    else:
        print("airflow not installed; run with --validate", file=sys.stderr)
        sys.exit(1)
```

### 3-2. `mlops/data_quality_check.py`

DAG 의 첫 step 으로 가벼운 입력 점검을 한다.

```python
# mlops/data_quality_check.py
"""raw_transactions / risk_score / feature_store_* 의 기본 위생 점검."""
from __future__ import annotations

import argparse
import os

from sqlalchemy import create_engine, text


CHECKS = {
    "fraud":    [("raw_transactions", "SELECT COUNT(*) FROM raw_transactions"),
                 ("risk_score",       "SELECT COUNT(*) FROM risk_score")],
    "forecast": [("raw_transactions", "SELECT COUNT(*) FROM raw_transactions"),
                 ("feature_store_forecast",
                  "SELECT COUNT(*) FROM feature_store_forecast WHERE feature_set='forecast_v1'")],
    "segment":  [("raw_transactions", "SELECT COUNT(*) FROM raw_transactions"),
                 ("feature_store_segment",
                  "SELECT COUNT(*) FROM feature_store_segment WHERE feature_set='customer_segment_v1'")],
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, choices=sorted(CHECKS))
    parser.add_argument("--min-rows", type=int, default=100)
    args = parser.parse_args()

    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    engine = create_engine(url, future=True)
    failed: list[str] = []
    with engine.connect() as conn:
        for name, sql in CHECKS[args.domain]:
            count = int(conn.execute(text(sql)).scalar() or 0)
            status = "OK" if count >= args.min_rows else "FAIL"
            print(f"  {status} {name}: {count:,} rows")
            if status == "FAIL":
                failed.append(name)
    if failed:
        raise SystemExit(f"data quality fail: {failed}")


if __name__ == "__main__":
    main()
```

### 3-3. `mlops/gate_check.py`

DAG 가 학습 직후 호출하는 gate 진입점. `version-from-latest` 옵션으로 가장 최근에 만들어진 artifact 폴더를 자동 선택한다.

```python
# mlops/gate_check.py
"""Day 1 promotion_gates 를 학습 직후 자동 평가하는 진입점."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import create_engine

from mlops.promotion_gates import evaluate_all, gates_passed


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _latest_version(model_name: str) -> str:
    candidates = sorted((Path("data/artifacts") / model_name).glob("*/model.joblib"))
    if not candidates:
        raise SystemExit(f"no artifact for {model_name}")
    return candidates[-1].parent.name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--version", help="명시 안 하면 최근 artifact 폴더명 사용")
    parser.add_argument("--version-from-latest", action="store_true")
    parser.add_argument("--run-id", help="lineage 검증용 MLflow run id")
    args = parser.parse_args()

    version = args.version or (_latest_version(args.model) if args.version_from_latest else None)
    if not version:
        raise SystemExit("either --version or --version-from-latest is required")

    engine = _engine()
    results = evaluate_all(engine, args.model, version, args.run_id)
    print(f"Gate evaluation for {args.model} v{version}:")
    for r in results:
        print(f"  [{ 'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    if not gates_passed(results):
        raise SystemExit(2)
    print("gate OK")


if __name__ == "__main__":
    main()
```

### 3-4. Retraining Runbook 시드

```bash
cat > docs/mlops/retraining-and-monitoring-runbook.md << 'EOF'
# Retraining & Monitoring Runbook — Week 4

## 1. 재학습 trigger

- schedule: 매주 월요일 02:00 KST (DAG `ml_retraining_<model>`)
- drift: monitoring snapshot 의 `status='FAIL'` 이 2 snapshot 연속이면 manual trigger
- manual: `make retraining-dag-validate` + Airflow UI 의 trigger DAG

## 2. Metric Gate 기준

`mlops/promotion_gates.py` 의 `METRIC_GATES` 가 단일 소스. 모델별 임계값은 본 모듈만 갱신한다.

## 3. 실패 복구

- `data_quality_check` 실패: 입력 테이블 row 수가 100 미만이면 Week 1~2 ingestion 재실행 (`make load-sample` 또는 `make fraud-import-upstream`).
- `validate_metric_gate` 실패: gate 결과를 검토 후 모델 후보를 archive 로 이동 (`make promote-model STAGE=Archived`).
- `register_candidate` 실패: MLflow Tracking 서버 health 확인 후 동일 task 재실행.

## 4. Rollback

`Production` 으로 올린 버전이 monitoring 에서 FAIL 인 경우, `model_promotion_log` 에서 직전 Production 버전을 찾아 다시 `Production` 으로 transition 한다. 이때 archive_existing_versions=True 가 자동 적용된다.
EOF
```

### 3-5. Makefile 타겟 추가

```makefile
.PHONY: retraining-dag-validate retraining-test data-quality-check gate-check

retraining-dag-validate:
	docker compose exec runner python -m dags.ml_retraining_dag --validate

retraining-test:
	docker compose exec runner pytest -q tests/test_mlops_end_to_end.py::test_retraining_dag_order

data-quality-check:
	docker compose exec runner python -m mlops.data_quality_check --domain $${DOMAIN}

gate-check:
	docker compose exec runner python -m mlops.gate_check --model $${MODEL} --version-from-latest
```

### Day 3 완료 기준

* `make retraining-dag-validate` 가 fraud/forecast/segment 3개 DAG 모두 마지막 task 가 `register_candidate` 임을 확인하고 `validation OK` 를 반환한다.
* `make data-quality-check DOMAIN=fraud` 가 raw_transactions, risk_score row 수를 출력하고 100 미만이면 비정상 종료한다.
* `make gate-check MODEL=nexuspay_fraud_classifier` 가 PASS/FAIL 결과를 출력하고 FAIL 이면 exit code 2 로 끝난다.
* `docs/mlops/retraining-and-monitoring-runbook.md` 가 trigger/gate/복구/rollback 4 절을 포함한다.

---

## Day 4: Monitoring

### 4-0. Monitoring 범위

Week 4 monitoring 은 실시간 관제 플랫폼을 만드는 단계가 아니라, Nexus Pay 운영팀이 매일 같은 방식으로 확인할 수 있는 batch snapshot 을 만든다. 핵심은 fraud, forecast, segment 를 같은 테이블에 억지로 끼워 넣지 않고 **task-aware metric JSON** 으로 구분하는 것이다.

| 과제 | 입력 테이블 | 핵심 지표 | drift 기준 |
|------|-------------|-----------|------------|
| fraud classifier | `prediction_result`, `risk_score` | score p50/p95, high decision rate, alert count | 학습 artifact 의 baseline score distribution 대비 PSI |
| revenue forecast | `prediction_result`, `revenue_forecast` | 예측 금액 평균, horizon 별 forecast count, PI width | 학습 artifact 의 baseline target/score distribution 대비 PSI |
| customer segment | `customer_segment`, `segment_action_recommendation` | segment 비중, action coverage, unstable segment count | 학습 artifact 의 baseline segment share 대비 PSI |

Baseline window 는 모델 등록 시점의 train split 분포를 우선 사용한다. Week 1~3 artifact 에 baseline stats 가 없으면 최근 7일 snapshot 을 temporary baseline 으로 사용하되, 리포트에 `baseline_source=rolling_7d_fallback` 으로 기록한다.

### 4-1. `schemas/model_monitoring.sql`

일별 snapshot 이므로 `UNIQUE (model_name, model_version, snapshot_date, task_type)` 를 두고 재실행은 `ON CONFLICT DO UPDATE` 로 멱등 처리한다.

```sql
-- schemas/model_monitoring.sql
CREATE TABLE IF NOT EXISTS model_monitoring_snapshot (
    snapshot_id        BIGSERIAL PRIMARY KEY,
    model_name         VARCHAR(80) NOT NULL,
    model_version      VARCHAR(64) NOT NULL,
    task_type          VARCHAR(32) NOT NULL,
    snapshot_date      DATE NOT NULL,
    prediction_count   BIGINT NOT NULL,
    metric_json        JSONB NOT NULL,
    quality_json       JSONB NOT NULL,
    drift_json         JSONB NOT NULL,
    status             VARCHAR(20) NOT NULL,
    baseline_source    VARCHAR(80) NOT NULL,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (model_name, model_version, snapshot_date, task_type)
);

CREATE INDEX IF NOT EXISTS ix_monitoring_snapshot_model_date
    ON model_monitoring_snapshot (model_name, snapshot_date);
```

### 4-2. `mlops/drift_metrics.py` — PSI 단일 소스

PSI 는 분포가 얼마나 달라졌는지 설명하기 쉬운 drift 지표다. `WARN >= 0.10`, `FAIL >= 0.25` 를 기본값으로 두고, 운영 기준이 바뀌면 이 모듈만 수정한다.

```python
# mlops/drift_metrics.py
"""Nexus Pay monitoring 에서 공통으로 쓰는 drift metric 함수."""
from __future__ import annotations

import numpy as np


def psi(expected, actual, bins: int = 10, eps: float = 1e-6) -> float:
    """expected baseline 과 actual snapshot 의 Population Stability Index 를 계산한다."""
    exp = np.asarray(expected, dtype=float)
    act = np.asarray(actual, dtype=float)
    exp = exp[np.isfinite(exp)]
    act = act[np.isfinite(act)]
    if len(exp) == 0 or len(act) == 0:
        return 0.0

    edges = np.unique(np.quantile(exp, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        edges = np.linspace(float(exp.min()), float(exp.max()) + eps, bins + 1)
    exp_hist, _ = np.histogram(exp, bins=edges)
    act_hist, _ = np.histogram(act, bins=edges)
    exp_pct = np.maximum(exp_hist / max(exp_hist.sum(), 1), eps)
    act_pct = np.maximum(act_hist / max(act_hist.sum(), 1), eps)
    return float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))


def drift_status(value: float, warn: float = 0.10, fail: float = 0.25) -> str:
    if value >= fail:
        return "FAIL"
    if value >= warn:
        return "WARN"
    return "PASS"
```

코드 해설:

* `psi()` 는 train split 또는 rolling baseline 과 현재 snapshot 의 분포 차이를 하나의 숫자로 만든다.
* quantile bin 을 사용해 이상치에 덜 민감하게 만들고, 빈 구간은 `eps` 로 보정해 0 나눗셈을 막는다.
* `drift_status()` 는 리포트와 promotion gate 가 같은 WARN/FAIL 기준을 쓰게 한다.

### 4-3. `mlops/monitor_predictions.py` — task-aware snapshot + 자동 리포트

Week 2/3 의 `--processing-date-from/to` 패턴을 유지한다. snapshot 적재 뒤에는 row 수와 status 분포를 출력해 sanity assertion 역할을 한다.

```python
# mlops/monitor_predictions.py
"""일별 prediction monitoring snapshot 을 만들고 markdown 리포트를 생성한다."""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from mlops.drift_metrics import drift_status, psi


TASKS = {
    "fraud": {
        "model_name": "nexuspay_fraud_classifier",
        "task_type": "binary_classifier",
        "sql": """
            SELECT p.model_name, p.model_version, p.score, p.prediction, p.scored_at
            FROM prediction_result p
            WHERE p.model_name IN ('nexuspay_baseline_classifier', 'nexuspay_fraud_classifier')
              AND DATE(p.scored_at) BETWEEN :date_from AND :date_to
        """,
    },
    "forecast": {
        "model_name": "nexuspay_revenue_forecast",
        "task_type": "multi_output_regression",
        "sql": """
            SELECT model_name, model_version, forecast_amount AS score, forecast_date AS scored_at
            FROM revenue_forecast
            WHERE forecast_date BETWEEN :date_from AND :date_to
        """,
    },
    "segment": {
        "model_name": "nexuspay_customer_segment",
        "task_type": "clustering",
        "sql": """
            SELECT model_name, model_version, segment_label AS score, segmented_at AS scored_at
            FROM customer_segment
            WHERE DATE(segmented_at) BETWEEN :date_from AND :date_to
        """,
    },
}


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def _numeric_scores(df: pd.DataFrame) -> np.ndarray:
    scores = pd.to_numeric(df["score"], errors="coerce").dropna()
    return scores.to_numpy(dtype=float)


def _rolling_baseline(conn, task: str, snapshot_date: date) -> tuple[list[float], str]:
    spec = TASKS[task]
    rows = conn.execute(
        text(spec["sql"]),
        {"date_from": snapshot_date - pd.Timedelta(days=7), "date_to": snapshot_date - pd.Timedelta(days=1)},
    ).mappings().all()
    base = pd.DataFrame(rows)
    if base.empty:
        return [0.0], "empty_fallback"
    return _numeric_scores(base).tolist() or [0.0], "rolling_7d_fallback"


def _metrics(task: str, df: pd.DataFrame) -> dict:
    if df.empty:
        return {"count": 0}
    if task == "segment":
        share = df["score"].astype(str).value_counts(normalize=True).round(4).to_dict()
        return {"segment_share": share, "segment_count": int(df["score"].nunique())}
    scores = _numeric_scores(df)
    return {
        "score_mean": float(np.mean(scores)) if len(scores) else None,
        "score_p50": float(np.percentile(scores, 50)) if len(scores) else None,
        "score_p95": float(np.percentile(scores, 95)) if len(scores) else None,
        "high_decision_rate": float(np.mean(scores >= 0.7)) if len(scores) else None,
    }


def _quality(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"missing_score_rate": 1.0, "row_count": 0}
    return {
        "missing_score_rate": float(df["score"].isna().mean()),
        "row_count": int(len(df)),
        "model_versions": sorted(df["model_version"].astype(str).unique().tolist()),
    }


def _upsert(conn, *, model_name: str, model_version: str, task_type: str, snapshot_date: date,
            prediction_count: int, metric_json: dict, quality_json: dict, drift_json: dict,
            status: str, baseline_source: str) -> None:
    conn.execute(
        text(
            """
            INSERT INTO model_monitoring_snapshot(
                model_name, model_version, task_type, snapshot_date, prediction_count,
                metric_json, quality_json, drift_json, status, baseline_source
            )
            VALUES (:model_name, :model_version, :task_type, :snapshot_date, :prediction_count,
                    CAST(:metric_json AS JSONB), CAST(:quality_json AS JSONB),
                    CAST(:drift_json AS JSONB), :status, :baseline_source)
            ON CONFLICT (model_name, model_version, snapshot_date, task_type)
            DO UPDATE SET
                prediction_count = EXCLUDED.prediction_count,
                metric_json = EXCLUDED.metric_json,
                quality_json = EXCLUDED.quality_json,
                drift_json = EXCLUDED.drift_json,
                status = EXCLUDED.status,
                baseline_source = EXCLUDED.baseline_source,
                updated_at = NOW()
            """
        ),
        {
            "model_name": model_name,
            "model_version": model_version,
            "task_type": task_type,
            "snapshot_date": snapshot_date,
            "prediction_count": prediction_count,
            "metric_json": json.dumps(metric_json),
            "quality_json": json.dumps(quality_json),
            "drift_json": json.dumps(drift_json),
            "status": status,
            "baseline_source": baseline_source,
        },
    )


def _render_report(rows: list[dict], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"model_monitoring_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md"
    lines = ["# Nexus Pay Model Monitoring Report", "", "| Task | Model | Version | Date | Count | Status | PSI |", "|------|-------|---------|------|-------|--------|-----|"]
    for r in rows:
        lines.append(
            f"| {r['task']} | {r['model_name']} | {r['model_version']} | {r['snapshot_date']} | "
            f"{r['prediction_count']} | {r['status']} | {r['psi']:.4f} |"
        )
    lines.extend(["", "## Stakeholder Interpretation", "", "- `WARN` 은 다음 재학습 window 에서 원인 분석 대상이다.", "- `FAIL` 이 2회 연속이면 retraining DAG 를 manual trigger 한다."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processing-date-from", required=True)
    parser.add_argument("--processing-date-to", required=True)
    parser.add_argument("--task", choices=["all", *TASKS], default="all")
    parser.add_argument("--report-dir", default="data/reports")
    args = parser.parse_args()

    date_from = pd.to_datetime(args.processing_date_from).date()
    date_to = pd.to_datetime(args.processing_date_to).date()
    tasks = list(TASKS) if args.task == "all" else [args.task]
    engine = _engine()
    snapshots: list[dict] = []

    with engine.begin() as conn:
        for task in tasks:
            spec = TASKS[task]
            df = pd.DataFrame(conn.execute(text(spec["sql"]), {"date_from": date_from, "date_to": date_to}).mappings().all())
            if df.empty:
                df = pd.DataFrame(columns=["model_name", "model_version", "score", "scored_at"])
            model_version = str(df["model_version"].dropna().iloc[-1]) if not df["model_version"].dropna().empty else "unknown"
            actual = _numeric_scores(df)
            baseline, baseline_source = _rolling_baseline(conn, task, date_to)
            psi_value = psi(baseline, actual if len(actual) else [0.0])
            status = drift_status(psi_value)
            metric_json = _metrics(task, df)
            quality_json = _quality(df)
            drift_json = {"psi": psi_value, "warn": 0.10, "fail": 0.25}
            _upsert(
                conn,
                model_name=spec["model_name"],
                model_version=model_version,
                task_type=spec["task_type"],
                snapshot_date=date_to,
                prediction_count=int(len(df)),
                metric_json=metric_json,
                quality_json=quality_json,
                drift_json=drift_json,
                status=status,
                baseline_source=baseline_source,
            )
            snapshots.append({"task": task, "model_name": spec["model_name"], "model_version": model_version, "snapshot_date": date_to, "prediction_count": int(len(df)), "status": status, "psi": psi_value})

    report = _render_report(snapshots, Path(args.report_dir))
    status_counts = pd.Series([s["status"] for s in snapshots]).value_counts().to_dict()
    print(f"monitoring snapshots written: {len(snapshots)}")
    print(f"status distribution: {status_counts}")
    print(f"report: {report}")
    if not snapshots:
        raise SystemExit("no monitoring snapshot generated")


if __name__ == "__main__":
    main()
```

코드 해설:

* `TASKS` 는 모델별 입력 테이블, task type, 조회 SQL 을 한 곳에서 관리한다.
* `_upsert()` 는 `ON CONFLICT` 를 사용해 같은 날짜 snapshot 재실행이 실패하지 않게 한다.
* `--processing-date-from/to` 를 사용해 Week 2/3 과 같은 일자 처리 패턴을 유지한다.
* 실행 후 snapshot 수, status 분포, 리포트 경로를 출력해 sanity assertion 을 남긴다.

### 4-4. Makefile 타겟 추가

```makefile
.PHONY: monitoring-schema monitoring-snapshot

monitoring-schema:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-mluser} -d $${POSTGRES_DB:-ml_db} < schemas/model_monitoring.sql

monitoring-snapshot:
	docker compose exec runner python -m mlops.monitor_predictions \
	  --processing-date-from $${DATE_FROM:-2026-06-01} \
	  --processing-date-to $${DATE_TO:-2026-06-07} \
	  --task $${TASK:-all}
```

### Day 4 완료 기준

* `make monitoring-schema` 가 `model_monitoring_snapshot` 을 생성한다.
* `make monitoring-snapshot DATE_FROM=... DATE_TO=...` 실행 후 fraud/forecast/segment snapshot 이 task-aware JSON 컬럼으로 적재된다.
* 같은 날짜로 재실행해도 `ON CONFLICT DO UPDATE` 로 성공한다.
* `data/reports/model_monitoring_report_*.md` 가 생성되고 PSI, status, stakeholder interpretation 을 포함한다.
* CFO/COO 관점에서 성능 저하가 운영 비용과 retraining trigger 로 어떻게 이어지는지 설명할 수 있다.

---

## Day 5: RAG/LLM Feedback Loop + Handoff

### 5-0. RAG + feedback loop 도입 범위

Week 4 에서 RAG 는 모델 예측을 대신하지 않는다. 운영 문서, runbook, 평가 리포트, schema 설명을 검색해 운영자의 질문에 답하고, 그 답변의 좋고 나쁨을 feedback table 과 preference pair 로 남기는 prototype 이다.

| 구성 | Week 4 결정 |
|------|-------------|
| Embedding model | `sentence-transformers/all-MiniLM-L6-v2` |
| Vector dimension | 384 |
| Vector DB | Qdrant collection `nexuspay_mlops_docs`, cosine distance |
| LLM | 기본은 Ollama `llama3`; `OPENAI_API_KEY` 가 있으면 OpenAI API 로 교체 가능하도록 adapter 분리 |
| Reindex 정책 | `--rebuild` 는 collection recreate, 기본 실행은 chunk hash 기반 upsert |
| Sensitive filter | ingestion 직전 `rag/pii_filter.py` 의 email/phone/card/secret 패턴을 통과해야 함 |
| Feedback store | `schemas/llm_feedback.sql` 의 answer evaluation + preference pair 테이블 |

질문 예시:

* "fraud 모델을 staging 으로 올리려면 어떤 gate 를 확인해야 하나?"
* "risk_score 와 prediction_result 의 차이는 무엇인가?"
* "forecast 모델의 MAPE 가 나빠지면 어떤 절차로 재학습하나?"
* "API readiness 가 실패하면 어느 runbook 을 봐야 하나?"

### 5-1. `schemas/llm_feedback.sql`

plan 의 Day 5 핵심은 답변 생성이 아니라 **답변 품질을 운영 개선 루프로 남기는 것**이다. 따라서 질문, 답변, 평가 점수, preference pair, reviewer, rationale 을 별도 테이블로 둔다.

```sql
-- schemas/llm_feedback.sql
CREATE TABLE IF NOT EXISTS llm_answer_feedback (
    feedback_id     BIGSERIAL PRIMARY KEY,
    question        TEXT NOT NULL,
    answer          TEXT NOT NULL,
    source_paths    JSONB NOT NULL,
    rating          INTEGER CHECK (rating BETWEEN 1 AND 5),
    feedback_label  VARCHAR(30) CHECK (feedback_label IN ('good', 'bad', 'needs_review')),
    reviewer        VARCHAR(80),
    rationale       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS llm_preference_pair (
    pair_id         BIGSERIAL PRIMARY KEY,
    question        TEXT NOT NULL,
    answer_a        TEXT NOT NULL,
    answer_b        TEXT NOT NULL,
    preferred       CHAR(1) NOT NULL CHECK (preferred IN ('A', 'B')),
    reviewer        VARCHAR(80) NOT NULL,
    rationale       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
```

### 5-2. `rag/pii_filter.py` — ingestion 전 민감정보 차단

```python
# rag/pii_filter.py
"""RAG ingestion 전에 문서에 민감정보 패턴이 있는지 검사한다."""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PiiFinding:
    kind: str
    sample: str


PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"\b(?:010|011|016|017|018|019)-?\d{3,4}-?\d{4}\b"),
    "card": re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
    "secret": re.compile(r"(?i)\b(api[_-]?key|secret|password|token)\s*[:=]\s*['\"]?[^'\"\s]+"),
}


def find_pii(text: str) -> list[PiiFinding]:
    findings: list[PiiFinding] = []
    for kind, pattern in PATTERNS.items():
        for match in pattern.finditer(text):
            findings.append(PiiFinding(kind=kind, sample=match.group(0)[:12] + "..."))
    return findings


def assert_no_pii(text: str, source_path: str) -> None:
    findings = find_pii(text)
    if findings:
        detail = ", ".join(f"{f.kind}:{f.sample}" for f in findings[:5])
        raise ValueError(f"PII/secret pattern blocked in {source_path}: {detail}")
```

코드 해설:

* `find_pii()` 는 문서 전체를 Qdrant 로 보내기 전에 email/phone/card/secret 패턴을 찾는다.
* `assert_no_pii()` 는 ingestion 을 중단해 CISO 요구사항인 “raw PII ingestion 금지”를 코드로 보장한다.

### 5-3. `rag/ingest_documents.py` — chunking, embedding, Qdrant upsert

```python
# rag/ingest_documents.py
"""운영 문서를 chunk 로 나누고 embedding 한 뒤 Qdrant collection 에 적재한다."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from rag.pii_filter import assert_no_pii


COLLECTION = "nexuspay_mlops_docs"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
VECTOR_SIZE = 384
SOURCE_GLOBS = ["docs/**/*.md", "schemas/**/*.sql", "reports/**/*.md"]


def chunk_text(text: str, size: int = 900, overlap: int = 120) -> list[str]:
    chunks: list[str] = []
    start = 0
    while start < len(text):
        chunk = text[start:start + size].strip()
        if chunk:
            chunks.append(chunk)
        start += size - overlap
    return chunks


def iter_documents(root: Path):
    for pattern in SOURCE_GLOBS:
        for path in root.glob(pattern):
            if path.is_file():
                yield path


def point_id(source: str, chunk: str) -> int:
    digest = hashlib.sha1(f"{source}:{chunk}".encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--qdrant-url", default="http://qdrant:6333")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    model = SentenceTransformer(EMBEDDING_MODEL)
    client = QdrantClient(url=args.qdrant_url)

    if args.rebuild or not client.collection_exists(COLLECTION):
        client.recreate_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )

    points: list[PointStruct] = []
    for path in iter_documents(root):
        text = path.read_text(encoding="utf-8")
        assert_no_pii(text, str(path))
        for idx, chunk in enumerate(chunk_text(text)):
            vector = model.encode(chunk).tolist()
            payload = {
                "source_path": str(path).replace("\\", "/"),
                "chunk_index": idx,
                "chunk_hash": hashlib.sha1(chunk.encode("utf-8")).hexdigest(),
                "text": chunk,
            }
            points.append(PointStruct(id=point_id(payload["source_path"], payload["chunk_hash"]), vector=vector, payload=payload))

    if points:
        client.upsert(collection_name=COLLECTION, points=points)
    print(f"ingested chunks: {len(points)} into {COLLECTION}")
    if not points:
        raise SystemExit("no document chunks ingested")


if __name__ == "__main__":
    main()
```

### 5-4. `rag/retrieve_context.py` 와 `rag/query_assistant.py`

`query_assistant.py` 는 답변에 source path 를 반드시 포함하도록 Pydantic 응답 스키마를 사용한다. LLM 을 연결하지 못하는 로컬 환경에서도 검색 chunk 기반의 extractive answer 로 동작한다.

```python
# rag/retrieve_context.py
"""Qdrant 에서 운영 문서 context 를 검색한다."""
from __future__ import annotations

import argparse

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

COLLECTION = "nexuspay_mlops_docs"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def retrieve(question: str, top_k: int = 5, qdrant_url: str = "http://qdrant:6333") -> list[dict]:
    model = SentenceTransformer(EMBEDDING_MODEL)
    client = QdrantClient(url=qdrant_url)
    vector = model.encode(question).tolist()
    hits = client.search(collection_name=COLLECTION, query_vector=vector, limit=top_k)
    return [
        {
            "score": float(hit.score),
            "source_path": hit.payload["source_path"],
            "text": hit.payload["text"],
        }
        for hit in hits
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()
    for item in retrieve(args.question, args.top_k):
        print(f"[{item['score']:.4f}] {item['source_path']}\n{item['text'][:500]}\n")


if __name__ == "__main__":
    main()
```

```python
# rag/query_assistant.py
"""운영 질문에 source path 를 포함한 답변을 생성한다."""
from __future__ import annotations

import argparse
import json

from pydantic import BaseModel, Field

from rag.retrieve_context import retrieve


class AssistantAnswer(BaseModel):
    question: str
    answer: str
    source_paths: list[str] = Field(min_length=1)
    confidence: str


def answer_question(question: str, top_k: int = 5) -> AssistantAnswer:
    contexts = retrieve(question, top_k=top_k)
    if not contexts:
        return AssistantAnswer(
            question=question,
            answer="관련 운영 문서를 찾지 못했다. 먼저 registry, API, retraining runbook 을 확인해야 한다.",
            source_paths=["docs/mlops/model-registry-runbook.md"],
            confidence="low",
        )
    source_paths = sorted({c["source_path"] for c in contexts})
    evidence = "\n".join(f"- {c['text'][:350]}" for c in contexts[:3])
    return AssistantAnswer(
        question=question,
        answer=f"검색된 운영 문서 기준으로 답한다.\n{evidence}",
        source_paths=source_paths,
        confidence="medium",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()
    print(json.dumps(answer_question(args.question, args.top_k).model_dump(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
```

### 5-5. `reports/mlops_operational_readiness_report.py`

```python
# reports/mlops_operational_readiness_report.py
"""Week 4 운영 준비도 리포트를 한 페이지 markdown 으로 생성한다."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path


CHECKS = [
    ("Registry", "docs/mlops/model-registry-runbook.md"),
    ("Candidate evaluation", "docs/mlops/model-candidate-evaluation.md"),
    ("API hardening", "docs/mlops/api-hardening-runbook.md"),
    ("Retraining/Monitoring", "docs/mlops/retraining-and-monitoring-runbook.md"),
    ("RAG design", "docs/rag/ml-ops-assistant-design.md"),
    ("LLM feedback loop", "docs/llm-feedback-loop-design.md"),
    ("Monitoring report", "data/reports"),
]


def main() -> None:
    rows = []
    for name, path in CHECKS:
        exists = Path(path).exists()
        rows.append((name, path, "READY" if exists else "MISSING"))
    out = Path("data/reports") / f"mlops_operational_readiness_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Nexus Pay MLOps Operational Readiness Report", "", "| Component | Evidence | Status |", "|-----------|----------|--------|"]
    lines += [f"| {name} | `{path}` | {status} |" for name, path, status in rows]
    lines.extend(["", "## Executive Handoff", "", "- CTO: registry, promotion, rollback evidence 확인", "- CISO: API hardening 과 RAG PII filter 확인", "- CEO/CMO: feedback loop 와 preference data 로 생성형 AI 품질 개선 가능성 확인"])
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"readiness report written: {out}")
    if any(status == "MISSING" for _, _, status in rows):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
```

### 5-6. RAG 설계 문서

`docs/rag/ml-ops-assistant-design.md` 는 운영 assistant 의 기술 결정을 고정한다.

```bash
mkdir -p docs/rag
cat > docs/rag/ml-ops-assistant-design.md << 'EOF'
# Nexus Pay ML Ops Assistant Design

## 1. Source Scope

`docs/`, `schemas/`, `reports/` 의 markdown/sql 파일만 ingestion 한다. `data/` 원천 파일, `.env`, raw PII, secrets 는 ingestion 하지 않는다.

## 2. Chunking

900자 chunk, 120자 overlap 을 기본값으로 사용한다. 각 chunk 는 `source_path`, `chunk_index`, `chunk_hash` metadata 를 가진다.

## 3. Embedding and Vector DB

- embedding model: `sentence-transformers/all-MiniLM-L6-v2`
- vector size: 384
- Qdrant collection: `nexuspay_mlops_docs`
- distance: cosine
- rebuild: `make rag-ingest` 의 `--rebuild` 옵션으로 collection recreate

## 4. Answer Policy

모든 답변은 최소 1개 이상의 `source_paths` 를 포함해야 한다. 근거 문서를 찾지 못하면 추측하지 않고 확인할 runbook 후보를 반환한다.

## 5. CISO Control

`rag/pii_filter.py` 가 email, phone, card, secret pattern 을 탐지하면 ingestion 을 중단한다.
EOF
```

### 5-7. Feedback loop 설계 문서

`docs/llm-feedback-loop-design.md` 를 생성한다. plan 의 파일명을 따른다.

```bash
cat > docs/llm-feedback-loop-design.md << 'EOF'
# Nexus Pay LLM Feedback Loop Design

## 1. 목적

운영 RAG assistant 의 답변을 그대로 신뢰하지 않고, 좋은 답변/나쁜 답변 평가 데이터셋과 preference pair 를 쌓아 향후 고객 상담·마케팅 자동화 품질 개선의 출발점으로 삼는다.

## 2. Answer Evaluation Dataset

`llm_answer_feedback` 은 question, answer, source_paths, rating, feedback_label, reviewer, rationale 을 저장한다. rating 1~2 는 bad, 3 은 needs_review, 4~5 는 good 으로 본다.

## 3. Preference Pair

`llm_preference_pair` 는 같은 질문에 대한 answer_a / answer_b 와 preferred 값을 저장한다. 이 데이터는 reward model 또는 RLAIF 평가셋으로 확장할 수 있다.

## 4. RLHF/RLAIF 매핑

| 개념 | Week 4 구현 |
|------|-------------|
| Human feedback | reviewer, rating, rationale |
| Preference data | answer_a, answer_b, preferred |
| Reward modeling | 직접 학습하지 않고 preference pair schema 와 평가 지표만 설계 |
| PPO/RL training | 이번 범위 제외 |
| 운영 개선 루프 | 낮은 rating 질문을 runbook 보강 또는 retrieval tuning backlog 로 전환 |

## 5. 품질 지표

- source path 포함률
- rating 평균
- bad answer 비율
- needs_review backlog count
- preference agreement rate
EOF
```

### 5-8. Makefile 타겟 추가

```makefile
.PHONY: rag-deps feedback-schema rag-ingest rag-query feedback-test readiness-report mlops-test

rag-deps:
	docker compose exec runner pip install qdrant-client==1.11.1 sentence-transformers==3.0.1

feedback-schema:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-mluser} -d $${POSTGRES_DB:-ml_db} < schemas/llm_feedback.sql

rag-ingest:
	docker compose exec runner python -m rag.ingest_documents --rebuild

rag-query:
	docker compose exec runner python -m rag.query_assistant \
	  --question "$${QUESTION:-fraud 모델을 staging 으로 올리려면 어떤 gate 를 확인해야 하나?}"

feedback-test:
	docker compose exec runner pytest -q tests/test_mlops_end_to_end.py::test_feedback_tables_accept_answer_and_preference

readiness-report:
	docker compose exec runner python -m reports.mlops_operational_readiness_report

mlops-test:
	docker compose exec runner pytest -q tests/test_mlops_end_to_end.py
```

### 5-9. `tests/test_mlops_end_to_end.py`

```python
# tests/test_mlops_end_to_end.py
"""Week 4 운영화 산출물의 핵심 계약을 검증한다."""
from __future__ import annotations

import json
import os

import pytest
from sqlalchemy import create_engine, text

from mlops.drift_metrics import drift_status, psi
from rag.pii_filter import find_pii
from rag.query_assistant import AssistantAnswer


@pytest.fixture()
def db_engine():
    url = (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url, future=True)


def test_drift_status_thresholds():
    assert drift_status(0.02) == "PASS"
    assert drift_status(0.12) == "WARN"
    assert drift_status(0.30) == "FAIL"
    assert psi([0.1, 0.2, 0.3], [0.1, 0.2, 0.9]) >= 0


def test_pii_filter_blocks_sensitive_patterns():
    findings = find_pii("api_key=abc123 and user@example.com")
    assert {f.kind for f in findings} >= {"secret", "email"}


def test_assistant_answer_requires_source_path():
    answer = AssistantAnswer(
        question="gate?",
        answer="promotion_gates.py 를 확인한다.",
        source_paths=["docs/mlops/model-registry-runbook.md"],
        confidence="medium",
    )
    assert answer.source_paths


def test_retraining_dag_order():
    from dags.ml_retraining_dag import PIPELINE_BY_MODEL

    for steps in PIPELINE_BY_MODEL.values():
        task_ids = [s[0] for s in steps]
        assert task_ids[-1] == "register_candidate"
        assert "validate_metric_gate" in task_ids


def test_feedback_tables_accept_answer_and_preference(db_engine):
    with db_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO llm_answer_feedback(question, answer, source_paths, rating, feedback_label, reviewer) "
                "VALUES (:q, :a, CAST(:sp AS JSONB), 5, 'good', 'ml-lead')"
            ),
            {"q": "gate?", "a": "promotion_gates.py", "sp": json.dumps(["docs/mlops/model-registry-runbook.md"])},
        )
        conn.execute(
            text(
                "INSERT INTO llm_preference_pair(question, answer_a, answer_b, preferred, reviewer, rationale) "
                "VALUES ('gate?', 'A', 'B', 'A', 'ml-lead', 'source path included')"
            )
        )
        count = conn.execute(text("SELECT COUNT(*) FROM llm_answer_feedback")).scalar()
    assert int(count or 0) >= 1
```

### End-to-end 검증 시나리오

```bash
make up
make healthcheck
make registry-schema
make registry-check MODEL=nexuspay_fraud_classifier VERSION=<version> EXTRA_ARGS="--run-id <run-id>"
make candidate-evaluation
make api-hardening-test
make retraining-dag-validate
make monitoring-schema
make monitoring-snapshot DATE_FROM=2026-06-01 DATE_TO=2026-06-07
make rag-deps
make feedback-schema
make rag-ingest
make rag-query
make feedback-test
make readiness-report
make mlops-test
```

기대 결과:

* registry metadata 에 `None / Staging / Production / Archived` stage 와 approval log 가 존재한다.
* candidate evaluation report 가 baseline 대비 후속 후보의 metric, 운영 비용, promotion gate 를 정리한다.
* production API readiness 가 모델 artifact 와 registry 상태를 검증한다.
* retraining DAG validation 이 성공한다.
* monitoring snapshot 과 markdown report 가 생성된다.
* RAG assistant 가 운영 질문에 source path 와 함께 답한다.
* `llm_answer_feedback`, `llm_preference_pair` 에 answer evaluation 과 preference pair sample 이 적재된다.
* operational readiness report 가 registry/API/DAG/monitoring/RAG/feedback loop 준비 상태를 한 페이지로 정리한다.

### Day 5 완료 기준

* RAG collection 에 Week 1~4 운영 문서가 PII 필터를 통과해 ingestion 된다.
* 운영 질문 5개에 대해 source 기반 답변이 생성되고, 모든 답변이 `source_paths` 를 포함한다.
* `schemas/llm_feedback.sql` 로 answer feedback 과 preference pair 테이블이 생성된다.
* `make feedback-test` 가 feedback table 적재를 검증한다.
* `docs/llm-feedback-loop-design.md` 가 answer evaluation dataset, preference pair, human feedback loop, RLHF/RLAIF 매핑을 포함한다.
* `make readiness-report` 가 CTO/CISO/CIO/CEO/CMO 관점 준비 상태를 markdown 으로 생성한다.

---

## Week 4 산출물 체크리스트

완료 검증일: `YYYY-MM-DD` - 작성 전

### 코드 산출물

- [ ] `study-ml-pipeline/mlops/promotion_gates.py`
- [ ] `study-ml-pipeline/mlops/register_model.py`
- [ ] `study-ml-pipeline/mlops/promote_model.py`
- [ ] `study-ml-pipeline/mlops/evaluate_model_candidates.py`
- [ ] `study-ml-pipeline/mlops/drift_metrics.py`
- [ ] `study-ml-pipeline/mlops/monitor_predictions.py`
- [ ] `study-ml-pipeline/schemas/model_monitoring.sql`
- [ ] `study-ml-pipeline/schemas/llm_feedback.sql`
- [ ] `study-ml-pipeline/serving/app.py`
- [ ] `study-ml-pipeline/dags/ml_retraining_dag.py`
- [ ] `study-ml-pipeline/rag/pii_filter.py`
- [ ] `study-ml-pipeline/rag/ingest_documents.py`
- [ ] `study-ml-pipeline/rag/retrieve_context.py`
- [ ] `study-ml-pipeline/rag/query_assistant.py`
- [ ] `study-ml-pipeline/reports/mlops_operational_readiness_report.py`
- [ ] `study-ml-pipeline/tests/test_mlops_end_to_end.py`
- [ ] `study-ml-pipeline/Makefile` mlops/rag 타겟 추가

### 문서 산출물

- [ ] `study-ml-pipeline/docs/mlops/model-registry-runbook.md`
- [ ] `study-ml-pipeline/docs/mlops/model-candidate-evaluation.md`
- [ ] `study-ml-pipeline/docs/mlops/api-hardening-runbook.md`
- [ ] `study-ml-pipeline/docs/mlops/retraining-and-monitoring-runbook.md`
- [ ] `study-ml-pipeline/docs/rag/ml-ops-assistant-design.md`
- [ ] `study-ml-pipeline/docs/llm-feedback-loop-design.md`
- [ ] `study-ml-pipeline/data/reports/model_monitoring_report_*.md`
- [ ] `study-ml-pipeline/data/reports/mlops_operational_readiness_report_*.md`

### 운영 산출물

- [ ] `make registry-check` 성공
- [ ] `make candidate-evaluation` 성공
- [ ] `make api-hardening-test` 성공
- [ ] `make retraining-dag-validate` 성공
- [ ] `make monitoring-schema` 성공
- [ ] `make monitoring-snapshot` 성공
- [ ] `make rag-deps` 성공
- [ ] `make feedback-schema` 성공
- [ ] `make rag-ingest` 성공
- [ ] `make rag-query` 성공
- [ ] `make feedback-test` 성공
- [ ] `make readiness-report` 성공
- [ ] `make mlops-test` 성공
- [ ] model registry metadata에 `None / Staging / Production / Archived` 상태 기록
- [ ] monitoring snapshot에 prediction distribution 기록
- [ ] `llm_answer_feedback`, `llm_preference_pair` 에 feedback sample 기록

### CTO/CIO/CISO/COO/CFO/CEO/CMO 요구사항 정합 점검

| 요구자 | 요구사항 | 충족 증빙 |
|--------|----------|-----------|
| CTO | 운영 승격과 rollback 가능한 모델 관리 | registry workflow, promotion runbook |
| CIO | retraining, lineage, monitoring 절차 | retraining/monitoring runbook |
| CISO | API hardening, 민감정보 없는 로그, RAG ingestion 제한 | API runbook, RAG design |
| COO | 장애 시 fallback과 운영 handoff | production API, rollback 절차 |
| CFO | 성능 저하와 비용 영향 판단 | monitoring report, evaluation gate |
| CEO/CMO | 생성형 AI 답변 품질 개선 루프와 마케팅/상담 자동화 확장성 | `llm_feedback` schema, preference pair, feedback loop design |

---

## 4주 Capability Build 완료 기준

다음 조건을 만족하면 `study-ml-pipeline`은 Nexus Pay ML Pipeline Lab의 4주 capability build를 완료한 것으로 본다.

* Week 1 baseline 모델이 feature/train/evaluate/predict/API/DAG 흐름으로 동작한다.
* Week 2 fraud classifier/anomaly detector가 risk score와 threshold policy를 생성한다.
* Week 3 forecast/segment 모델이 business insight 운영 테이블과 stakeholder report를 생성한다.
* Week 4 registry/API/retraining/monitoring/RAG/feedback loop runbook이 준비된다.
* 모든 주요 모델은 feature set, model version, prediction result, evaluation, lineage를 연결해 설명할 수 있다.
* CTO-level portfolio review에서 "모델 하나를 만든 프로젝트"가 아니라 "반복 가능한 ML 운영 체계"로 설명 가능하다.
