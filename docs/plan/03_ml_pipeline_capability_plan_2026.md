# 2026년 ML Pipeline 역량 강화 실행계획

이 문서는 3개월 역량 강화 계획 중 **ml-pipeline** 영역의 상세 실행계획이다. 목표는 `study-ml-pipeline`을 컨테이너로 재현 가능한 ML 운영 파이프라인으로 구축하는 것이다. 총괄 일정은 `01_capability_building_plan_2026.md`를 참조한다.

## 영역 B: ML 모델 구축 실습 (4주 · 2026-05-25~2026-06-21)

영역 B의 목적은 알고리즘을 많이 나열하는 것이 아니라, 데이터파이프라인 위에서 ML 모델이 운영되는 전체 흐름을 구축할 수 있게 만드는 것이다. 따라서 학습 단위는 `데이터셋 로딩 → 피처 생성 → 모델 학습 → 평가 → 모델 저장 → 배치 추론 → 예측 결과 저장 → API 또는 리포트 서빙`으로 통일한다.

| 기간 | 주제 | 데이터/예제 | 데이터파이프라인 구축 수준 학습 내용 | 산출물 |
|------|------|------------|------------------------------------|--------|
| 2026-05-25~2026-05-31 | ML 기본기와 모델링 파이프라인 | tabular 샘플 데이터, Nexus Pay 거래 샘플 | train/test split, time-based split, data leakage 방지, 결측·이상치 처리, feature engineering, sklearn Pipeline, 모델 입력/출력 스키마 정의, 평가 지표 템플릿 작성 | 공통 ML 프로젝트 템플릿, feature schema, metric report template |
| 2026-06-01~2026-06-07 | 분류·이상탐지 | fraud/churn/anomaly 예제 | 클래스 불균형 처리, precision/recall/F1/PR-AUC, 임계값 튜닝, 고위험 거래 scoring table 설계, 일배치 예측 결과를 PostgreSQL에 저장하는 구조 설계 | 분류 모델 비교 리포트, risk_score 테이블, batch scoring script |
| 2026-06-08~2026-06-14 | 회귀·시계열·군집화 | 매출·수요·고객 행동 예제 | 날짜/지역/고객 단위 집계 피처 생성, time-series validation, MAE/RMSE/MAPE 평가, 고객 RFM 군집화, 예측 결과와 세그먼트 결과를 Gold 테이블로 적재 | demand forecast notebook, customer segment table, Gold mart 설계 |
| 2026-06-15~2026-06-21 | 모델 운영 + RAG + 생성형 AI 피드백 루프 | MLflow, FastAPI, vector DB, 내부 문서 예제, 답변 평가 데이터셋 | MLflow model registry, 모델 버전 관리, FastAPI 추론 엔드포인트, Airflow 재학습 DAG 설계, embedding 생성 파이프라인, vector DB 적재, RAG 검색 품질 점검, 답변 평가 데이터셋 구성, 사용자 피드백 수집, preference pair 설계, RLHF/RLAIF 개념 이해 | model serving API, retraining DAG 초안, RAG ingestion pipeline, LLM feedback loop 설계 문서 |

#### 1주차: ML 기본기와 모델링 파이프라인

> 수행 시나리오: Nexus Pay 데이터팀이 이상거래 탐지와 고객 행동 예측을 시작하려 하지만, 분석용 CSV와 운영 데이터가 분리되어 있고 모델 입력 스키마도 정리되어 있지 않다. CTO는 "모델을 하나 만드는 것"보다 앞으로 여러 모델에 반복 적용할 수 있는 학습·평가·예측 파이프라인 표준을 먼저 만들 것을 요구한다.

C레벨 요구사항:

* CTO: 개발자 개인 노트북이 아니라 Docker Compose로 재현 가능한 ML 운영 실습 환경을 구성할 것
* CTO: 피처 생성, 모델 학습, 평가, 배치 추론, API 서빙이 각각 독립 실행 가능하면서도 하나의 흐름으로 연결될 것
* CIO: 모델 입력·출력 스키마와 데이터 계보를 문서화해 향후 감사와 운영 인수인계가 가능할 것
* COO: 예측 결과가 파일에 머물지 않고 운영팀이 조회 가능한 PostgreSQL 테이블로 저장될 것
* CFO: 모델 평가 리포트에는 정확도뿐 아니라 오탐·미탐이 운영 비용에 미치는 해석이 포함될 것

5일 일정 구성:

* Day 1: ML 프로젝트 구조 설계 — `data/`, `features/`, `models/`, `reports/`, `serving/` 구조 정의, 학습 입력과 예측 출력 스키마 초안 작성
* Day 2: 데이터 분할과 누수 방지 — train/test split, time-based split, 데이터 누수 사례 정리, 거래 데이터 기준 학습 시점과 예측 시점 분리
* Day 3: 피처 엔지니어링 기본 — 결측·이상치 처리, 범주형 인코딩, 금액·빈도·최근성 피처 생성, sklearn Pipeline 구성
* Day 4: 모델 학습과 평가 템플릿 — baseline 모델 학습, cross validation, 분류·회귀 공통 평가 리포트 템플릿 작성
* Day 5: 배치 예측 구조 정리 — 학습된 모델로 일배치 예측을 수행하고 예측 결과를 파일 또는 PostgreSQL 테이블에 저장하는 기본 흐름 구현

실습 예제 구성:

* `study-ml-pipeline/docker-compose.yml`: PostgreSQL, MLflow, FastAPI, Qdrant, Python runner를 포함한 로컬 ML 운영 실습 환경
* `study-ml-pipeline/.env.example`: DB 접속 정보, MLflow tracking URI, API 포트, 벡터 DB 주소 등 환경 변수 예시
* `study-ml-pipeline/README.md`: 컨테이너 기동, 피처 생성, 모델 학습, 평가, 배치 추론, API 호출 절차
* `study-ml-pipeline/Makefile`: `make up`, `make train`, `make predict`, `make api-test`, `make down` 등 반복 실행 명령
* `study-ml-pipeline/config/model_config.yaml`: 데이터 경로, 피처 목록, 모델 파라미터, 평가 지표, 저장 위치 설정
* `study-ml-pipeline/schemas/init.sql`: 피처 테이블, 예측 결과 테이블, 모델 평가 결과 테이블 스키마
* `study-ml-pipeline/features/build_features.py`: 원천 데이터에서 모델 입력 피처를 생성하고 PostgreSQL 또는 파일에 저장
* `study-ml-pipeline/models/train_baseline.py`: baseline 모델 학습, 평가 리포트 생성, MLflow 실험 기록
* `study-ml-pipeline/models/batch_predict.py`: 학습된 모델로 배치 추론을 수행하고 예측 결과를 PostgreSQL에 저장
* `study-ml-pipeline/serving/app.py`: FastAPI 기반 모델 추론 API 초안
* `study-ml-pipeline/dags/ml_training_pipeline_dag.py`: 피처 생성→학습→평가→배치 추론 흐름을 Airflow로 옮기기 위한 DAG 초안
* `study-ml-pipeline/docs/ml-feature-schema.md`: 모델 입력·출력 스키마와 데이터 누수 방지 기준 문서
* `study-ml-pipeline/docs/ml-pipeline-architecture.md`: 컨테이너 기반 ML 운영 파이프라인 아키텍처와 데이터 흐름 설명

#### 2주차: 분류·이상탐지

> 수행 시나리오: Nexus Pay COO가 고위험 거래를 사람이 전수 검토하기 어렵다고 보고한다. 결제 승인 이후 일정 시간 안에 고위험 거래를 선별하고, 운영팀이 확인할 수 있는 risk score 테이블과 일일 탐지 리포트가 필요하다. 목표는 모델 정확도를 높이는 것뿐 아니라, 예측 결과가 운영 테이블로 흘러가는 구조를 만드는 것이다.

C레벨 요구사항:

* COO: 운영팀이 매일 확인할 수 있는 high/medium/low risk 등급과 고위험 거래 Top N 리포트를 제공할 것
* CTO: 모델 버전, scoring 시각, 입력 피처 버전을 함께 남겨 예측 결과를 추적 가능하게 만들 것
* CIO: 이상거래 탐지 결과 테이블은 원천 거래 ID와 연결되어 감사 추적이 가능해야 할 것
* CFO: 임계값 조정 시 오탐으로 인한 검토 비용과 미탐으로 인한 손실 위험을 비교할 수 있을 것
* CISO: 샘플 데이터라도 카드번호·주민번호 등 민감정보가 포함되지 않도록 비식별 거래 데이터를 사용할 것

5일 일정 구성:

* Day 1: 이상거래 문제 정의 — fraud/churn/anomaly 차이 정리, 라벨 정의, 클래스 불균형 확인, 업무상 오탐·미탐 비용 정의
* Day 2: 분류 모델 학습 — Logistic Regression, RandomForest, XGBoost 또는 LightGBM 비교, precision/recall/F1/PR-AUC 평가
* Day 3: 임계값 튜닝 — 운영팀 처리 가능 건수 기준으로 threshold 조정, high/medium/low risk 등급 설계
* Day 4: 예측 결과 저장 — `risk_score` 테이블 설계, 거래 ID·모델 버전·점수·위험 등급·생성 시각 저장
* Day 5: 일일 탐지 리포트 — 고위험 거래 Top N, 탐지 건수, 모델 성능, 데이터 품질 이슈를 운영 리포트로 정리

실습 예제 구성:

* `study-ml-pipeline/config/fraud_model_config.yaml`: 라벨 컬럼, 피처 목록, 모델 후보, 임계값 후보, risk grade 기준 설정
* `study-ml-pipeline/schemas/risk_score.sql`: `transaction_id`, `model_name`, `model_version`, `risk_score`, `risk_grade`, `scored_at`을 포함한 예측 결과 테이블
* `study-ml-pipeline/data/sample/fraud_transactions.csv`: 분류·이상탐지 실습용 거래 샘플 데이터
* `study-ml-pipeline/features/build_fraud_features.py`: 거래 이력에서 최근 거래 횟수, 평균 금액, 지역 변경 여부, 가맹점 위험도 등 fraud 피처 생성
* `study-ml-pipeline/models/train_fraud_classifier.py`: Logistic Regression, RandomForest, XGBoost/LightGBM 후보 모델 학습과 MLflow 실험 기록
* `study-ml-pipeline/models/evaluate_fraud_model.py`: precision/recall/F1/PR-AUC, confusion matrix, feature importance 리포트 생성
* `study-ml-pipeline/models/tune_threshold.py`: 운영팀 처리 가능 건수 기준 임계값과 high/medium/low risk grade 산정
* `study-ml-pipeline/models/publish_risk_scores.py`: 일배치 scoring 결과를 PostgreSQL `risk_score` 테이블에 적재
* `study-ml-pipeline/serving/fraud_api.py`: 특정 거래 또는 고객의 risk score를 조회하는 FastAPI 엔드포인트 초안
* `study-ml-pipeline/reports/fraud_detection_daily_report.py`: PostgreSQL에서 고위험 거래 Top N과 모델 성능 지표를 읽어 Markdown 리포트 생성
* `study-ml-pipeline/tests/test_fraud_pipeline.py`: 피처 생성, 모델 예측, risk score 적재가 정상 동작하는지 검증하는 테스트
* `study-ml-pipeline/Makefile`: `make fraud-train`, `make fraud-score`, `make fraud-report`, `make fraud-test` 실행 명령 추가
* `study-ml-pipeline/docs/fraud-detection-runbook.md`: 데이터 준비, 모델 학습, 배치 scoring, API 조회, 장애 시 재처리 절차

#### 3주차: 회귀·시계열·군집화

> 수행 시나리오: 1~2주차에 `study-ml-pipeline`의 컨테이너 실행 환경과 이상거래 risk score 흐름이 만들어졌다. 이제 Nexus Pay CFO와 CMO는 같은 ML 운영 환경을 활용해 다음 달 거래량과 수수료 매출을 예측하고, 고객군별 활동 패턴을 보고 싶어 한다. 이번 주 목표는 2주차의 `risk_score` 결과와 거래·고객 피처를 함께 활용해 예측·세분화 결과를 Gold mart로 적재하고, 경영 리포트와 마케팅 실행에 연결하는 것이다.

C레벨 요구사항:

* CFO: 일별·주별 거래량과 수수료 매출 예측치를 제공하고, 예측 오차를 MAE/RMSE/MAPE로 설명할 것
* CMO: 고객 세그먼트를 단순 군집 번호가 아니라 재구매 유도, 휴면 방지, VIP 관리 등 실행 가능한 그룹으로 해석할 것
* COO: 수요 예측 결과가 운영 인력과 CS 대응량 계획에 활용될 수 있도록 날짜·시간대·위험 등급 단위로 집계될 것
* CTO: 1~2주차에서 만든 `study-ml-pipeline`의 PostgreSQL, MLflow, Makefile, 테스트 구조를 재사용할 것
* CIO: 예측·세그먼트 Gold mart가 원천 거래, risk score, 모델 버전과 연결되어 추적 가능해야 할 것

5일 일정 구성:

* Day 1: 수요·매출 예측용 Gold mart 설계 — 날짜·가맹점·고객군 단위 집계, lag/rolling average/holiday 피처 생성
* Day 2: 회귀·시계열 모델 학습 — baseline 회귀, tree 기반 모델, 간단한 시계열 검증, MAE/RMSE/MAPE 평가
* Day 3: 고객 군집화 — RFM, 거래 빈도, 평균 결제 금액, 최근 활동일 기반 고객 세그먼트 생성
* Day 4: 결과 테이블 적재 — `demand_forecast`, `customer_segment` Gold 테이블 설계와 적재
* Day 5: 경영·마케팅 리포트 작성 — 예측 오차, 매출 전망, 고객군별 액션 아이템을 CFO/CMO 관점으로 정리

실습 예제 구성:

* `study-ml-pipeline/config/demand_model_config.yaml`: 예측 기간, 집계 단위, lag/rolling 피처, 평가 지표 설정
* `study-ml-pipeline/schemas/ml_gold_marts.sql`: `demand_forecast`, `customer_segment`, `segment_action_recommendation` Gold 테이블 정의
* `study-ml-pipeline/features/build_demand_features.py`: 거래·고객·risk score 데이터를 조인해 수요·매출 예측 피처 생성
* `study-ml-pipeline/features/build_customer_segments.py`: RFM, 최근 활동성, 평균 결제 금액, 위험 등급 비중 기반 세그먼트 피처 생성
* `study-ml-pipeline/models/train_demand_forecast.py`: 회귀·시계열 예측 모델 학습과 MLflow 실험 기록
* `study-ml-pipeline/models/segment_customers.py`: 고객 군집화와 세그먼트 라벨링
* `study-ml-pipeline/models/publish_ml_gold_marts.py`: 예측·세그먼트 결과를 PostgreSQL Gold mart에 적재
* `study-ml-pipeline/reports/revenue_forecast_and_segmentation.py`: CFO/CMO 대상 매출 예측·고객 세분화 Markdown 리포트 생성
* `study-ml-pipeline/tests/test_forecast_segment_pipeline.py`: demand forecast와 customer segment 적재 검증
* `study-ml-pipeline/Makefile`: `make demand-train`, `make segment`, `make ml-gold`, `make business-report` 실행 명령 추가
* `study-ml-pipeline/docs/forecast-segmentation-runbook.md`: 예측 모델 재학습, 세그먼트 갱신, Gold mart 검증 절차

#### 4주차: 모델 운영 + RAG + 생성형 AI 피드백 루프

> 수행 시나리오: 1~3주차를 통해 `study-ml-pipeline`에는 피처 생성, fraud scoring, 수요 예측, 고객 세그먼트 Gold mart가 쌓였다. Nexus Pay CTO는 이 흐름을 수동 스크립트 실행에 머물게 하지 말고 모델 등록, API 서빙, 재학습 DAG로 운영화하라고 요구한다. 동시에 COO와 CS 리더는 fraud runbook, 예측 리포트, 장애 대응 문서를 검색형 Q&A로 활용하고, 답변 품질을 지속 개선할 수 있는 피드백 루프를 원한다.

C레벨 요구사항:

* CTO: 1~3주차에서 만든 모델과 mart 생성 스크립트를 Airflow 재학습 DAG와 MLflow model registry 흐름으로 연결할 것
* COO: 운영팀이 API 또는 리포트로 risk score, 수요 예측, 고객 세그먼트를 조회할 수 있을 것
* CIO: 모델 버전, 데이터 기간, 실행 DAG run ID, 산출 테이블을 연결해 운영 추적성을 확보할 것
* CISO: RAG에 적재되는 문서에는 민감정보가 포함되지 않도록 문서 정제와 접근 범위를 명시할 것
* CEO/CMO: 생성형 AI 답변의 좋고 나쁨을 평가할 수 있는 feedback loop를 설계해 향후 고객 상담·마케팅 자동화로 확장 가능성을 보여줄 것

5일 일정 구성:

* Day 1: 모델 버전 관리 — MLflow 개념 학습, 모델 artifact 저장, 모델 버전·파라미터·평가 지표 기록
* Day 2: 모델 서빙 API — FastAPI 기반 추론 엔드포인트 설계, 입력 검증, 예측 응답 스키마 정의
* Day 3: 재학습 DAG 설계 — Airflow로 feature build, train, evaluate, register, batch scoring 단계를 연결하는 DAG 초안 작성
* Day 4: RAG ingestion — 내부 문서 chunking, embedding 생성, vector DB 적재, 검색 품질 평가 기준 정리
* Day 5: 생성형 AI 피드백 루프 — 좋은 답변/나쁜 답변 평가 데이터셋, preference pair, 사용자 피드백 수집 필드, RLHF/RLAIF 개념을 운영 개선 루프로 정리

실습 예제 구성:

* `study-ml-pipeline/mlops/register_model.py`: MLflow 기반 모델 등록 스크립트
* `study-ml-pipeline/serving/app.py`: fraud score, demand forecast, customer segment 조회를 포함한 FastAPI 추론·조회 API
* `study-ml-pipeline/dags/ml_retraining_dag.py`: feature build, fraud train, demand train, segment, evaluate, register, batch scoring, Gold mart publish 단계를 연결하는 재학습 DAG 설계
* `study-ml-pipeline/rag/ingest_documents.py`: 문서 chunking, embedding, vector DB 적재 파이프라인
* `study-ml-pipeline/rag/query_assistant.py`: runbook과 리포트 문서를 검색해 답변하는 RAG 질의 스크립트
* `study-ml-pipeline/schemas/llm_feedback.sql`: 질문, 답변, 평가 점수, preference pair, reviewer, created_at을 저장하는 피드백 테이블
* `study-ml-pipeline/reports/mlops_operational_readiness_report.py`: 모델 등록, API, DAG, RAG, 피드백 루프 준비 상태를 점검하는 운영 준비도 리포트
* `study-ml-pipeline/tests/test_mlops_end_to_end.py`: API 응답, DAG 태스크 의존성, RAG ingestion, feedback table 적재 검증
* `study-ml-pipeline/Makefile`: `make register-model`, `make serve`, `make rag-ingest`, `make rag-query`, `make feedback-test` 실행 명령 추가
* `study-ml-pipeline/docs/llm-feedback-loop-design.md`: 답변 평가 데이터셋, preference pair, human feedback loop 설계 문서
* `study-ml-pipeline/docs/mlops-runbook.md`: 모델 등록, API 배포, 재학습, RAG 재색인, 장애 대응 절차

영역 B 완료 기준은 모델 정확도 하나로 판단하지 않는다. 학습이 끝났을 때 모델 입력 데이터가 어디서 생성되는지, 예측 결과가 어디에 저장되는지, 실패 시 어떤 DAG를 재실행해야 하는지, 다음 영역 C의 실제 데이터셋에 같은 구조를 어떻게 적용할지 설명 가능해야 한다.

> **강화학습 참고**: 생성형 AI 시대에는 RLHF, RLAIF, reward modeling, preference data, human feedback loop가 LLM 품질 개선의 핵심 개념으로 부상하고 있다. 다만 2026-05~07 3개월 집중 학습 기간에는 PPO 기반 강화학습 모델을 직접 학습하는 수준까지 확장하지 않고, 영역 B의 모델 운영·RAG 학습 안에서 답변 평가 데이터셋, 사용자 피드백 수집, preference pair 구성, 평가 지표 설계, 개선 루프 구축을 우선 학습한다. 전통적 강화학습 실습은 실제 수요가 발생하면 별도 심화 학습으로 분리한다.
