# 2026년 3개월 역량 강화 실행계획

이 문서는 데이터파이프라인과 AI/ML 구축 역량을 확보하기 위한 3개월 집중 학습 계획의 총괄 문서다. 사업계획서 본문에는 핵심 일정만 남기고, 상세 실행계획은 data-pipeline, ml-pipeline, scenario-pipeline 문서로 분리해 관리한다.

## 현재 기준

Week 5 Spark/Delta Lake 학습을 진행 중이며, Kafka·NiFi·Flink의 기초 흐름과 Docker Compose 기반 실습 환경 구성 경험을 확보한 상태다. MLflow·FastAPI·Qdrant를 포함한 모델 운영·서빙·RAG 구성은 아직 완료 경험으로 보지 않고, 별도 학습 대상으로 다룬다.

## 세부 실행계획 문서

| 구분 | 문서 | 목적 |
|------|------|------|
| Data Pipeline | [02_data_pipeline_capability_plan_2026.md](./02_data_pipeline_capability_plan_2026.md) | Spark JDBC·CDC·Airflow·E2E 통합까지 데이터파이프라인 잔여 역량 완성 |
| ML Pipeline | [03_ml_pipeline_capability_plan_2026.md](./03_ml_pipeline_capability_plan_2026.md) | `study-ml-pipeline` 기반 ML 모델 학습·평가·서빙·RAG·피드백 루프 구축 |
| Scenario Pipeline | [04_scenario_pipeline_portfolio_plan_2026.md](./04_scenario_pipeline_portfolio_plan_2026.md) | 실제 데이터셋 6회 반복 실습으로 사업 제안형 포트폴리오 구성 |

## 3개월 요약 일정

| 기간 | 집중 영역 | 완료 기준 |
|------|----------|----------|
| 2026-05-01~2026-05-03 | Spark/Delta Lake 마무리 | Week 5 산출물, 품질 리포트, README 정리 |
| 2026-05-04~2026-05-24 | Data Pipeline | Spark JDBC·CDC·Airflow·E2E 통합 완료 |
| 2026-05-25~2026-06-21 | ML Pipeline | 모델 학습·평가·저장·서빙·RAG·피드백 루프 기초 완료 |
| 2026-06-22~2026-07-19 | Scenario Pipeline | 해외 3건 + 국내 3건 포트폴리오 실습 완료 |
| 2026-07-20~2026-07-31 | 사업화 정리 | GitHub, 블로그, 제안서, 데모 시나리오 정리 |

## 산출물 요약

| 영역 | 핵심 산출물 |
|------|------------|
| Data Pipeline | Spark JDBC/CDC 이관, Airflow DAG, E2E 리허설, 장애·백필 리포트 |
| ML Pipeline | `study-ml-pipeline`, MLflow 실험 기록, FastAPI API, RAG ingestion, feedback loop 설계 |
| Scenario Pipeline | 해외 3건·국내 3건 실제 데이터셋 포트폴리오, 공통 README, 제안서 연결 문구 |

## 운영 원칙

- 세부 학습은 각 문서에서 관리하고, 본 문서는 일정과 산출물의 정합성만 관리한다.
- 각 상세 문서는 GitHub 공개, 블로그 작성, 제안서 템플릿에 재사용 가능한 산출물을 기준으로 작성한다.
- 3개월 완료 후에는 세 문서를 근거로 포트폴리오 데모 시나리오와 초기 영업 제안서를 작성한다.