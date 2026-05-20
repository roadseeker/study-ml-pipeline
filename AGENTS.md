# AGENTS.md — ML Pipeline Lab

This file provides repository guidance for Codex-style coding agents working in `C:\study\study-ml-pipeline`.

## Project Purpose

This repository is a portfolio-grade **ML pipeline and AI/ML consulting project** built to support a solo consulting business launch.

The project continues the Nexus Pay consulting scenario from the upstream `study-data-pipeline` lab, but this repository's primary focus is ML delivery:

- reusable ML project structure
- feature generation and data leakage prevention
- model training, evaluation, and reporting
- batch prediction and online inference
- MLflow-based model tracking
- Airflow-based orchestration design
- LLM/RAG adoption patterns when explicitly introduced by the current week

The practical goal is not only to make the code work, but to produce artifacts that can be shown to CTO-level stakeholders as proposal-quality consulting deliverables.

## Current Working Context

- Current learning and build phase: **ML Pipeline Capability Build**
- Current branch convention: `week{N}-{topic}`
- Current working branch context: `week1-ml-baseline`
- Current project progression:
  - Week 1: ML project foundation, feature schema, baseline training, batch prediction, FastAPI serving, DAG draft
  - Week 2: fraud classification and anomaly detection
  - Week 3: demand/revenue forecasting and customer segmentation
  - Week 4: model operations, MLflow registry workflow, API hardening, Airflow retraining, RAG/LLM feedback loop

When making changes, preserve the weekly progression. Do not introduce later-week implementation details into earlier-week assets unless the user explicitly asks for that.

## Scenario Context

All ML pipeline work is framed around the fictional fintech company **Nexus Pay**. Nexus Pay has an upstream data platform and now needs operational ML capabilities on top of governed transaction and customer data.

In this repository, the working role is to design, implement, validate, and operate the ML pipeline that supports Nexus Pay model delivery. Week-by-week deliverables should fit that scenario and feel like realistic consulting outputs for stakeholders such as the CTO, CIO, CFO, COO, and CISO.

Use this framing consistently in:

- architecture documents
- model and feature documentation
- runbooks and operations guides
- DAGs, scripts, APIs, and examples
- naming of datasets, model artifacts, endpoints, and demo assets

## Repository Structure

- `docker-compose.yml`: ML lab services such as PostgreSQL, MLflow, FastAPI, Qdrant, and Python runner
- `config/`: model, feature, evaluation, and service configuration
- `data/`: local sample data, generated features, model artifacts, reports, and disposable runtime outputs
- `dags/`: Airflow DAG drafts and orchestration assets
- `docs/`: consulting reports, weekly lab guides, architecture notes, schemas, and runbooks
- `features/`: feature generation, split logic, and reusable sklearn preprocessing utilities
- `models/`: training, evaluation, batch prediction, publishing, and threshold tuning code
- `mlops/`: model registry, promotion, monitoring, and operational workflow assets
- `rag/`: RAG ingestion, retrieval, and assistant workflow assets introduced in later weeks
- `reports/`: human-readable report templates and stakeholder-facing deliverables
- `schemas/`: database DDL, feature schemas, prediction schemas, and feedback schemas
- `scripts/`: project-level setup, health check, data generation, and verification utilities
- `serving/`: FastAPI inference services and API contracts
- `tests/`: unit, integration, leakage, schema, and workflow tests

## Artifact Placement Rules

- Weekly lab guides belong under `docs/guides/` and keep the `01_...` to `04_...` naming pattern unless the learning plan expands.
- Project-wide planning documents may stay under `docs/` root when they describe the overall capability roadmap.
- Weekly deliverable documents should be placed by responsibility when they become standalone artifacts:
  - `docs/features/`
  - `docs/models/`
  - `docs/serving/`
  - `docs/mlops/`
  - `docs/rag/`
  - `docs/reports/`
- Keep executable code in responsibility-based source folders such as `features/`, `models/`, `serving/`, `mlops/`, and `rag/`.
- Keep only project-level helper scripts under `scripts/`, such as `healthcheck.sh`, sample data generators, loaders, and verification utilities.
- If a week guide references an artifact, update the guide path at the same time as the file move.
- Do not move or rename stakeholder-facing deliverables casually; path stability matters for portfolio review.

## Working Rules

- Keep changes aligned with the current week or the explicit user request.
- Prefer minimal, focused edits over speculative expansion.
- Treat docs as first-class deliverables, not secondary notes.
- Maintain consistency between code, verification scripts, schemas, reports, and documentation.
- Favor production-style naming and realistic business examples over toy examples.
- Preserve the Apache open-source and Python-first positioning. Do not casually replace the stack with commercial alternatives.
- Do not assume MLflow, FastAPI, Qdrant, Airflow, or RAG assets are complete unless the repository state shows they have been implemented.

## Documentation Standards

- Write documentation in a professional consulting style.
- Keep architecture and operations documents clear enough for technical stakeholders.
- When creating new docs, explain purpose, assumptions, steps, expected outputs, and validation points.
- Weekly deliverables should feel demo-ready and portfolio-ready.
- In weekly lab guides, keep `Day N 완료 기준` in the day section itself.
- Record dated completion verification notes at the start of that week's deliverables checklist section.
- Model reports should include technical metrics and stakeholder interpretation, especially operational cost, false positives, false negatives, and handoff notes.

## Implementation Standards

- Python is the primary language for scripts, features, training, inference, orchestration helpers, and RAG utilities.
- Use scikit-learn-compatible patterns for Week 1 and Week 2 baseline models unless a later week explicitly introduces additional frameworks.
- Keep custom sklearn transformers clone-compatible by following estimator conventions such as `BaseEstimator` and `TransformerMixin`.
- Use FastAPI and Pydantic for serving contracts and request/response validation.
- Use MLflow for experiment tracking and model artifact lineage when model training is in scope.
- Airflow assets should reflect orchestration, dependency management, SLA monitoring, and recovery concerns, even when the local Week 1 container does not run Airflow.
- Shell scripts should be readable, task-oriented, and verification-friendly.
- Keep verification assets discoverable with names like `verify_*.sh`, `verify_*.py`, or focused `test_*.py`.
- Prefer realistic business-oriented sample records and production-style identifiers.
- Use **Nexus Pay** for human-facing scenario names, service descriptions, and stakeholder narratives.
- Standardize machine-readable identifiers on `nexuspay` / `NexusPay` for schemas, model names, job IDs, endpoints, package names, and example assets unless compatibility with an existing artifact requires otherwise.

## Data and Environment Safety

- Do not commit secrets from `.env`.
- Treat `data/`, generated artifacts, logs, checkpoints, MLflow local outputs, and runtime files as disposable lab outputs unless the user explicitly wants them versioned.
- Avoid destructive cleanup beyond the requested scope.
- Do not include raw sensitive data, card numbers, resident registration numbers, credentials, or personal identifiers in sample datasets.
- Keep sample data fictional, non-sensitive, and suitable for stakeholder demos.

## Architecture Conventions

- ML input data should pass through documented schemas before training or inference.
- Feature generation should distinguish raw source data, feature store representation, training frames, and online inference payloads.
- Train/validation/test split logic must be time-aware when modeling transaction or customer behavior.
- Prevent leakage from future timestamps, label confirmation timing, target-derived fields, group overlap, and preprocessing fitted outside the training set.
- Model artifacts should include the fitted pipeline, ordered feature columns, threshold or decision policy, and enough metadata to connect predictions back to the training run.
- Prediction outputs should include transaction or entity ID, model name, model version, feature set, feature version, score, prediction, scored timestamp, and run lineage.
- Evaluation outputs should include metrics, confusion matrix or task-specific diagnostics, cost interpretation, and stakeholder handoff notes.
- Batch and online inference should share the same model artifact and input feature contract whenever practical.
- RAG work should distinguish source documents, chunks, embeddings, vector collections, retrieval metadata, and generated answers.
- Migration or upstream pipeline references should distinguish full load, incremental load, and CDC, but these are background dependencies rather than this repository's core build target.

## Tech Stack Notes

### ML Pipeline

| Layer | Tools |
|-------|-------|
| Data source and state | PostgreSQL, local CSV/parquet, upstream data pipeline outputs |
| Feature engineering | pandas, scikit-learn Pipeline, custom sklearn-compatible transformers |
| ML frameworks | scikit-learn, XGBoost, LightGBM when explicitly introduced |
| Experiment tracking | MLflow |
| Inference | FastAPI, uvicorn, Pydantic |
| Orchestration | Airflow DAGs and validation helpers |
| Vector DB | Qdrant |
| Embeddings | sentence-transformers |
| LLM local | Ollama |
| LLM cloud | OpenAI APIs when explicitly needed |
| Dashboard | Streamlit when explicitly introduced |

### Upstream Data Pipeline Context

| Layer | Tools |
|-------|-------|
| Ingest | Apache Kafka, Apache NiFi |
| Transform | Apache Flink, Apache Spark |
| Store | PostgreSQL, Redis 7, Delta Lake/local data workspace |
| Orchestration | Apache Airflow |
| Migration | Spark JDBC, Debezium CDC |

The upstream data pipeline is a prerequisite context, not the main implementation surface of this repository.

## Service Ports

| Service | Container | Port |
|---------|-----------|------|
| PostgreSQL | `ml-postgres` | 5432 |
| MLflow | `ml-mlflow` | 5000 |
| FastAPI inference API | `ml-api` | 8000 |
| Qdrant | `ml-qdrant` | 6333 |
| Python runner | `ml-runner` | internal only |

If a port is already occupied, use the `.env` file or Compose overrides to adjust the local mapping and update the relevant guide.

## Advisory Team Operating Model

This repository uses the company-wide Platform Advisory TF role system from `%OPENDIGM_OPERATIONS_ROOT%\agents\`.

- Address the user as `이사님`.
- The assistant's default working role is `파트너`.
- Use `%OPENDIGM_OPERATIONS_ROOT%\agents\team-structure.md` for the overall team model.
- Use the matching role file under `%OPENDIGM_OPERATIONS_ROOT%\agents\roles\` when the user asks for a role-based review.
- Do not maintain duplicate company-wide role definitions inside this repository.
- If the user asks for broad planning, review, or execution, `파트너` should decompose the work across relevant roles and synthesize the result.
- Use `윤대리` for mail, calendar, Slack, Confluence, meeting preparation, and follow-up tracking tasks.
- Use `최팀장` for schedule, risk, deliverable tracking, and cross-repository project management.
- Use ML Platform roles for `study-ml-pipeline`, model delivery, MLOps, serving, model quality, and AI adoption work.
- Use Data Platform roles when work touches upstream `study-data-pipeline`, source data contracts, CDC, Lakehouse outputs, or feature data dependencies.
- Keep role outputs practical: findings, risks, decisions needed, and next actions.
- Do not preface ordinary responses with phrases like "파트너 역할로" or "윤대리 역할로"; respond naturally in the appropriate voice.
- When role-based reasoning is useful, present it as a concise `역할별 의견` section with each role's viewpoint, judgment, and reason.
- End broad role-based reviews with `파트너 종합` when a synthesized decision or next action is needed.

## Agent Behavior

- Before making broad changes, understand which week and ML layer the task belongs to.
- If a request is ambiguous, prefer the interpretation most consistent with the current repo phase and existing docs.
- Do not rewrite unrelated parts of the repository for style alone.
- When adding new files, keep them in the directory structure implied by the weekly lab design.
- If a change affects docs and implementation together, keep them synchronized.
- Before creating or modifying documents, guides, AGENTS instructions, Codex instructions, Claude instructions, or repository-structure-related assets, first present the proposed change, the reason for it, and the target path(s), then explicitly ask whether the user wants the file created or modified.
- Apply the same confirmation-first approach to path corrections, file moves, and stakeholder-facing deliverables so the user can approve the direction before edits are made.

## Business Context

This project is the sole portfolio vehicle for a consulting business launch. Code quality, documentation quality, and the end-to-end demo narrative matter as much as technical correctness.

Each week's repository state should be demonstrable to a CTO-level audience. The first operations/maintenance or ML adoption contract is an important survival milestone, but weekly work should remain grounded in realistic implementation, verification, and consulting-grade documentation.

## Commit Message Convention

- When suggesting or writing a git commit command, use a single `-m` option with a multi-line message body.
- Format commit messages as: first line summary, blank line, then flat `- ` bullets describing the main changes.
- Keep the summary concise and scoped to the actual change set.
- Write commit messages in Korean by default unless the user explicitly requests another language.
