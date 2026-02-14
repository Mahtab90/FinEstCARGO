# CARGO  
## Compliant Analytics for Resilient Governance and Orchestration of Maritime Emissions

CARGO is a scalable, explainable, and governance-aware serverless framework for maritime emissions analytics. It integrates EU MRV CO₂ reports, ERA5 meteorological data, and AIS voyage trajectories into an adaptive Directed Acyclic Graph (DAG) orchestration model designed for regulatory compliance, cyber-trust, and Digital Twin readiness.

This repository provides the full replication package accompanying the IEEE Access publication:

> Shahin, M., Malhi, A., Palu, R., Kim, A., Bauk, S., & Soe, R.-M. (2026).  
> **CARGO: Compliant Analytics for Resilient Governance and Orchestration of Maritime Emissions in an Explainable Serverless Framework.**  
> IEEE Access.  
> DOI: 10.5281/zenodo.XXXXXXX (to be updated upon archival)

---

# Overview

CARGO unifies three core dimensions of compliance-driven maritime analytics:

### 1. Adaptive Orchestration
- Elastic serverless DAG scheduling
- Cost–latency optimization
- Workload-aware scaling using EWMA estimators
- Concurrency cap (Kmax) control

### 2. Explainable Analytics
- Association Rule Mining (ARM) via PySpark FP-Growth
- Entropy-based discretization (MDLP)
- Vessel archetype clustering (K-means)
- Weather–emission interaction analysis

### 3. Governance by Design
- AES-256 encryption
- TLS 1.3 secure transmission
- Role-Based Access Control (RBAC)
- Immutable audit logging

---

# Repository Structure

FinEstCARGO/
├── configs/
│ ├── spark_config_prod.yml
│ ├── spark_submit.sh
│ ├── arm_config.yaml
│ └── autoscaler.yaml
├── orchestration/
│ ├── dag_workflow.yaml
│ └── dag_manifest.json
├── scripts/
│ ├── preprocess.py
│ ├── spatio_temporal_join.py
│ ├── discretize_entropy.py
│ ├── arm_pyspark.py
│ ├── measure_coldstart.py
│ └── dt_calibrate.py
├── serverless/
│ ├── lambda_handler.py
│ └── serverless.yml
├── docker/
│ └── Dockerfile
├── docs/
│ └── reproduction_checklist.md
└── README.md


---

# Data Requirements

Due to regulatory and commercial licensing constraints, raw vessel-level MRV and AIS data are not redistributed.

To reproduce experiments, obtain:

- **EMSA THETIS-MRV**  
  https://mrv.emsa.europa.eu  

- **ERA5 Meteorology (Copernicus CDS)**  
  https://cds.climate.copernicus.eu  

- **AIS Trajectories**  
  Commercial provider (e.g., MarineTraffic) or open mirror:  
  https://www.aishub.net  

---

# Quick Start

### 1. Clone repository

```bash
git clone https://github.com/Mahtab90/FinEstCARGO
cd FinEstCARGO


2. Install dependencies (Python 3.10)
pip install -r requirements.txt

3. Run preprocessing
python scripts/preprocess.py \
  --mrv data/MRV/*.csv \
  --era5 data/ERA5/*.nc \
  --ais data/AIS/*.csv \
  --output outputs/features.parquet

4. Run ARM pipeline (PySpark)
spark-submit configs/spark_submit.sh

Key Experimental Configuration
ARM Parameters
Parameter	Value
minSupport	0.05
minConfidence	0.60
minLift	1.2
maxPatternLength	5
numPartitions	2000

Transaction encoding format:

feature=value

Serverless Deployment (500 GB Experiment)

Total distinct function types: ~32

Peak reserved concurrency: Kmax = 200

ARM parallel partitions: 200

Average cold-start latency: 28 ms

Throughput: ~1,540 MB/s

Orchestration latency: ~115 ms

Mathematical Model

CARGO minimizes:

min_{k_t} λ L(k_t) + (1−λ) C(k_t)
subject to governance_check(v) = true


Where:

L(k_t) = latency

C(k_t) = cloud cost

k_t = number of concurrent serverless functions

Cold-start latency:

L_cold = L_first − L_warm

Digital Twin Integration

CARGO integrates with maritime Digital Twin (DT) systems by feeding:

ΔFuel estimates

ΔSpeed under weather scenarios

ARM-derived compliance rules

Vessel efficiency archetypes

Physics-informed propulsion model:

P ∝ v³
P_eff = P (1 + α_w W + α_h H)
Fuel = P_eff / (η · LHV)

Performance Results (500 GB Benchmark)
Platform	Throughput (MB/s)	Latency (ms)
Apache Spark	1,100	185
AWS Lambda	950	210
Google Cloud Functions	1,020	195
CARGO	1,540	115
Reproducibility

All configuration files, orchestration manifests, and measurement scripts are included.

To reproduce:

Obtain datasets (see Data Requirements)

Follow docs/reproduction_checklist.md

Use provided Spark and serverless configs

A Zenodo archival snapshot will be linked here upon final publication.

Citation

If you use this repository, please cite:

@article{shahin2026cargo,
  title={CARGO: Compliant Analytics for Resilient Governance and Orchestration of Maritime Emissions in an Explainable Serverless Framework},
  author={Shahin, Mahtab and Malhi, Avleen and Palu, Riina and Kim, Austin and Bauk, Sanja and Soe, Ralf-Martin},
  journal={IEEE Access},
  year={2026},
}

License

MIT License

Contact

Mahtab Shahin
Tallinn University of Technology
mahtab.shahin@taltech.ee

Avleen Malhi
Aalto University
avleen.malhi@aalto.fi




