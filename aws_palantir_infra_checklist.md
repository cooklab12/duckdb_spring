# ✅ Infra Requests Checklist – AWS & Palantir

This checklist maps **capabilities → infra needs** in a tick-box format for easy tracking by the infra team.

---

## 1. AWS – Option 1 (UI On-Prem, AWS for AI + Execution)

- [ ] **Text-to-SQL (AI)**
  - [ ] Enable Amazon Bedrock (Claude, Llama3, Titan) OR SageMaker for LLM hosting
  - [ ] Configure IAM roles/policies for Bedrock API access

- [ ] **Execution on Hive (On-Prem)**
  - [ ] Expose HiveServer2/JDBC endpoint securely
  - [ ] Set up VPN/Direct Connect between on-prem and Hive cluster

- [ ] **Execution on Snowflake (Cloud)**
  - [ ] Ensure active Snowflake account in AWS region
  - [ ] Configure AWS Private Link or VPC peering for Snowflake connectivity

- [ ] **Data Quality Engine**
  - [ ] On Hive → Enable Spark/Hive scripts or Great Expectations
  - [ ] On Snowflake → Enable AWS Glue Data Quality jobs

- [ ] **Results Storage**
  - [ ] On-Prem → Local DB (Postgres/MySQL/Hive table)
  - [ ] Cloud → S3 bucket + Athena OR Snowflake schema

- [ ] **Visualization**
  - [ ] On-Prem → Grafana/Apache Superset/custom UI
  - [ ] Enable JDBC/ODBC access to Hive/Snowflake

- [ ] **Networking**
  - [ ] VPN/Direct Connect for secure metadata + execution API calls
  - [ ] IAM integration for Bedrock/Glue services

---

## 2. AWS – Option 2 (Full Cloud including UI)

- [ ] **Text-to-SQL (AI)**
  - [ ] Deploy Amazon Bedrock or SageMaker in same VPC as Glue/Snowflake
  - [ ] IAM roles for UI integration

- [ ] **Execution on Snowflake**
  - [ ] Enable Snowflake compute in AWS region

- [ ] **Execution on Hive (Hybrid)**
  - [ ] Option 1: Deploy AWS EMR cluster with Hive/Presto
  - [ ] Option 2: Lift & shift Hive data to S3, query via Athena

- [ ] **Data Quality Engine**
  - [ ] Enable Glue Data Quality jobs
  - [ ] Configure Glue Data Catalog for schema management

- [ ] **Results Storage**
  - [ ] Store results in Snowflake schema
  - [ ] Enable S3 data lake + Athena queries

- [ ] **Visualization**
  - [ ] Enable Amazon QuickSight dashboards
  - [ ] Configure direct BI integration with Snowflake or Athena

- [ ] **Networking & Access**
  - [ ] Configure VPC endpoints for Glue, Bedrock, S3, Snowflake
  - [ ] Set IAM policies for secure execution and storage

---

## 3. Palantir Foundry + AIP

- [ ] **Text-to-SQL (AI)**
  - [ ] Enable Palantir AIP with Ontology/Schema mapping
  - [ ] (Optional) Enable AIP API-only mode for text-to-SQL output

- [ ] **Execution**
  - [ ] Hive (On-Prem) → ❌ Not possible directly; only via external execution
  - [ ] Snowflake → Enable Snowflake external connection into Foundry
  - [ ] Mirror Snowflake tables into Foundry datasets

- [ ] **Data Quality Engine**
  - [ ] Configure Foundry transforms + validation logic
  - [ ] Enable ontology-aware DQ workflows

- [ ] **Results Storage**
  - [ ] Cloud → Store results in Foundry datasets
  - [ ] On-Prem → ❌ Not possible (export required)

- [ ] **Visualization**
  - [ ] Foundry Workshops/Dashboards/Reports enabled
  - [ ] On-Prem visualization requires export to 3rd party BI tool

- [ ] **Networking & Access**
  - [ ] Enable secure ingestion pipelines from Snowflake → Foundry
  - [ ] Configure governance via Foundry access controls & audit logs
  - [ ] Securely expose Palantir AIP API if used in API-only setup

---

### 🔑 Key Notes
- **AWS Option 1 (Hybrid)** → Focus on connectivity (Hive + Snowflake), Bedrock API, Glue DQ, secure result storage.  
- **AWS Option 2 (Full Cloud)** → Focus on Glue, Bedrock/SageMaker, QuickSight, Snowflake compute.  
- **Palantir** → Requires AIP + Snowflake integration; on-prem execution/storage not supported except via export.  
