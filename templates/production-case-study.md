# Production Case Study: [Company] [Scenario Name]

## 🚨 Incident Summary
* **Date:** YYYY-MM-DD
* **Severity:** P0/Critical
* **Duration:** X hours
* **Impact:** Loss of downstream analytics, query latency degradation.

## 🔍 Architectural Context
How the architecture was structured before the incident occurred.

## 🪵 Chronology of the Failure
A step-by-step timeline of events, system alerts, and actions taken by the site reliability engineering (SRE) team.

## 🧠 Root Cause Analysis (RCA)
Deep technical breakdown of why the system failed (e.g., NameNode RPC queue saturation due to small files creation run amok, ZooKeeper session timeout due to JVM GC pause).

## 🩹 Mitigation and Short-Term Fixes
What was done immediately to bring the platform back to a healthy state.

## 📐 Long-Term Architectural Remediations
Changes made to the system architecture, monitoring alerts, or configuration guidelines to ensure the incident never repeats.
