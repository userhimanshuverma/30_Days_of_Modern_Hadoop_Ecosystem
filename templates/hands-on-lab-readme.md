# Hands-On Lab: [Lab Name]

## 🎯 Lab Objectives
* Clear, measurable technical milestones for the lab.

## ⚙️ Environment Provisioning
Instructions to launch the infrastructure.

```bash
# Navigate to the lab docker directory
cd hands-on-labs/docker
# Launch the specific environment profiles
docker-compose --profile [service-name] up -d
```

## 📈 Step-by-Step Lab Tasks
1. **Task 1: Bootstrap and Config Injection** - Injecting custom properties.
2. **Task 2: Ingesting Data** - Writing to storage.
3. **Task 3: Executing Job** - Processing step.
4. **Task 4: High Availability Verification** - Forcing a worker crash and verifying failover.

## 🔍 Validation Script
Provide the concrete commands to run to prove the cluster is operating correctly.
```bash
./hands-on-labs/validation/verify-[lab-name].sh
```

## 🛑 Clean Up
```bash
docker-compose --profile [service-name] down -v
```
