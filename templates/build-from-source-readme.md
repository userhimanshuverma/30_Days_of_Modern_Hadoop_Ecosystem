# Building [Component Name] From Source

## 🔗 GitHub Source & Version Target
* **Repository:** [Link to Apache GitHub Mirror]
* **Target Git Tag/Branch:** `rel/release-x.y.z`

## 📋 Prerequisites
Specify required software versions.
* **Operating System:** Ubuntu 22.04 LTS / Rocky Linux 9
* **JDK:** OpenJDK 8 / 11 / 17
* **Build System:** Maven x.y.z / Gradle a.b.c
* **System Utilities:** `gcc`, `g++`, `make`, `protobuf-compiler`

## 🔨 Compilation Instructions
```bash
# Clone the repository
git clone https://github.com/apache/[component].git
cd [component]
git checkout tags/rel/release-x.y.z -b build-x.y.z

# Run build command with flags
mvn clean package -Pdist,native -DskipTests -Dtar
```

## 📦 Packaging and Distribution
* Location of the compiled tarball: `[path-to-dist-target]`
* Extracting and setting up environment variables (`HADOOP_HOME`, `SPARK_HOME`).

## ⚙️ Baseline Production Configuration
Example of minimal required configuration files.

## 🚀 Startup and Verification
Commands to spin up the master/worker processes and check execution.

## 🩹 Troubleshooting
* **Issue:** Proto compiler mismatch.
  * **Solution:** Install native protobuf matching version `x.y.z`.
* **Issue:** Heap space out of memory during compilation.
  * **Solution:** `export MAVEN_OPTS="-Xmx2048m -XX:MaxMetaspaceSize=512m"`
