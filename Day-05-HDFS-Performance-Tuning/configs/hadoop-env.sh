# Export Java configurations
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk}

# NameNode JVM Memory Sizing and GC Tuning (Optimized for Production G1GC)
export HADOOP_NAMENODE_OPTS="-Xms2g -Xmx2g \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=200 \
-XX:InitiatingHeapOccupancyPercent=45 \
-XX:G1ReservePercent=15 \
-XX:ParallelGCThreads=4 \
-XX:ConcGCThreads=2 \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9904 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
$HADOOP_NAMENODE_OPTS"

# DataNode JVM Memory Sizing and GC Tuning
export HADOOP_DATANODE_OPTS="-Xms1g -Xmx1g \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=200 \
-XX:ParallelGCThreads=2 \
-XX:ConcGCThreads=1 \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9905 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
$HADOOP_DATANODE_OPTS"

# Client JVM Memory Sizing
export HADOOP_CLIENT_OPTS="-Xms512m -Xmx1g $HADOOP_CLIENT_OPTS"

# Secure short-circuit reads require unix socket location permissions
export HADOOP_SECURE_DN_USER=root
export HADOOP_PID_DIR=/var/run/hadoop
export HADOOP_LOG_DIR=/var/log/hadoop
