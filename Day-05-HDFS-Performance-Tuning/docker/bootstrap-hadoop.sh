#!/bin/bash

# Copy optimized configurations to hadoop config directory
cp /tmp/configs/core-site.xml /etc/hadoop/core-site.xml
cp /tmp/configs/hdfs-site.xml /etc/hadoop/hdfs-site.xml
cp /tmp/configs/hadoop-env.sh /etc/hadoop/hadoop-env.sh

# Create the domain socket directory for Short-Circuit Local Reads
mkdir -p /var/lib/hadoop-hdfs
chown -R hdfs:hadoop /var/lib/hadoop-hdfs
chmod 755 /var/lib/hadoop-hdfs

# Generate Rack Topology Script
cat << 'EOF' > /etc/hadoop/rack-topology.sh
#!/bin/bash
# A simple rack topology mapping script for HDFS performance benchmarking
# datanode1 and datanode2 belong to /rack1
# datanode3 belongs to /rack2

while [ $# -gt 0 ]; do
  nodeArg=$1
  exec 3>&1
  # Resolve IP address to hostname if IP is passed
  hostName=$(getent hosts "$nodeArg" | awk '{print $2}')
  if [ -z "$hostName" ]; then
    hostName="$nodeArg"
  fi
  
  case "$hostName" in
    *datanode1*|*datanode1.hadoop.local)
      echo "/rack1" ;;
    *datanode2*|*datanode2.hadoop.local)
      echo "/rack1" ;;
    *datanode3*|*datanode3.hadoop.local)
      echo "/rack2" ;;
    *namenode*|*namenode.hadoop.local)
      echo "/rack1" ;;
    *)
      echo "/rack1" ;;
  esac
  shift
done
EOF

chmod +x /etc/hadoop/rack-topology.sh

# Directory for PID files
mkdir -p /var/run/hadoop
chown -R hdfs:hadoop /var/run/hadoop

if [ "$SERVICE_TYPE" = "namenode" ]; then
  echo "Starting NameNode..."
  # Format Namenode if not formatted
  if [ ! -d "/opt/hadoop/dfs/name/current" ]; then
    echo "Formatting Namenode filesystem..."
    /opt/hadoop/bin/hdfs namenode -format -force
  fi
  /opt/hadoop/bin/hdfs namenode
  
elif [ "$SERVICE_TYPE" = "datanode" ]; then
  echo "Starting DataNode..."
  /opt/hadoop/bin/hdfs datanode
  
elif [ "$SERVICE_TYPE" = "client" ]; then
  echo "Starting Client Node..."
  # Setup short-circuit socket directory permissions in client
  mkdir -p /var/lib/hadoop-hdfs
  chmod 755 /var/lib/hadoop-hdfs
  # Keep running
  tail -f /dev/null
  
else
  echo "Unknown service type: $SERVICE_TYPE"
  exec "$@"
fi
