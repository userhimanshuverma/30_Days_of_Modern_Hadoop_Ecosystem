#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM openjdk:8-jdk-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    python2 \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set Environment variables
ENV RANGER_HOME=/opt/ranger-admin
ENV RANGER_VERSION=2.4.0

# Copy built tarball from compilation target
# (Assumes Ranger was compiled via the 'Build From Source' guide)
COPY ./ranger-${RANGER_VERSION}-admin.tar.gz /opt/

WORKDIR /opt
RUN tar -xvf ranger-${RANGER_VERSION}-admin.tar.gz && \
    mv ranger-${RANGER_VERSION}-admin ${RANGER_HOME} && \
    rm ranger-${RANGER_VERSION}-admin.tar.gz

WORKDIR ${RANGER_HOME}

# Copy setup configuration properties template
COPY ./install.properties ${RANGER_HOME}/install.properties

# Expose Ranger Admin standard port
EXPOSE 6080

# Run Ranger Admin setup and start script
CMD ["/bin/bash", "-c", "python setup.py && ./ranger-admin-services.sh start && tail -f /var/log/ranger/admin/xa_portal.log"]
