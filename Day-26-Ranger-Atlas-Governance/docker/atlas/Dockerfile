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
    && rm -rf /var/lib/apt/lists/*

# Set Environment variables
ENV ATLAS_HOME=/opt/apache-atlas
ENV ATLAS_VERSION=2.3.0

# Copy built tarball from compilation target
# (Assumes Atlas was compiled via the 'Build From Source' guide)
COPY ./apache-atlas-${ATLAS_VERSION}-bin.tar.gz /opt/

WORKDIR /opt
RUN tar -xvf apache-atlas-${ATLAS_VERSION}-bin.tar.gz && \
    mv apache-atlas-${ATLAS_VERSION} ${ATLAS_HOME} && \
    rm apache-atlas-${ATLAS_VERSION}-bin.tar.gz

WORKDIR ${ATLAS_HOME}

# Expose Atlas default admin port
EXPOSE 21000

# Start Atlas metadata engine
CMD ["/bin/bash", "-c", "./bin/atlas_start.py && tail -f logs/application.log"]
