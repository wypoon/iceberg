#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Prepare the Jenkins build/execution environment, install build tools
# This script requires sudo privileges

set -euo pipefail
set -x

# Detect OS platform
REDHAT=
UBUNTU=

if [[ -f /etc/redhat-release ]]; then
  REDHAT=true
  echo "Identified redhat system."
else
  source /etc/lsb-release
  if [[ $DISTRIB_ID = Ubuntu ]]
  then
    UBUNTU=true
    echo "Identified Ubuntu system."
    export DEBIAN_FRONTEND=noninteractive
  else
    echo "This script only supports Ubuntu or RedHat" >&2
    exit 1
  fi
fi

# Helper function to execute following command only on Ubuntu
function ubuntu {
  if [[ "$UBUNTU" == true ]]; then
    "$@"
  fi
}

# Helper function to execute following command only on RedHat
function redhat {
  if [[ "$REDHAT" == true ]]; then
    "$@"
  fi
}

# Install prerequisites
redhat sudo yum install -y curl git-core \
        wget java-1.8.0-openjdk-devel

ubuntu sudo apt-get update
ubuntu sudo DEBIAN_FRONTEND=noninteractive apt-get --yes install curl wget git-core \
        openjdk-8-jdk
