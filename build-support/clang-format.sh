#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script run the clang-format to check and fix
# cplusplus source files.
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DORIS_HOME=$(
    cd "${ROOT}/.."
    pwd
)
export DORIS_HOME

# 专门使用 clang-format-16
if [[ -z $(command -v clang-format-16) ]]; then
    echo "clang-format-16 not found, please install clang-format-16"
    exit 1
fi

# 验证 clang-format-16 版本
CLANG_FORMAT_VERSION=$(clang-format-16 --version | perl -nle 'print $& if m{version \K[0-9]+}')
if [[ ${CLANG_FORMAT_VERSION} -ne 16 ]]; then
    echo "clang-format-16 version is not 16, please install clang-format-16 version 16 or upgrade your clang-format-16 version to 16"
    exit 1
fi

# 直接使用 clang-format-16
CLANG_FORMAT="clang-format-16"

python "${DORIS_HOME}/build-support/run_clang_format.py" "--clang-format-executable" "${CLANG_FORMAT}" "-r" "--style" "file" "--inplace" "true" "--extensions" "c,h,C,H,cpp,hpp,cc,hh,c++,h++,cxx,hxx" "--exclude" "none" "be/src be/test cloud/src cloud/test"
