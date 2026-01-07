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

export DORIS_TOOLCHAIN=clang
export BUILD_TYPE=debug
export DISABLE_BUILD_UI=ON
export DISABLE_BUILD_AZURE=ON
export DORIS_CLANG_HOME=/home/yangtao555/local/ldb-toolchain-0.27
export CLANG_FORMAT_BINARY=/home/yangtao555/local/ldb-toolchain-0.18/bin/clang-format-16
