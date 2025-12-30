// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdafBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdtfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.UdfBuilder;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * New function registry for nereids.
 *
 * this class is developing for more functions.
 */
@Developing
@ThreadSafe
public class FunctionRegistry {

    // to record the global alias function and other udf.
    private static final String GLOBAL_FUNCTION = "__GLOBAL_FUNCTION__";

    private final Map<String, List<FunctionBuilder>> name2BuiltinBuilders;
    private final Map<String, Map<String, List<FunctionBuilder>>> name2UdfBuilders;

    public FunctionRegistry() {
        name2BuiltinBuilders = new ConcurrentHashMap<>();
        name2UdfBuilders = new ConcurrentHashMap<>();
        registerBuiltinFunctions(name2BuiltinBuilders);
        afterRegisterBuiltinFunctions(name2BuiltinBuilders);
    }

    public Map<String, List<FunctionBuilder>> getName2BuiltinBuilders() {
        return name2BuiltinBuilders;
    }

    public String getGlobalFunctionDbName() {
        return GLOBAL_FUNCTION;
    }

    public Map<String, Map<String, List<FunctionBuilder>>> getName2UdfBuilders() {
        return name2UdfBuilders;
    }

    // this function is used to test.
    // for example, you can create child class of FunctionRegistry and clear builtin functions or add more functions
    // in this method
    @VisibleForTesting
    protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {}

    public FunctionBuilder findFunctionBuilder(String name, List<?> arguments) {
        return findFunctionBuilder(null, name, arguments);
    }

    public FunctionBuilder findFunctionBuilder(String name, Object argument) {
        return findFunctionBuilder(null, name, ImmutableList.of(argument));
    }

    public Optional<List<FunctionBuilder>> tryGetBuiltinBuilders(String name) {
        List<FunctionBuilder> builders = name2BuiltinBuilders.get(name);
        return name2BuiltinBuilders.get(name) == null
                ? Optional.empty()
                : Optional.of(ImmutableList.copyOf(builders));
    }

    public boolean isAggregateFunction(String dbName, String name) {
        name = name.toLowerCase();
        Class<?> aggClass = org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction.class;
        if (StringUtils.isEmpty(dbName)) {
            List<FunctionBuilder> functionBuilders = name2BuiltinBuilders.get(name);
            if (functionBuilders != null) {
                for (FunctionBuilder functionBuilder : functionBuilders) {
                    if (aggClass.isAssignableFrom(functionBuilder.functionClass())) {
                        return true;
                    }
                }
            }
        }

        List<FunctionBuilder> udfBuilders = findUdfBuilder(dbName, name);
        for (FunctionBuilder udfBuilder : udfBuilders) {
            if (aggClass.isAssignableFrom(udfBuilder.functionClass())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 根据函数名、数据库名和参数列表查找匹配的函数构建器。
     *
     * 该方法实现了 Doris 中函数查找的核心逻辑，包括：
     * 1. 根据配置决定查找顺序（UDF 优先还是内置函数优先）
     * 2. 在候选函数中筛选出参数个数和类型匹配的函数
     * 3. 处理 Java UDF 的启用/禁用状态
     * 4. 处理函数歧义情况（多个候选函数时的选择逻辑）
     *
     * 查找顺序：
     * - 如果 preferUdfOverBuiltin = true：先查找 UDF，再查找内置函数
     * - 如果 preferUdfOverBuiltin = false：先查找内置函数，再查找 UDF
     * - 如果指定了 dbName（非空），则只在该数据库中查找 UDF
     *
     * 参数匹配：
     * - 通过 canApply() 方法检查函数构建器是否能够处理给定的参数
     * - 检查参数个数（arity）和参数类型是否匹配
     *
     * 歧义处理：
     * - 如果找到多个候选函数，且都是 UDF，则使用 UdfSignatureSearcher 进行精确匹配
     * - 如果无法确定唯一匹配，则抛出歧义异常
     *
     * @param dbName 数据库名，如果为空则查找内置函数或当前数据库的 UDF
     * @param name 函数名（不区分大小写）
     * @param arguments 函数参数列表
     * @return 匹配的函数构建器
     * @throws AnalysisException 如果找不到函数、参数不匹配或存在歧义
     */
    // currently we only find function by name and arity and args' types.
    public FunctionBuilder findFunctionBuilder(String dbName, String name, List<?> arguments) {
        List<FunctionBuilder> functionBuilders = null;
        // 计算参数个数（arity）
        int arity = arguments.size();
        // 构建完全限定名（用于错误提示）
        String qualifiedName = StringUtils.isEmpty(dbName) ? name : dbName + "." + name;

        // 获取会话变量，决定查找顺序：UDF 优先还是内置函数优先
        boolean preferUdfOverBuiltin = ConnectContext.get() == null ? false
                : ConnectContext.get().getSessionVariable().preferUdfOverBuiltin;

        // 步骤 1: 根据配置决定查找顺序
        if (preferUdfOverBuiltin) {
            // 策略 1: UDF 优先
            // 先查找 UDF，如果未找到且未指定数据库名，再查找内置函数
            functionBuilders = findUdfBuilder(dbName, name);
            if (CollectionUtils.isEmpty(functionBuilders) && StringUtils.isEmpty(dbName)) {
                // 如果 dbName 不为空，则不应该搜索内置函数（因为指定了数据库）
                // 如果 dbName 为空，则在 UDF 未找到时回退到内置函数
                functionBuilders = findBuiltinFunctionBuilder(name, arguments);
            }
        } else {
            // 策略 2: 内置函数优先（默认策略）
            // 先查找内置函数，如果未找到，再查找 UDF
            if (StringUtils.isEmpty(dbName)) {
                // 只有在未指定数据库名时才查找内置函数
                functionBuilders = findBuiltinFunctionBuilder(name, arguments);
            }
            if (CollectionUtils.isEmpty(functionBuilders)) {
                // 内置函数未找到，尝试查找 UDF
                functionBuilders = findUdfBuilder(dbName, name);
            }
        }

        // 步骤 2: 检查是否找到任何函数
        if (functionBuilders == null || functionBuilders.isEmpty()) {
            throw new AnalysisException("Can not found function '" + qualifiedName + "'");
        }

        // 步骤 3: 检查参数个数和类型匹配
        // 遍历所有候选函数构建器，筛选出能够处理给定参数的构建器
        List<FunctionBuilder> candidateBuilders = Lists.newArrayListWithCapacity(arguments.size());
        for (FunctionBuilder functionBuilder : functionBuilders) {
            // canApply() 方法检查函数是否能够处理给定的参数（检查参数个数和类型）
            if (functionBuilder.canApply(arguments)) {
                candidateBuilders.add(functionBuilder);
            }
        }
        // 如果没有找到匹配的候选函数，抛出异常并提示可能的候选函数
        if (candidateBuilders.isEmpty()) {
            String candidateHints = getCandidateHint(name, functionBuilders);
            throw new AnalysisException("Can not found function '" + qualifiedName
                    + "' which has " + arity + " arity. Candidate functions are: " + candidateHints);
        }

        // 步骤 4: 检查 Java UDF 是否启用
        // 如果 Java UDF 被禁用，则过滤掉所有 Java UDF 构建器
        if (!Config.enable_java_udf) {
            candidateBuilders = candidateBuilders.stream()
                    .filter(fb -> !(fb instanceof JavaUdfBuilder || fb instanceof JavaUdafBuilder
                            || fb instanceof JavaUdtfBuilder))
                    .collect(Collectors.toList());
            // 如果过滤后没有候选函数，抛出异常
            if (candidateBuilders.isEmpty()) {
                throw new AnalysisException("java_udf has been disabled.");
            }
        }

        // 步骤 5: 处理函数歧义情况
        // 如果找到多个候选函数，需要确定唯一匹配
        if (candidateBuilders.size() > 1) {
            boolean needChooseOne = true;
            List<FunctionSignature> signatures = Lists.newArrayListWithCapacity(candidateBuilders.size());
            // 收集所有候选函数的签名（仅当所有候选都是 UDF 时）
            for (FunctionBuilder functionBuilder : candidateBuilders) {
                if (functionBuilder instanceof UdfBuilder) {
                    signatures.addAll(((UdfBuilder) functionBuilder).getSignatures());
                } else {
                    // 如果候选函数中包含非 UDF（如内置函数），则不能使用签名匹配
                    needChooseOne = false;
                    break;
                }
            }
            // 检查所有参数是否都是 Expression 类型（签名匹配需要）
            for (Object argument : arguments) {
                if (!(argument instanceof Expression)) {
                    needChooseOne = false;
                    break;
                }
            }
            // 如果满足条件，使用 UdfSignatureSearcher 进行精确匹配
            if (needChooseOne) {
                FunctionSignature signature = new UdfSignatureSearcher(signatures, (List) arguments).getSignature();
                // 找到匹配的签名，返回对应的函数构建器
                for (int i = 0; i < signatures.size(); i++) {
                    if (signatures.get(i).equals(signature)) {
                        return candidateBuilders.get(i);
                    }
                }
            }
            // 无法确定唯一匹配，抛出歧义异常
            String candidateHints = getCandidateHint(name, candidateBuilders);
            throw new AnalysisException("Function '" + qualifiedName + "' is ambiguous: " + candidateHints);
        }
        // 步骤 6: 返回唯一匹配的函数构建器
        return candidateBuilders.get(0);
    }

    /**
     * 查找内置函数构建器。
     *
     * 该方法首先尝试直接查找函数名对应的内置函数构建器。如果未找到且函数名是聚合状态组合器
     *（AggState Combinator），则尝试通过组合器机制动态构建函数。
     *
     * 聚合状态组合器（AggState Combinator）：
     * - 用于处理聚合状态类型的特殊函数，包括 _state、_merge、_union、_foreach 等后缀
     * - 例如：sum_state、sum_merge、sum_union、sum_foreach
     * - 组合器函数名格式：<嵌套函数名>_<组合器后缀>
     *   - sum_state: 嵌套函数是 sum，组合器是 state
     *   - count_merge: 嵌套函数是 count，组合器是 merge
     *
     * 处理流程：
     * 1. 直接查找：在 name2BuiltinBuilders 中查找函数名（转换为小写）
     * 2. 组合器查找（如果直接查找失败）：
     *    - 检查函数名是否是聚合状态组合器（以 _state、_merge、_union、_foreach 结尾）
     *    - 提取嵌套函数名（例如从 "sum_state" 提取 "sum"）
     *    - 提取组合器后缀（例如从 "sum_state" 提取 "state"）
     *    - 查找嵌套函数名对应的函数构建器
     *    - 为每个匹配的嵌套函数构建器创建 AggCombinerFunctionBuilder
     *    - 检查组合器构建器是否能处理给定参数，筛选出匹配的候选
     *
     * @param name 函数名（不区分大小写）
     * @param arguments 函数参数列表
     * @return 匹配的函数构建器列表，如果未找到则返回 null 或空列表
     */
    public List<FunctionBuilder> findBuiltinFunctionBuilder(String name, List<?> arguments) {
        List<FunctionBuilder> functionBuilders;
        // 步骤 1: 直接查找内置函数
        // 在 name2BuiltinBuilders 映射中查找函数名（转换为小写以支持不区分大小写的查找）
        functionBuilders = name2BuiltinBuilders.get(name.toLowerCase());
        
        // 步骤 2: 如果直接查找失败，尝试通过聚合状态组合器机制查找
        // 检查函数名是否是聚合状态组合器（以 _state、_merge、_union、_foreach 结尾）
        if (CollectionUtils.isEmpty(functionBuilders) && AggCombinerFunctionBuilder.isAggStateCombinator(name)) {
            // 提取嵌套函数名（例如从 "sum_state" 提取 "sum"）
            String nestedName = AggCombinerFunctionBuilder.getNestedName(name);
            // 提取组合器后缀（例如从 "sum_state" 提取 "state"）
            String combinatorSuffix = AggCombinerFunctionBuilder.getCombinatorSuffix(name);
            
            // 查找嵌套函数名对应的函数构建器
            functionBuilders = name2BuiltinBuilders.get(nestedName.toLowerCase());
            if (functionBuilders != null) {
                // 为每个匹配的嵌套函数构建器创建组合器构建器
                List<FunctionBuilder> candidateBuilders = Lists.newArrayListWithCapacity(functionBuilders.size());
                for (FunctionBuilder functionBuilder : functionBuilders) {
                    // 创建聚合状态组合器构建器，将嵌套函数构建器包装为组合器
                    AggCombinerFunctionBuilder combinerBuilder
                            = new AggCombinerFunctionBuilder(combinatorSuffix, functionBuilder);
                    // 检查组合器构建器是否能处理给定的参数
                    if (combinerBuilder.canApply(arguments)) {
                        candidateBuilders.add(combinerBuilder);
                    }
                }
                // 使用筛选后的组合器构建器列表替换原始列表
                functionBuilders = candidateBuilders;
            }
        }
        return functionBuilders;
    }

    /**
     * public for test.
     */
    public List<FunctionBuilder> findUdfBuilder(String dbName, String name) {
        List<String> scopes = ImmutableList.of(GLOBAL_FUNCTION);
        if (ConnectContext.get() != null) {
            dbName = dbName == null ? ConnectContext.get().getDatabase() : dbName;
            if (dbName == null || !Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                            PrivPredicate.SELECT)) {
                scopes = ImmutableList.of(GLOBAL_FUNCTION);
            } else {
                scopes = ImmutableList.of(dbName, GLOBAL_FUNCTION);
            }
        }

        synchronized (name2UdfBuilders) {
            for (String scope : scopes) {
                List<FunctionBuilder> candidate = name2UdfBuilders.getOrDefault(scope, ImmutableMap.of())
                        .get(name.toLowerCase());
                if (candidate != null && !candidate.isEmpty()) {
                    return candidate;
                }
            }
        }
        return ImmutableList.of();
    }

    private void registerBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {
        FunctionHelper.addFunctions(name2Builders, BuiltinScalarFunctions.INSTANCE.scalarFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinAggregateFunctions.INSTANCE.aggregateFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinTableValuedFunctions.INSTANCE.tableValuedFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinTableGeneratingFunctions.INSTANCE.tableGeneratingFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinWindowFunctions.INSTANCE.windowFunctions);
    }

    public String getCandidateHint(String name, List<FunctionBuilder> candidateBuilders) {
        return candidateBuilders.stream()
                .filter(builder -> {
                    if (builder instanceof BuiltinFunctionBuilder) {
                        Constructor<BoundFunction> builderMethod
                                = ((BuiltinFunctionBuilder) builder).getBuilderMethod();
                        if (Modifier.isAbstract(builderMethod.getModifiers())
                                || !Modifier.isPublic(builderMethod.getModifiers())) {
                            return false;
                        }
                        for (Class<?> parameterType : builderMethod.getParameterTypes()) {
                            if (!Expression.class.isAssignableFrom(parameterType)
                                    && !(parameterType.isArray()
                                        && Expression.class.isAssignableFrom(parameterType.getComponentType()))) {
                                return false;
                            }
                        }
                    }
                    return true;
                })
                .map(builder -> name + builder.parameterDisplayString())
                .collect(Collectors.joining(", ", "[", "]"));
    }

    public void addUdf(String dbName, String name, UdfBuilder builder) {
        if (dbName == null) {
            dbName = GLOBAL_FUNCTION;
        }
        synchronized (name2UdfBuilders) {
            Map<String, List<FunctionBuilder>> builders = name2UdfBuilders
                    .computeIfAbsent(dbName, k -> Maps.newHashMap());
            builders.computeIfAbsent(name, k -> Lists.newArrayList()).add(builder);
        }
    }

    public void dropUdf(String dbName, String name, List<DataType> argTypes) {
        if (dbName == null) {
            dbName = GLOBAL_FUNCTION;
        }
        synchronized (name2UdfBuilders) {
            Map<String, List<FunctionBuilder>> builders = name2UdfBuilders.getOrDefault(dbName, ImmutableMap.of());
            builders.getOrDefault(name, Lists.newArrayList())
                    .removeIf(builder -> ((UdfBuilder) builder).getArgTypes().equals(argTypes));

            // the name will be used when show functions, so remove the name when it's dropped
            if (builders.getOrDefault(name, Lists.newArrayList()).isEmpty()) {
                builders.remove(name);
            }
        }
    }

    /**
     * use for search appropriate signature for UDFs if candidate more than one.
     */
    static class UdfSignatureSearcher implements ExplicitlyCastableSignature {

        private final List<FunctionSignature> signatures;
        private final List<Expression> arguments;

        public UdfSignatureSearcher(List<FunctionSignature> signatures, List<Expression> arguments) {
            this.signatures = signatures;
            this.arguments = arguments;
        }

        @Override
        public List<FunctionSignature> getSignatures() {
            return signatures;
        }

        @Override
        public FunctionSignature getSignature() {
            return searchSignature(signatures);
        }

        @Override
        public boolean nullable() {
            throw new AnalysisException("could not call nullable on UdfSignatureSearcher");
        }

        @Override
        public List<Expression> children() {
            return arguments;
        }

        @Override
        public Expression child(int index) {
            return arguments.get(index);
        }

        @Override
        public int arity() {
            return arguments.size();
        }

        @Override
        public <T> Optional<T> getMutableState(String key) {
            return Optional.empty();
        }

        @Override
        public void setMutableState(String key, Object value) {
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            throw new AnalysisException("could not call withChildren on UdfSignatureSearcher");
        }
    }
}
