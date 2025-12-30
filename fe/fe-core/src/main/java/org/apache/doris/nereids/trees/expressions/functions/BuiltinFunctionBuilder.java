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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.common.Pair;
import org.apache.doris.common.util.ReflectionUtils;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class used to resolve all builtin function
 */
public class BuiltinFunctionBuilder extends FunctionBuilder {
    public final int arity;

    public final boolean isVariableLength;

    // Concrete BoundFunction's constructor
    private final Constructor<BoundFunction> builderMethod;
    private final Class<? extends BoundFunction> functionClass;

    public BuiltinFunctionBuilder(
            Class<? extends BoundFunction> functionClass, Constructor<BoundFunction> builderMethod) {
        this.functionClass = Objects.requireNonNull(functionClass, "functionClass can not be null");
        this.builderMethod = Objects.requireNonNull(builderMethod, "builderMethod can not be null");
        this.arity = builderMethod.getParameterCount();
        this.isVariableLength = arity > 0 && builderMethod.getParameterTypes()[arity - 1].isArray();
    }

    public Constructor<BoundFunction> getBuilderMethod() {
        return builderMethod;
    }

    @Override
    public Class<? extends BoundFunction> functionClass() {
        return functionClass;
    }

    /**
     * 检查给定的参数列表是否能够匹配当前函数构建器的构造函数。
     *
     * 该方法执行两个层次的检查：
     * 1. 参数个数检查：验证参数个数是否匹配构造函数的要求
     *    - 变长参数函数：参数个数必须 >= arity - 1（至少要有固定参数部分）
     *    - 定长参数函数：参数个数必须 == arity（完全匹配）
     * 2. 参数类型检查：验证每个参数的类型是否匹配构造函数参数类型
     *    - 直接类型匹配：使用 isInstance() 检查
     *    - 基本类型兼容：处理基本类型和包装类型之间的兼容性（如 int 和 Integer）
     *
     * 示例：
     * - 构造函数：Concat(Expression... args)，arity = 1，isVariableLength = true
     *   - 参数 [expr1, expr2, expr3]：个数 3 >= 1-1 = 0，通过个数检查
     * - 构造函数：Abs(Expression arg)，arity = 1，isVariableLength = false
     *   - 参数 [expr]：个数 1 == 1，通过个数检查
     *   - 参数 [expr1, expr2]：个数 2 != 1，不通过个数检查
     *
     * @param arguments 待检查的参数列表
     * @return true 如果参数能够匹配构造函数，false 否则
     */
    @Override
    public boolean canApply(List<? extends Object> arguments) {
        // 步骤 1: 检查参数个数是否匹配
        if (isVariableLength) {
            // 变长参数函数：参数个数必须 >= arity - 1
            // 例如：Concat(Expression... args)，arity = 1
            //   - 至少需要 0 个参数（arity - 1 = 0），可以接受任意多个参数
            //   - 如果 arguments.size() = 0，则 arity (1) > 0 + 1 = 1，返回 false（错误）
            //   - 如果 arguments.size() = 1，则 arity (1) > 1 + 1 = 2，返回 false（错误）
            // 实际上应该是：arity - 1 <= arguments.size()
            // 即：arguments.size() >= arity - 1
            // 所以条件应该是：arity > arguments.size() + 1 等价于 arguments.size() < arity - 1
            if (arity > arguments.size() + 1) {
                return false;
            }
        } else {
            // 定长参数函数：参数个数必须完全匹配
            // 例如：Abs(Expression arg)，arity = 1
            //   - 必须恰好有 1 个参数
            if (arguments.size() != arity) {
                return false;
            }
        }

        // 步骤 2: 检查每个参数的类型是否匹配构造函数参数类型
        for (int i = 0; i < arguments.size(); i++) {
            // 获取构造函数中第 i 个参数的类型
            // 对于变长参数，索引 >= arity - 1 的参数都使用数组元素类型
            Class constructorArgumentType = getConstructorArgumentType(i);
            Object argument = arguments.get(i);
            
            // 检查参数类型是否直接匹配（使用 isInstance() 进行运行时类型检查）
            if (!constructorArgumentType.isInstance(argument)) {
                // 如果直接类型不匹配，尝试基本类型兼容性检查
                // 例如：构造函数参数是 Integer，实际参数是 int（基本类型）
                Optional<Class> primitiveType = ReflectionUtils.getPrimitiveType(argument.getClass());
                if (!primitiveType.isPresent() || !constructorArgumentType.isAssignableFrom(primitiveType.get())) {
                    // 如果无法找到对应的基本类型，或者基本类型也无法赋值给构造函数参数类型，则不匹配
                    return false;
                }
            }
        }
        // 所有检查通过，参数可以匹配构造函数
        return true;
    }

    private Class getConstructorArgumentType(int index) {
        if (isVariableLength && index + 1 >= arity) {
            return builderMethod.getParameterTypes()[arity - 1].getComponentType();
        }
        return builderMethod.getParameterTypes()[index];
    }

    @Override
    public Pair<BoundFunction, BoundFunction> build(String name, List<? extends Object> arguments) {
        try {
            if (isVariableLength) {
                // 变长参数构造器：将固定参数原样传入，将可变参数段打包成数组实例，匹配最后一个数组类型入参
                return Pair.ofSame(builderMethod.newInstance(toVariableLengthArguments(arguments)));
            } else {
                // 定长参数构造器：直接以 arguments.toArray() 作为入参，按顺序调用构造函数
                return Pair.ofSame(builderMethod.newInstance(arguments.toArray()));
            }
        } catch (Throwable t) {
            String argString = arguments.stream()
                    .map(arg -> {
                        if (arg == null) {
                            return "null";
                        } else if (arg instanceof Expression) {
                            return ((Expression) arg).toSql();
                        } else {
                            return arg.toString();
                        }
                    })
                    .collect(Collectors.joining(", ", "(", ")"));
            throw new IllegalStateException("Can not build function: '" + name
                    + "', expression: " + name + argString + ", " + t.getCause().getMessage(), t);
        }
    }

    private Object[] toVariableLengthArguments(List<? extends Object> arguments) {
        Object[] constructorArguments = new Object[arity];

        List<?> nonVarArgs = arguments.subList(0, arity - 1);
        for (int i = 0; i < nonVarArgs.size(); i++) {
            constructorArguments[i] = nonVarArgs.get(i);
        }

        List<?> varArgs = arguments.subList(arity - 1, arguments.size());
        Class constructorArgumentType = getConstructorArgumentType(arity);
        Object varArg = Array.newInstance(constructorArgumentType, varArgs.size());
        for (int i = 0; i < varArgs.size(); i++) {
            Array.set(varArg, i, varArgs.get(i));
        }
        constructorArguments[arity - 1] = varArg;

        return constructorArguments;
    }

    /**
     * resolve a Concrete boundFunction's class and convert the constructors to
     * FunctionBuilder
     * @param functionClass a class which is the child class of BoundFunction and can not be abstract class
     * @return list of FunctionBuilder which contains the constructor
     */
    public static List<FunctionBuilder> resolve(Class<? extends BoundFunction> functionClass) {
        Preconditions.checkArgument(!Modifier.isAbstract(functionClass.getModifiers()),
                "Can not resolve bind function which is abstract class: "
                        + functionClass.getSimpleName());
        return Arrays.stream(functionClass.getConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .map(constructor -> new BuiltinFunctionBuilder(functionClass, (Constructor<BoundFunction>) constructor))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String parameterDisplayString() {
        return Arrays.stream(builderMethod.getParameterTypes())
                .map(p -> {
                    if (p.isArray()) {
                        return p.getComponentType().getSimpleName() + "...";
                    } else {
                        return p.getSimpleName();
                    }
                })
                .collect(Collectors.joining(", ", "(", ")"));
    }
}
