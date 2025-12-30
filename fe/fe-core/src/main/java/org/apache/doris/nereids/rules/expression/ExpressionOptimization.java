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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.rules.expression.rules.AddMinMax;
import org.apache.doris.nereids.rules.expression.rules.ArrayContainToArrayOverlap;
import org.apache.doris.nereids.rules.expression.rules.BetweenToEqual;
import org.apache.doris.nereids.rules.expression.rules.CaseWhenToCompoundPredicate;
import org.apache.doris.nereids.rules.expression.rules.CaseWhenToIf;
import org.apache.doris.nereids.rules.expression.rules.CondReplaceNullWithFalse;
import org.apache.doris.nereids.rules.expression.rules.DateFunctionRewrite;
import org.apache.doris.nereids.rules.expression.rules.DistinctPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.ExtractCommonFactorRule;
import org.apache.doris.nereids.rules.expression.rules.LikeToEqualRewrite;
import org.apache.doris.nereids.rules.expression.rules.NestedCaseWhenCondToLiteral;
import org.apache.doris.nereids.rules.expression.rules.NullSafeEqualToEqual;
import org.apache.doris.nereids.rules.expression.rules.PushIntoCaseWhenBranch;
import org.apache.doris.nereids.rules.expression.rules.SimplifyComparisonPredicate;
import org.apache.doris.nereids.rules.expression.rules.SimplifyConflictCompound;
import org.apache.doris.nereids.rules.expression.rules.SimplifyInPredicate;
import org.apache.doris.nereids.rules.expression.rules.SimplifyRange;
import org.apache.doris.nereids.rules.expression.rules.SimplifySelfComparison;
import org.apache.doris.nereids.rules.expression.rules.TopnToMax;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * optimize expression of plan rule set.
 */
public class ExpressionOptimization extends ExpressionRewrite {
    /**
     * 表达式优化规则列表。
     * 这些规则在规范化之后执行，进一步优化和简化表达式，提高查询性能。
     */
    public static final List<ExpressionRewriteRule<ExpressionRewriteContext>> OPTIMIZE_REWRITE_RULES = ImmutableList.of(
            bottomUp(
                    // 1. 简化 IN 谓词：优化日期类型转换
                    //    例如：cast(date_col as DateV2) IN (DateTimeV2Literal, ...) 
                    //          → date_col IN (DateV2Literal, ...)（如果转换无损失）
                    SimplifyInPredicate.INSTANCE,

                    // ========== 比较谓词优化 ==========
                    
                    // 2. 简化比较谓词：优化类型转换和范围限制
                    //    例如：cast(c1 as DateV2) >= DateV2Literal → c1 >= DateLiteral
                    //         cast(c1 AS double) > 2.0 → c1 >= 2（c1 是整数类型）
                    SimplifyComparisonPredicate.INSTANCE,
                    
                    // 3. 简化自比较：消除冗余的自比较表达式
                    //    例如：a = a → TRUE
                    //         a + b > a + b → FALSE
                    SimplifySelfComparison.INSTANCE,

                    // ========== 复合谓词优化 ==========
                    
                    // 4. 简化范围谓词：合并和优化范围条件
                    //    例如：a > 1 AND a > 2 → a > 2
                    //         a > 1 OR a > 2 → a > 1
                    //         a IN (1,2,3) AND a > 1 → a IN (2,3)
                    //         a IN (1,2,3) AND a IN (3,4,5) → a = 3
                    //         a IN (1,2,3) AND a IN (4,5,6) → FALSE
                    SimplifyRange.INSTANCE,
                    
                    // 5. 简化冲突的复合谓词：消除矛盾的条件
                    //    例如：x AND NOT x → FALSE
                    //         x OR NOT x → TRUE
                    SimplifyConflictCompound.INSTANCE,
                    
                    // 6. 去重复合谓词中的重复表达式
                    //    例如：(a = 1) AND (b > 2) AND (a = 1) → (a = 1) AND (b > 2)
                    //         (a = 1) OR (a = 1) → (a = 1)
                    DistinctPredicatesRule.INSTANCE,
                    
                    // 7. 提取公共因子：优化复合谓词的结构
                    //    例如：(a OR b) AND (a OR c) → a OR (b AND c)
                    //         (a AND b) OR (a AND c) → a AND (b OR c)
                    ExtractCommonFactorRule.INSTANCE,

                    // ========== 函数和表达式转换 ==========
                    
                    // 8. 日期函数重写：优化日期相关函数
                    DateFunctionRewrite.INSTANCE,
                    
                    // 9. 数组包含转换为数组重叠
                    //    例如：array_contain(col, val) → array_overlap(col, [val])
                    ArrayContainToArrayOverlap.INSTANCE,
                    
                    // 10. 条件中 NULL 替换为 FALSE
                    CondReplaceNullWithFalse.INSTANCE,
                    
                    // 11. 嵌套 CASE WHEN 条件转换为字面量
                    NestedCaseWhenCondToLiteral.INSTANCE,
                    
                    // 12. CASE WHEN 转换为 IF 函数
                    CaseWhenToIf.INSTANCE,
                    
                    // 13. CASE WHEN 转换为复合谓词
                    CaseWhenToCompoundPredicate.INSTANCE,
                    
                    // 14. 将表达式推入 CASE WHEN 分支
                    PushIntoCaseWhenBranch.INSTANCE,
                    
                    // 15. TopN 转换为 MAX（特定场景优化）
                    TopnToMax.INSTANCE,
                    
                    // 16. NULL 安全等值转换为普通等值
                    //     例如：a <=> b → a = b（当可以确定非 NULL 时）
                    NullSafeEqualToEqual.INSTANCE,
                    
                    // 17. LIKE 转换为等值比较（当模式是精确匹配时）
                    //     例如：name LIKE 'John' → name = 'John'
                    LikeToEqualRewrite.INSTANCE,
                    
                    // 18. BETWEEN 转换为等值比较（当上下界相等时）
                    //     例如：a BETWEEN 5 AND 5 → a = 5
                    BetweenToEqual.INSTANCE
            )
    );

    /**
     * don't use it with PushDownFilterThroughJoin, it may cause dead loop:
     *   LogicalFilter(origin expr)
     *      => LogicalFilter((origin expr) and (add min max range))
     *      => LogicalFilter((origin expr)) // use PushDownFilterThroughJoin
     *      => ...
     */
    public static final List<ExpressionRewriteRule<ExpressionRewriteContext>> ADD_RANGE = ImmutableList.of(
            bottomUp(
                    AddMinMax.INSTANCE
            )
    );

    private static final ExpressionRuleExecutor EXECUTOR = new ExpressionRuleExecutor(OPTIMIZE_REWRITE_RULES);

    public ExpressionOptimization() {
        super(EXECUTOR);
    }
}
