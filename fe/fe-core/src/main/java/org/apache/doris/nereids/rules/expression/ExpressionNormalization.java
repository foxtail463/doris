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

import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.ConcatWsMultiArrayToOne;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.rules.expression.rules.DigitalMaskingConvert;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.InPredicateDedup;
import org.apache.doris.nereids.rules.expression.rules.InPredicateExtractNonConstant;
import org.apache.doris.nereids.rules.expression.rules.InPredicateToEqualToRule;
import org.apache.doris.nereids.rules.expression.rules.LogToLn;
import org.apache.doris.nereids.rules.expression.rules.MedianConvert;
import org.apache.doris.nereids.rules.expression.rules.MergeDateTrunc;
import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.NormalizeStructElement;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticComparisonRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyCastRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyEqualBooleanLiteral;
import org.apache.doris.nereids.rules.expression.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.rules.expression.rules.SupportJavaDateFormatter;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * normalize expression of plan rule set.
 */
public class ExpressionNormalization extends ExpressionRewrite {
    // we should run supportJavaDateFormatter before foldConstantRule or be will fold
    // from_unixtime(timestamp, 'yyyyMMdd') to 'yyyyMMdd'
    // specically note: LogToLn and ConcatWsMultiArrayToOne must  before FoldConstantRule,otherwise log will core when
    // input single argument like log(100),and concat_ws will retuen a wrong result when input multi array
    /**
     * 表达式规范化规则列表。
     * 这些规则按顺序执行，将表达式转换为规范形式，便于后续优化和匹配。
     * 
     * 规则执行顺序说明：
     * 1. SupportJavaDateFormatter 必须在 FoldConstantRule 之前执行，否则 BE 会将
     *    from_unixtime(timestamp, 'yyyyMMdd') 折叠为 'yyyyMMdd'
     * 2. LogToLn 和 ConcatWsMultiArrayToOne 必须在 FoldConstantRule 之前执行，
     *    否则 log(100) 会 core，concat_ws 输入多数组时会返回错误结果
     */
    public static final List<ExpressionRewriteRule<ExpressionRewriteContext>> NORMALIZE_REWRITE_RULES
                = ImmutableList.of(
            bottomUp(
                // 1. 支持 Java 日期格式化函数（如 from_unixtime）
                //    必须在常量折叠之前执行，避免格式字符串被错误折叠
                SupportJavaDateFormatter.INSTANCE,
                
                // 2. 规范化二元比较谓词：将常量放在右边，列放在左边
                //    例如：5 > id → id < 5
                NormalizeBinaryPredicatesRule.INSTANCE,
                
                // 3. 去重 IN 谓词中的重复值
                //    例如：id IN (1, 1, 2) → id IN (1, 2)
                InPredicateDedup.INSTANCE,
                
                // 4. 提取 IN 谓词中的非常量表达式
                //    例如：id IN (1, 2, col1) → id IN (1, 2) AND id = col1
                InPredicateExtractNonConstant.INSTANCE,
                
                // 5. 将单元素的 IN 谓词转换为等值比较
                //    例如：id IN (5) → id = 5
                InPredicateToEqualToRule.INSTANCE,
                
                // 6. 简化 NOT 表达式
                //    例如：NOT NOT a → a
                //         NOT a > b → a <= b
                //         NOT (a >= b AND a <= c) → (a < b OR a > c)
                SimplifyNotExprRule.INSTANCE,
                
                // 7. 简化算术表达式：展平并重新组织算术运算
                //    例如：a + 1 + b - 2 - ((c - d) + 1) → a + b - c + d - 2
                //    在常量折叠之前执行，将变量和常量分离
                SimplifyArithmeticRule.INSTANCE,
                
                // 8. 将 log 函数转换为 ln（自然对数）
                //    例如：log(x) → ln(x)
                LogToLn.INSTANCE,
                
                // 9. 将多个数组参数的 concat_ws 转换为单个数组
                //    例如：concat_ws(',', [a], [b], [c]) → concat_ws(',', [a, b, c])
                ConcatWsMultiArrayToOne.INSTANCE,
                
                // 10. 常量折叠：计算常量表达式的值
                //     例如：1 + 2 → 3, 'a' || 'b' → 'ab'
                //     必须在 LogToLn 和 ConcatWsMultiArrayToOne 之后执行
                FoldConstantRule.INSTANCE,
                
                // 11. 简化类型转换表达式
                //     例如：cast(cast(x as int) as int) → cast(x as int)
                SimplifyCastRule.INSTANCE,
                
                // 12. 数字掩码转换：处理数据脱敏相关的函数
                DigitalMaskingConvert.INSTANCE,
                
                // 13. 中位数转换：将 median 函数转换为其他形式
                MedianConvert.INSTANCE,
                
                // 14. 简化算术比较表达式
                //     例如：a + 1 > a + 2 → 1 > 2 → false
                SimplifyArithmeticComparisonRule.INSTANCE,
                
                // 15. 转换聚合状态类型转换
                ConvertAggStateCast.INSTANCE,
                
                // 16. 合并 date_trunc 函数调用
                //     例如：date_trunc(date_trunc(x, 'day'), 'day') → date_trunc(x, 'day')
                MergeDateTrunc.INSTANCE,
                
                // 17. 规范化结构体元素访问
                NormalizeStructElement.INSTANCE,
                
                // 18. 检查类型转换的有效性
                CheckCast.INSTANCE,
                
                // 19. 简化布尔字面量的等值比较
                //     例如：x = true → x, x = false → NOT x
                SimplifyEqualBooleanLiteral.INSTANCE
            )
    );

    public ExpressionNormalization() {
        super(new ExpressionRuleExecutor(NORMALIZE_REWRITE_RULES));
    }

    @Override
    public Expression rewrite(Expression expression, ExpressionRewriteContext expressionRewriteContext) {
        return super.rewrite(expression, expressionRewriteContext);
    }
}
