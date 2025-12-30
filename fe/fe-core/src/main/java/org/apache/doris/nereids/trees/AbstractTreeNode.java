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

package org.apache.doris.nereids.trees;

import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.parser.ParserUtils;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.MutableState.EmptyMutableState;
import org.apache.doris.nereids.util.Utils;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;

/**
 * 树节点的抽象基类。
 * 为 Nereids 优化器中的计划节点（Plan）和表达式（Expression）提供通用的树节点实现。
 * 实现了 TreeNode 接口，提供了不可变的树结构、类型信息缓存、可变状态管理等基础功能。
 *
 * @param <NODE_TYPE> 节点类型，必须是 TreeNode 的子类型，通常是 Plan 或 Expression
 */
public abstract class AbstractTreeNode<NODE_TYPE extends TreeNode<NODE_TYPE>>
        implements TreeNode<NODE_TYPE> {

    /** 节点的来源信息，用于追踪节点在原始 SQL 中的位置 */
    protected final Optional<Origin> origin = ParserUtils.getOrigin();

    /** 类型位图，缓存了所有子节点及其自身的类型信息，用于快速类型检查 */
    protected final BitSet containsTypes;

    /** 子节点列表，使用不可变列表存储 */
    protected final List<NODE_TYPE> children;

    /**
     * 可变状态。
     * 注意：TreeNode 的其他字段都是不可变的，但在某些场景下需要可变状态。
     * 例如：重写框架需要区分计划是否由规则创建，可以通过设置此字段快速判断，而无需创建新的计划。
     * 应该尽量避免使用，因为可变状态容易导致 bug 且难以定位。
     */
    private MutableState mutableState = EmptyMutableState.INSTANCE;

    /**
     * 构造函数：使用可变参数列表。
     * 初始化子节点列表和类型位图。
     * 注意：使用 Utils.fastToImmutableList 直接生成不可变列表，避免 ImmutableList.copyOf 的额外克隆。
     *
     * @param children 子节点数组
     */
    protected AbstractTreeNode(NODE_TYPE... children) {
        // 直接生成不可变列表，避免额外的列表克隆
        this.children = Utils.fastToImmutableList(children);

        // 初始化类型位图：合并所有子节点的类型信息，并添加当前节点的类型
        this.containsTypes = new BitSet();
        for (NODE_TYPE child : children) {
            BitSet childTypes = child.getAllChildrenTypes();
            containsTypes.or(childTypes);
        }
        containsTypes.or(getSuperClassTypes());
    }

    /**
     * 构造函数：使用列表参数。
     * 初始化子节点列表和类型位图。
     * 注意：使用 Utils.fastToImmutableList 直接生成不可变列表，避免 ImmutableList.copyOf 的额外克隆。
     *
     * @param children 子节点列表
     */
    protected AbstractTreeNode(List<NODE_TYPE> children) {
        // 直接生成不可变列表，避免额外的列表克隆
        this.children = Utils.fastToImmutableList(children);

        // 初始化类型位图：合并所有子节点的类型信息，并添加当前节点的类型
        this.containsTypes = new BitSet();
        for (NODE_TYPE child : children) {
            BitSet childTypes = child.getAllChildrenTypes();
            containsTypes.or(childTypes);
        }
        containsTypes.or(getSuperClassTypes());
    }

    /**
     * 获取节点的来源信息。
     * 用于追踪节点在原始 SQL 中的位置，便于错误报告和调试。
     *
     * @return 节点的来源信息，如果不存在则返回空
     */
    @Override
    public Optional<Origin> getOrigin() {
        return origin;
    }

    /**
     * 获取指定索引位置的子节点。
     *
     * @param index 子节点的索引位置（从 0 开始）
     * @return 指定索引位置的子节点
     */
    @Override
    public NODE_TYPE child(int index) {
        return children.get(index);
    }

    /**
     * 获取所有子节点。
     *
     * @return 所有子节点的不可变列表
     */
    @Override
    public List<NODE_TYPE> children() {
        return children;
    }

    /**
     * 获取可变状态。
     * 根据键获取存储的可变状态值。
     *
     * @param key 状态键
     * @param <T> 状态值的类型
     * @return 状态值，如果不存在则返回空
     */
    @Override
    public <T> Optional<T> getMutableState(String key) {
        return mutableState.get(key);
    }

    /**
     * 设置可变状态。
     * 设置键值对到可变状态中，返回新的 MutableState 对象（不可变模式）。
     *
     * @param key 状态键
     * @param state 状态值
     */
    @Override
    public void setMutableState(String key, Object state) {
        this.mutableState = this.mutableState.set(key, state);
    }

    /**
     * 获取所有可变状态。
     *
     * @return 所有可变状态的 MutableState 对象
     */
    @Override
    public MutableState getMutableStates() {
        return mutableState;
    }

    /**
     * 获取所有子节点的类型位图。
     * 返回缓存的类型位图，包含所有子节点及其自身的类型信息。
     * 用于快速类型检查，避免递归遍历整个树。
     *
     * @return 类型位图
     */
    @Override
    public BitSet getAllChildrenTypes() {
        return containsTypes;
    }

    /**
     * 获取子节点数量（节点的度）。
     *
     * @return 子节点的数量
     */
    @Override
    public int arity() {
        return children.size();
    }

    /**
     * 获取当前节点的超类类型位图。
     * 用于类型检查和类型匹配。
     *
     * @return 超类类型位图
     */
    @Override
    public BitSet getSuperClassTypes() {
        return SuperClassId.getSuperClassIds(getClass());
    }
}
