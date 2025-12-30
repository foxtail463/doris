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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AccessControllerManager is the entry point of privilege authentication.
 * There are 2 kinds of access controller:
 * SystemAccessController: for global level priv, resource priv and other Doris internal priv checking
 * CatalogAccessController: for specified catalog's priv checking, can be customized.
 * And using InternalCatalogAccessController as default.
 * 
 * AccessControllerManager 是权限认证的入口点。
 * 有两种类型的访问控制器：
 * SystemAccessController: 用于全局级别权限、资源权限和其他 Doris 内部权限检查
 * CatalogAccessController: 用于指定 catalog 的权限检查，可以自定义。
 * 默认使用 InternalCatalogAccessController。
 */
public class AccessControllerManager {
    private static final Logger LOG = LogManager.getLogger(AccessControllerManager.class);

    /** 认证对象，用于权限管理 */
    private Auth auth;
    /** 默认访问控制器实例，用于处理未指定特定控制器的情况 */
    // Default access controller instance used for handling cases where no specific controller is specified
    private CatalogAccessController defaultAccessController;
    /** 存储 catalog 与其对应访问控制器之间映射关系的 Map */
    // Map that stores the mapping between catalogs and their corresponding access controllers
    private Map<String, CatalogAccessController> ctlToCtlAccessController = Maps.newConcurrentMap();
    /** 已加载的访问控制器工厂缓存，用于快速创建新的访问控制器 */
    // Cache of loaded access controller factories for quick creation of new access controllers
    private ConcurrentHashMap<String, AccessControllerFactory> accessControllerFactoriesCache
            = new ConcurrentHashMap<>();
    /** 访问控制器类名与其标识符之间的映射，便于查找工厂标识符 */
    // Mapping between access controller class names and their identifiers for easy lookup of factory identifiers
    private ConcurrentHashMap<String, String> accessControllerClassNameMapping = new ConcurrentHashMap<>();

    /**
     * 构造函数，初始化访问控制器管理器
     * @param auth 认证对象
     */
    public AccessControllerManager(Auth auth) {
        this.auth = auth;
        // 加载访问控制器插件
        loadAccessControllerPlugins();
        // 从配置中获取访问控制器类型
        String accessControllerName = Config.access_controller_type;
        // 加载或创建默认访问控制器
        this.defaultAccessController = loadAccessControllerOrThrow(accessControllerName);
        // 将内部 catalog 与默认访问控制器关联
        ctlToCtlAccessController.put(InternalCatalog.INTERNAL_CATALOG_NAME, defaultAccessController);
    }

    /**
     * 加载访问控制器，如果加载失败则抛出异常
     * @param accessControllerName 访问控制器名称
     * @return 访问控制器实例
     */
    private CatalogAccessController loadAccessControllerOrThrow(String accessControllerName) {
        // 如果指定为 "default"，则创建内部访问控制器
        if (accessControllerName.equalsIgnoreCase("default")) {
            return new InternalAccessController(auth);
        }
        // 如果缓存中存在该访问控制器的工厂，则使用工厂创建实例
        if (accessControllerFactoriesCache.containsKey(accessControllerName)) {
            Map<String, String> prop;
            try {
                // 加载访问控制器属性配置
                prop = PropertiesUtils.loadAccessControllerPropertiesOrNull();
            } catch (IOException e) {
                throw new RuntimeException("Failed to load authorization properties."
                        + "Please check the configuration file, authorization name is " + accessControllerName, e);
            }
            // 使用工厂创建访问控制器
            return accessControllerFactoriesCache.get(accessControllerName).createAccessController(prop);
        }
        // 如果找不到对应的工厂，抛出异常
        throw new RuntimeException("No authorization plugin factory found for " + accessControllerName
                + ". Please confirm that your plugin is placed in the correct location.");
    }

    /**
     * 加载访问控制器插件
     * 从类路径和目录中加载所有可用的访问控制器工厂
     */
    private void loadAccessControllerPlugins() {
        // 从类路径加载访问控制器工厂
        ServiceLoader<AccessControllerFactory> loaderFromClasspath = ServiceLoader.load(AccessControllerFactory.class);
        for (AccessControllerFactory factory : loaderFromClasspath) {
            LOG.info("Found Authentication Plugin Factories: {} from class path.", factory.factoryIdentifier());
            // 将工厂添加到缓存中
            accessControllerFactoriesCache.put(factory.factoryIdentifier(), factory);
            // 建立类名到标识符的映射
            accessControllerClassNameMapping.put(factory.getClass().getName(), factory.factoryIdentifier());
        }
        // 从目录加载访问控制器工厂
        List<AccessControllerFactory> loader = null;
        try {
            loader = ClassLoaderUtils.loadServicesFromDirectory(AccessControllerFactory.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Authentication Plugin Factories", e);
        }
        for (AccessControllerFactory factory : loader) {
            LOG.info("Found Access Controller Plugin Factory: {} from directory.", factory.factoryIdentifier());
            // 将工厂添加到缓存中
            accessControllerFactoriesCache.put(factory.factoryIdentifier(), factory);
            // 建立类名到标识符的映射
            accessControllerClassNameMapping.put(factory.getClass().getName(), factory.factoryIdentifier());
        }
    }

    /**
     * 获取指定 catalog 的访问控制器，如果不存在则返回默认访问控制器
     * @param ctl catalog 名称
     * @return 访问控制器实例
     */
    public CatalogAccessController getAccessControllerOrDefault(String ctl) {
        // 先从缓存中查找
        CatalogAccessController catalogAccessController = ctlToCtlAccessController.get(ctl);
        if (catalogAccessController != null) {
            return catalogAccessController;
        }
        // 如果缓存中没有，尝试获取 catalog 对象
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
        // 如果是外部 catalog，则延迟加载其访问控制器
        if (catalog != null && catalog instanceof ExternalCatalog) {
            lazyLoadCtlAccessController((ExternalCatalog) catalog);
            return ctlToCtlAccessController.get(ctl);
        }

        // 如果都没有找到，返回默认访问控制器
        return defaultAccessController;
    }

    /**
     * 延迟加载外部 catalog 的访问控制器
     * @param catalog 外部 catalog 对象
     */
    private synchronized void lazyLoadCtlAccessController(ExternalCatalog catalog) {
        // 如果已经存在，直接返回
        if (ctlToCtlAccessController.containsKey(catalog.getName())) {
            return;
        }
        // 初始化 catalog 的访问控制器
        catalog.initAccessController(false);
        // 如果初始化后仍然没有，则使用默认访问控制器
        if (!ctlToCtlAccessController.containsKey(catalog.getName())) {
            ctlToCtlAccessController.put(catalog.getName(), defaultAccessController);
        }
    }

    /**
     * 检查指定 catalog 是否存在访问控制器
     * @param ctl catalog 名称
     * @return 如果存在返回 true，否则返回 false
     */
    public boolean checkIfAccessControllerExist(String ctl) {
        return ctlToCtlAccessController.containsKey(ctl);
    }

    /**
     * 为指定 catalog 创建访问控制器
     * @param ctl catalog 名称
     * @param acFactoryClassName 访问控制器工厂类名
     * @param prop 属性配置
     * @param isDryRun 是否为试运行模式（不实际创建）
     */
    public void createAccessController(String ctl, String acFactoryClassName, Map<String, String> prop,
                                       boolean isDryRun) {
        // 获取插件标识符
        String pluginIdentifier = getPluginIdentifierForAccessController(acFactoryClassName);
        // 使用工厂创建访问控制器
        CatalogAccessController accessController = accessControllerFactoriesCache.get(pluginIdentifier)
                .createAccessController(prop);
        // 如果不是试运行模式，则将访问控制器添加到映射中
        if (!isDryRun) {
            ctlToCtlAccessController.put(ctl, accessController);
            LOG.info("create access controller {} for catalog {}", acFactoryClassName, ctl);
        }
    }

    /**
     * 根据访问控制器类名获取插件标识符
     * @param acClassName 访问控制器类名
     * @return 插件标识符
     */
    private String getPluginIdentifierForAccessController(String acClassName) {
        String pluginIdentifier = null;
        // 先从类名映射中查找
        if (accessControllerClassNameMapping.containsKey(acClassName)) {
            pluginIdentifier = accessControllerClassNameMapping.get(acClassName);
        }
        // 如果类名本身就是标识符，直接使用
        if (accessControllerFactoriesCache.containsKey(acClassName)) {
            pluginIdentifier = acClassName;
        }
        // 如果找不到对应的工厂，抛出异常
        if (null == pluginIdentifier || !accessControllerFactoriesCache.containsKey(pluginIdentifier)) {
            throw new RuntimeException("Access Controller Plugin Factory not found for " + acClassName);
        }
        return pluginIdentifier;
    }

    /**
     * 移除指定 catalog 的访问控制器
     * @param ctl catalog 名称
     */
    public void removeAccessController(String ctl) {
        if (StringUtils.isBlank(ctl)) {
            return;
        }
        if (ctlToCtlAccessController.containsKey(ctl)) {
            ctlToCtlAccessController.remove(ctl);
        }
        LOG.info("remove access controller for catalog {}", ctl);
    }

    /**
     * 获取认证对象
     * @return 认证对象
     */
    public Auth getAuth() {
        return this.auth;
    }

    // ==== Global ====
    /**
     * 检查全局权限
     * @param ctx 连接上下文
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    /**
     * 检查全局权限
     * @param currentUser 当前用户身份
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return defaultAccessController.checkGlobalPriv(currentUser, wanted);
    }

    // ==== Catalog ====
    /**
     * 检查 catalog 权限
     * @param ctx 连接上下文
     * @param ctl catalog 名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted);
    }

    /**
     * 检查 catalog 权限
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        // 先检查是否有全局权限
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        // 检查 catalog 权限时，始终使用 InternalAccessController
        // 因为 catalog 权限只保存在 InternalAccessController 中
        // for checking catalog priv, always use InternalAccessController.
        // because catalog priv is only saved in InternalAccessController.
        return defaultAccessController.checkCtlPriv(hasGlobal, currentUser, ctl, wanted);
    }

    // ==== Database ====
    /**
     * 检查数据库权限
     * @param ctx 连接上下文
     * @param ctl catalog 名称
     * @param db 数据库名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), ctl, db, wanted);
    }

    /**
     * 检查数据库权限
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param db 数据库名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        // 先检查是否有全局权限
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        // 使用对应 catalog 的访问控制器检查数据库权限
        return getAccessControllerOrDefault(ctl).checkDbPriv(hasGlobal, currentUser, ctl, db, wanted);
    }

    // ==== Table ====
    /**
     * 检查表权限
     * @param ctx 连接上下文
     * @param tableName 表名信息
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkTblPriv(ConnectContext ctx, TableNameInfo tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), wanted);
    }

    /**
     * 检查表权限
     * @param ctx 连接上下文
     * @param qualifiedCtl 完整的 catalog 名称
     * @param qualifiedDb 完整的数据库名称
     * @param tbl 表名
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
                                String qualifiedDb, String tbl, PrivPredicate wanted) {
        // 如果设置了跳过认证，直接返回 true
        if (ctx.isSkipAuth()) {
            return true;
        }
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedCtl, qualifiedDb, tbl, wanted);
    }

    /**
     * 检查表权限
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param db 数据库名称
     * @param tbl 表名
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        // 先检查是否有全局权限
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        // 使用对应 catalog 的访问控制器检查表权限
        return getAccessControllerOrDefault(ctl).checkTblPriv(hasGlobal, currentUser, ctl, db, tbl, wanted);
    }

    // ==== Column ====
    /**
     * 检查列权限
     * 如果参数包含 ctx，可以通过 ctx 中的 isSkipAuth 字段跳过认证
     * If param has ctx, we can skip auth by isSkipAuth field in ctx
     * @param ctx 连接上下文
     * @param ctl catalog 名称
     * @param qualifiedDb 完整的数据库名称
     * @param tbl 表名
     * @param cols 列名集合
     * @param wanted 需要的权限谓词
     * @throws UserException 用户异常
     */
    public void checkColumnsPriv(ConnectContext ctx, String ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        // 如果设置了跳过认证，直接返回
        if (ctx.isSkipAuth()) {
            return;
        }
        checkColumnsPriv(ctx.getCurrentUserIdentity(), ctl, qualifiedDb, tbl, cols, wanted);
    }

    /**
     * 检查列权限
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param qualifiedDb 完整的数据库名称
     * @param tbl 表名
     * @param cols 列名集合
     * @param wanted 需要的权限谓词
     * @throws UserException 用户异常
     */
    public void checkColumnsPriv(UserIdentity currentUser, String
            ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        // 先检查是否有全局权限
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        // 获取对应 catalog 的访问控制器
        CatalogAccessController accessController = getAccessControllerOrDefault(ctl);
        // 记录开始时间，用于性能监控
        long start = System.currentTimeMillis();
        // 检查列权限
        accessController.checkColsPriv(hasGlobal, currentUser, ctl, qualifiedDb,
                tbl, cols, wanted);
        // 如果是调试模式，记录检查耗时
        if (LOG.isDebugEnabled()) {
            LOG.debug("checkColumnsPriv use {} mills, user: {}, ctl: {}, db: {}, table: {}, cols: {}",
                    System.currentTimeMillis() - start, currentUser, ctl, qualifiedDb, tbl, cols);
        }
    }

    // ==== Resource ====
    /**
     * 检查资源权限
     * @param ctx 连接上下文
     * @param resourceName 资源名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    /**
     * 检查资源权限
     * @param currentUser 当前用户身份
     * @param resourceName 资源名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return defaultAccessController.checkResourcePriv(currentUser, resourceName, wanted);
    }

    // ==== Cloud ====
    /**
     * 检查云资源权限
     * @param ctx 连接上下文
     * @param cloudName 云名称
     * @param wanted 需要的权限谓词
     * @param type 资源类型
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkCloudPriv(ConnectContext ctx, String cloudName, PrivPredicate wanted, ResourceTypeEnum type) {
        return checkCloudPriv(ctx.getCurrentUserIdentity(), cloudName, wanted, type);
    }

    /**
     * 检查云资源权限
     * @param currentUser 当前用户身份
     * @param cloudName 云名称
     * @param wanted 需要的权限谓词
     * @param type 资源类型
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
                                  PrivPredicate wanted, ResourceTypeEnum type) {
        return defaultAccessController.checkCloudPriv(currentUser, cloudName, wanted, type);
    }

    /**
     * 检查存储库权限
     * @param ctx 连接上下文
     * @param storageVaultName 存储库名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkStorageVaultPriv(ConnectContext ctx, String storageVaultName, PrivPredicate wanted) {
        return checkStorageVaultPriv(ctx.getCurrentUserIdentity(), storageVaultName, wanted);
    }

    /**
     * 检查存储库权限
     * @param currentUser 当前用户身份
     * @param storageVaultName 存储库名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted) {
        return defaultAccessController.checkStorageVaultPriv(currentUser, storageVaultName, wanted);
    }

    /**
     * 检查工作负载组权限
     * @param ctx 连接上下文
     * @param workloadGroupName 工作负载组名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkWorkloadGroupPriv(ConnectContext ctx, String workloadGroupName, PrivPredicate wanted) {
        return checkWorkloadGroupPriv(ctx.getCurrentUserIdentity(), workloadGroupName, wanted);
    }

    /**
     * 检查工作负载组权限
     * @param currentUser 当前用户身份
     * @param workloadGroupName 工作负载组名称
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return defaultAccessController.checkWorkloadGroupPriv(currentUser, workloadGroupName, wanted);
    }

    // ==== Other ====
    /**
     * 根据授权信息检查权限
     * @param ctx 连接上下文
     * @param authInfo 授权信息
     * @param wanted 需要的权限谓词
     * @return 如果有权限返回 true，否则返回 false
     */
    public boolean checkPrivByAuthInfo(ConnectContext ctx, AuthorizationInfo authInfo, PrivPredicate wanted) {
        // 如果授权信息为空，返回 false
        if (authInfo == null) {
            return false;
        }
        // 如果数据库名称为空，返回 false
        if (authInfo.getDbName() == null) {
            return false;
        }
        // 如果表名列表为空，只检查数据库权限
        if (authInfo.getTableNameList() == null || authInfo.getTableNameList().isEmpty()) {
            return checkDbPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(), wanted);
        }
        // 遍历表名列表，检查每个表的权限
        for (String tblName : authInfo.getTableNameList()) {
            if (!checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(),
                    tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 评估数据脱敏策略（批量）
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param db 数据库名称
     * @param tbl 表名
     * @param cols 列名集合
     * @return 列名到数据脱敏策略的映射
     */
    public Map<String, Optional<DataMaskPolicy>> evalDataMaskPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl, Set<String> cols) {
        Map<String, Optional<DataMaskPolicy>> res = Maps.newHashMap();
        // 为每个列评估数据脱敏策略
        for (String col : cols) {
            res.put(col, evalDataMaskPolicy(currentUser, ctl, db, tbl, col));
        }
        return res;
    }

    /**
     * 评估数据脱敏策略（单个列）
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param db 数据库名称
     * @param tbl 表名
     * @param col 列名
     * @return 数据脱敏策略（如果存在）
     */
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String
            ctl, String db, String tbl, String col) {
        // 参数校验
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        Objects.requireNonNull(col, "require col object");
        // 使用对应 catalog 的访问控制器评估数据脱敏策略（列名转为小写）
        return getAccessControllerOrDefault(ctl).evalDataMaskPolicy(currentUser, ctl, db, tbl, col.toLowerCase());
    }

    /**
     * 评估行过滤策略
     * @param currentUser 当前用户身份
     * @param ctl catalog 名称
     * @param db 数据库名称
     * @param tbl 表名
     * @return 行过滤策略列表
     */
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl) {
        // 参数校验
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        // 使用对应 catalog 的访问控制器评估行过滤策略
        return getAccessControllerOrDefault(ctl).evalRowFilterPolicies(currentUser, ctl, db, tbl);
    }
}
