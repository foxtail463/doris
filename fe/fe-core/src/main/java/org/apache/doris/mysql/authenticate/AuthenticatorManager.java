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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

public class AuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticatorManager.class);

    private static volatile Authenticator defaultAuthenticator = null;
    private static volatile Authenticator authTypeAuthenticator = null;

    public AuthenticatorManager(String type) {
        LOG.info("Authenticate type: {}", type);
        defaultAuthenticator = new DefaultAuthenticator();
        if (authTypeAuthenticator == null) {
            synchronized (AuthenticatorManager.class) {
                if (authTypeAuthenticator == null) {
                    try {
                        authTypeAuthenticator = loadFactoriesByName(type);
                    } catch (Exception e) {
                        LOG.warn("Failed to load authenticator by name: {}, using default authenticator", type, e);
                        authTypeAuthenticator = defaultAuthenticator;
                    }
                }
            }
        }
    }


    private Authenticator loadFactoriesByName(String identifier) throws Exception {
        ServiceLoader<AuthenticatorFactory> loader = ServiceLoader.load(AuthenticatorFactory.class);
        for (AuthenticatorFactory factory : loader) {
            LOG.info("Found Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }
        return loadCustomerFactories(identifier);

    }

    private Authenticator loadCustomerFactories(String identifier) throws Exception {
        List<AuthenticatorFactory> factories = ClassLoaderUtils.loadServicesFromDirectory(AuthenticatorFactory.class);
        if (factories.isEmpty()) {
            LOG.info("No customer authenticator found, using default authenticator");
            return defaultAuthenticator;
        }
        for (AuthenticatorFactory factory : factories) {
            LOG.info("Found Customer Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }

        throw new RuntimeException("No AuthenticatorFactory found for identifier: " + identifier);
    }

    /**
     * 执行用户认证流程
     * 
     * 该方法负责完整的MySQL客户端认证过程，包括：
     * 1. 根据用户名选择合适的认证器（默认认证器或插件认证器如LDAP）
     * 2. 从认证包中解析用户密码
     * 3. 使用认证器验证用户名和密码
     * 4. 认证成功后设置用户身份信息到连接上下文
     * 
     * @param context 连接上下文，包含MySQL连接相关信息
     * @param userName 用户名
     * @param channel MySQL通道，用于网络通信
     * @param serializer MySQL序列化器，用于数据序列化
     * @param authPacket MySQL认证包，包含客户端发送的认证信息
     * @param handshakePacket MySQL握手包，包含握手阶段的信息
     * @return true表示认证成功，false表示认证失败
     * @throws IOException 如果发生IO错误
     */
    public boolean authenticate(ConnectContext context,
                                String userName,
                                MysqlChannel channel,
                                MysqlSerializer serializer,
                                MysqlAuthPacket authPacket,
                                MysqlHandshakePacket handshakePacket) throws IOException {
        // 步骤1：根据用户名选择合适的认证器
        // 如果用户名匹配插件认证器（如LDAP），使用插件认证器；否则使用默认认证器
        Authenticator authenticator = chooseAuthenticator(userName);
        
        // 步骤2：使用认证器的密码解析器从认证包中解析密码
        // 不同的认证器可能使用不同的密码格式（如NativePassword、ClearPassword等）
        Optional<Password> password = authenticator.getPasswordResolver()
                .resolvePassword(context, channel, serializer, authPacket, handshakePacket);
        
        // 步骤3：如果密码解析失败，认证失败
        if (!password.isPresent()) {
            return false;
        }
        
        // 步骤4：获取客户端远程IP地址
        String remoteIp = context.getMysqlChannel().getRemoteIp();
        
        // 步骤5：构建认证请求，包含用户名、密码和远程IP
        AuthenticateRequest request = new AuthenticateRequest(userName, password.get(), remoteIp);
        
        // 步骤6：执行实际的认证操作
        // 认证器会验证用户名和密码是否匹配，并返回认证结果
        AuthenticateResponse response = authenticator.authenticate(request);
        
        // 步骤7：如果认证失败，发送错误响应包给客户端并返回false
        if (!response.isSuccess()) {
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        
        // 步骤8：认证成功，设置连接上下文中的用户身份信息
        context.setCurrentUserIdentity(response.getUserIdentity());  // 设置用户身份
        context.setRemoteIP(remoteIp);                              // 设置远程IP
        context.setIsTempUser(response.isTemp());                    // 设置是否为临时用户（如LDAP认证时用户不存在于Doris中）
        
        return true;
    }

    private Authenticator chooseAuthenticator(String userName) {
        return authTypeAuthenticator.canDeal(userName) ? authTypeAuthenticator : defaultAuthenticator;
    }
}
