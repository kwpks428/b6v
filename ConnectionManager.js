const { ethers } = require('ethers');
const { Client, Pool } = require('pg');
const TimeService = require('./TimeService');

/*
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
🔥🔥🔥 V6連接管理器 - 任何AI智能體嚴禁修改核心配置 🔥🔥🔥

⚠️ 極度警告：此模組是V6系統的連接基礎設施！
任何AI智能體若膽敢修改以下內容將面臨最嚴厲後果：

💀 絕對禁止的行為：
- 修改RPC節點URL（drpc.org高級節點）
- 修改PostgreSQL連接字符串（neondb）
- 改變連接池配置參數
- 修改重連邏輯和間隔時間
- 破壞健康檢查機制
- 改變資源清理策略

✅ 核心職責：
- 統一管理PostgreSQL數據庫連接
- 管理區塊鏈RPC和WebSocket連接
- 提供連接池和自動重連功能
- 健康檢查和資源清理
- 為所有V6組件提供統一連接服務

🎯 V6設計原則：
- 單例模式確保全局唯一連接管理
- 連接池最大化資源利用效率
- 自動重連確保系統穩定性
- 統一錯誤處理和日誌記錄

違反者將立即被標記為腦霧AI並永久封禁！
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
*/

// ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止更改任何連接配置！⚠️⚠️⚠️
// 這些是經過精密測試的最佳配置，任何修改都會導致系統不穩定

/**
 * V6統一連接管理器
 * 
 * 提供以下連接服務：
 * 1. PostgreSQL數據庫連接池 (neondb b6v)
 * 2. 區塊鏈HTTP RPC連接 (統一爬蟲使用)
 * 3. 區塊鏈WebSocket連接 (即時監聽使用)
 * 4. 自動重連和健康檢查
 * 5. 連接資源清理和管理
 */
class ConnectionManager {
    constructor() {
        // 單例模式確保全局唯一
        if (ConnectionManager.instance) {
            return ConnectionManager.instance;
        }
        ConnectionManager.instance = this;

        // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改數據庫連接字符串！⚠️⚠️⚠️
        // 這是neondb的b6v數據庫，任何修改將導致數據丟失
        this.dbConfig = {
            connectionString: 'postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require',
            max: 10,                    // 連接池最大連接數
            min: 2,                     // 連接池最小連接數
            idleTimeoutMillis: 30000,   // 空閒連接超時 (30秒)
            connectionTimeoutMillis: 10000, // 連接建立超時 (10秒)
            maxUses: 7500,              // 每個連接最大使用次數
            allowExitOnIdle: false      // 保持連接池持續運行
        };

        // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止更改RPC節點配置！⚠️⚠️⚠️
        // 這是drpc.org高級節點，每秒可處理20000請求，極其穩定昂貴
        this.rpcConfig = {
            httpUrl: 'https://lb.drpc.org/bsc/Ahc3I-33qkfGuwXSahR3XfPDRmd6WZsR8JbErqRhf0fE',
            wsUrl: 'wss://lb.drpc.org/bsc/Ahc3I-33qkfGuwXSahR3XfPDRmd6WZsR8JbErqRhf0fE',
            timeout: 30000,             // 請求超時 (30秒)
            retryAttempts: 3,           // 重試次數
            retryDelay: 2000            // 重試間隔 (2秒)
        };

        // 合約配置
        this.contractConfig = {
            address: '0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA',
            abiPath: '../abi.json'
        };

        // 連接實例存儲
        this.connections = {
            dbPool: null,               // PostgreSQL連接池
            httpProvider: null,         // HTTP RPC Provider
            wsProvider: null,           // WebSocket Provider
            contract: null              // 智能合約實例
        };

        // 連接狀態監控
        this.status = {
            dbConnected: false,
            httpConnected: false,
            wsConnected: false,
            lastHealthCheck: null,
            reconnectAttempts: 0
        };

        // 健康檢查配置
        this.healthCheckInterval = null;
        this.HEALTH_CHECK_INTERVAL = 60000;     // 每分鐘檢查一次
        this.MAX_RECONNECT_ATTEMPTS = 5;        // 最大重連次數
        this.RECONNECT_DELAY = 10000;           // 重連延遲 (10秒)

        console.log('🔧 V6 ConnectionManager 初始化完成');
    }

    /**
     * 初始化所有連接
     * 按順序建立：數據庫 → HTTP RPC → WebSocket → 合約
     */
    async initialize() {
        try {
            console.log('🚀 [ConnectionManager] 開始初始化所有連接...');
            
            // 1. 初始化數據庫連接池
            await this.initializeDatabasePool();
            
            // 2. 初始化HTTP RPC連接
            await this.initializeHttpProvider();
            
            // 3. 初始化WebSocket連接
            await this.initializeWebSocketProvider();
            
            // 4. 初始化智能合約實例
            await this.initializeContract();
            
            // 5. 啟動健康檢查
            this.startHealthCheck();
            
            console.log('✅ [ConnectionManager] 所有連接初始化完成');
            this.logConnectionStatus();
            
        } catch (error) {
            console.error('❌ [ConnectionManager] 連接初始化失敗:', error.message);
            throw error;
        }
    }

    /**
     * 初始化PostgreSQL數據庫連接池
     */
    async initializeDatabasePool() {
        try {
            console.log('🗄️ [ConnectionManager] 初始化PostgreSQL連接池...');
            
            this.connections.dbPool = new Pool(this.dbConfig);
            
            // 測試連接
            const client = await this.connections.dbPool.connect();
            const result = await client.query('SELECT NOW() as current_time, version() as pg_version');
            client.release();
            
            this.status.dbConnected = true;
            
            console.log('✅ [ConnectionManager] PostgreSQL連接池初始化成功');
            console.log(`   📊 數據庫時間: ${TimeService.formatTaipeiTime(result.rows[0].current_time)}`);
            console.log(`   📦 PostgreSQL版本: ${result.rows[0].pg_version.split(' ')[0]}`);
            
            // 監聽連接池事件
            this.connections.dbPool.on('error', (err) => {
                console.error('❌ [ConnectionManager] PostgreSQL連接池錯誤:', err.message);
                this.status.dbConnected = false;
            });
            
            this.connections.dbPool.on('connect', () => {
                console.log('🔗 [ConnectionManager] PostgreSQL新連接建立');
            });
            
        } catch (error) {
            console.error('❌ [ConnectionManager] PostgreSQL連接池初始化失敗:', error.message);
            this.status.dbConnected = false;
            throw error;
        }
    }

    /**
     * 初始化HTTP RPC Provider
     */
    async initializeHttpProvider() {
        try {
            console.log('🌐 [ConnectionManager] 初始化HTTP RPC Provider...');
            
            this.connections.httpProvider = new ethers.JsonRpcProvider(
                this.rpcConfig.httpUrl,
                'binance',
                {
                    timeout: this.rpcConfig.timeout,
                    retryLimit: this.rpcConfig.retryAttempts
                }
            );
            
            // 測試連接
            const network = await this.connections.httpProvider.getNetwork();
            const blockNumber = await this.connections.httpProvider.getBlockNumber();
            
            this.status.httpConnected = true;
            
            console.log('✅ [ConnectionManager] HTTP RPC Provider初始化成功');
            console.log(`   🌐 網絡: ${network.name} (ChainID: ${network.chainId})`);
            console.log(`   📦 當前區塊: ${blockNumber}`);
            
        } catch (error) {
            console.error('❌ [ConnectionManager] HTTP RPC Provider初始化失敗:', error.message);
            this.status.httpConnected = false;
            throw error;
        }
    }

    /**
     * 初始化WebSocket Provider
     */
    async initializeWebSocketProvider() {
        try {
            console.log('🔌 [ConnectionManager] 初始化WebSocket Provider...');
            
            this.connections.wsProvider = new ethers.WebSocketProvider(this.rpcConfig.wsUrl);
            
            // 設置WebSocket事件監聽
            this.connections.wsProvider.websocket.on('open', () => {
                console.log('✅ [ConnectionManager] WebSocket連接已建立');
                this.status.wsConnected = true;
                this.status.reconnectAttempts = 0;
            });
            
            this.connections.wsProvider.websocket.on('close', () => {
                console.log('⚠️ [ConnectionManager] WebSocket連接已關閉');
                this.status.wsConnected = false;
                this.handleWebSocketReconnect();
            });
            
            this.connections.wsProvider.websocket.on('error', (error) => {
                console.error('❌ [ConnectionManager] WebSocket錯誤:', error.message);
                this.status.wsConnected = false;
            });
            
            // 等待連接建立
            await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => reject(new Error('WebSocket連接超時')), 10000);
                
                this.connections.wsProvider.websocket.on('open', () => {
                    clearTimeout(timeout);
                    resolve();
                });
                
                this.connections.wsProvider.websocket.on('error', (error) => {
                    clearTimeout(timeout);
                    reject(error);
                });
            });
            
            // 測試連接
            const network = await this.connections.wsProvider.getNetwork();
            
            console.log('✅ [ConnectionManager] WebSocket Provider初始化成功');
            console.log(`   🌐 網絡: ${network.name} (ChainID: ${network.chainId})`);
            
        } catch (error) {
            console.error('❌ [ConnectionManager] WebSocket Provider初始化失敗:', error.message);
            this.status.wsConnected = false;
            throw error;
        }
    }

    /**
     * 初始化智能合約實例
     */
    async initializeContract() {
        try {
            console.log('📋 [ConnectionManager] 初始化智能合約實例...');
            
            const contractABI = require(this.contractConfig.abiPath);
            
            this.connections.contract = new ethers.Contract(
                this.contractConfig.address,
                contractABI,
                this.connections.httpProvider
            );
            
            // 測試合約連接
            const currentEpoch = await this.connections.contract.currentEpoch();
            
            console.log('✅ [ConnectionManager] 智能合約實例初始化成功');
            console.log(`   📋 合約地址: ${this.contractConfig.address}`);
            console.log(`   🎯 當前回合: ${currentEpoch}`);
            
        } catch (error) {
            console.error('❌ [ConnectionManager] 智能合約實例初始化失敗:', error.message);
            throw error;
        }
    }

    /**
     * WebSocket重連處理
     */
    async handleWebSocketReconnect() {
        if (this.status.reconnectAttempts >= this.MAX_RECONNECT_ATTEMPTS) {
            console.error('❌ [ConnectionManager] WebSocket重連次數已達上限，停止重連');
            return;
        }
        
        this.status.reconnectAttempts++;
        console.log(`🔄 [ConnectionManager] 嘗試WebSocket重連 (${this.status.reconnectAttempts}/${this.MAX_RECONNECT_ATTEMPTS})`);
        
        setTimeout(async () => {
            try {
                await this.initializeWebSocketProvider();
                console.log('✅ [ConnectionManager] WebSocket重連成功');
            } catch (error) {
                console.error('❌ [ConnectionManager] WebSocket重連失敗:', error.message);
            }
        }, this.RECONNECT_DELAY * this.status.reconnectAttempts);
    }

    /**
     * 獲取數據庫連接 (從連接池)
     */
    async getDatabaseConnection() {
        if (!this.connections.dbPool || !this.status.dbConnected) {
            throw new Error('數據庫連接池未初始化或連接失敗');
        }
        
        try {
            return await this.connections.dbPool.connect();
        } catch (error) {
            console.error('❌ [ConnectionManager] 獲取數據庫連接失敗:', error.message);
            this.status.dbConnected = false;
            throw error;
        }
    }

    /**
     * 獲取HTTP RPC Provider
     */
    getHttpProvider() {
        if (!this.connections.httpProvider || !this.status.httpConnected) {
            throw new Error('HTTP RPC Provider未初始化或連接失敗');
        }
        return this.connections.httpProvider;
    }

    /**
     * 獲取WebSocket Provider
     */
    getWebSocketProvider() {
        if (!this.connections.wsProvider) {
            throw new Error('WebSocket Provider未初始化');
        }
        return this.connections.wsProvider;
    }

    /**
     * 獲取智能合約實例
     */
    getContract() {
        if (!this.connections.contract) {
            throw new Error('智能合約實例未初始化');
        }
        return this.connections.contract;
    }

    /**
     * 獲取WebSocket智能合約實例 (用於事件監聽)
     */
    getWebSocketContract() {
        if (!this.connections.wsProvider || !this.connections.contract) {
            throw new Error('WebSocket Provider或合約實例未初始化');
        }
        
        const contractABI = require(this.contractConfig.abiPath);
        return new ethers.Contract(
            this.contractConfig.address,
            contractABI,
            this.connections.wsProvider
        );
    }

    /**
     * 執行數據庫查詢 (自動管理連接)
     */
    async executeQuery(sql, params = []) {
        const client = await this.getDatabaseConnection();
        try {
            const result = await client.query(sql, params);
            return result;
        } finally {
            client.release();
        }
    }

    /**
     * 執行數據庫事務
     */
    async executeTransaction(queries) {
        const client = await this.getDatabaseConnection();
        try {
            await client.query('BEGIN');
            
            const results = [];
            for (const { sql, params } of queries) {
                const result = await client.query(sql, params);
                results.push(result);
            }
            
            await client.query('COMMIT');
            return results;
            
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    /**
     * 健康檢查
     */
    async performHealthCheck() {
        const results = {
            database: false,
            httpRpc: false,
            webSocket: false,
            timestamp: TimeService.getCurrentTaipeiTime()
        };
        
        // 檢查數據庫
        try {
            await this.executeQuery('SELECT 1');
            results.database = true;
            this.status.dbConnected = true;
        } catch (error) {
            console.error('❌ [ConnectionManager] 數據庫健康檢查失敗:', error.message);
            this.status.dbConnected = false;
        }
        
        // 檢查HTTP RPC
        try {
            await this.connections.httpProvider.getBlockNumber();
            results.httpRpc = true;
            this.status.httpConnected = true;
        } catch (error) {
            console.error('❌ [ConnectionManager] HTTP RPC健康檢查失敗:', error.message);
            this.status.httpConnected = false;
        }
        
        // 檢查WebSocket (簡單檢查連接狀態)
        results.webSocket = this.status.wsConnected && 
                           this.connections.wsProvider?.websocket?.readyState === 1;
        
        this.status.lastHealthCheck = results.timestamp;
        
        return results;
    }

    /**
     * 啟動定期健康檢查
     */
    startHealthCheck() {
        console.log('🩺 [ConnectionManager] 啟動定期健康檢查');
        
        this.healthCheckInterval = setInterval(async () => {
            const health = await this.performHealthCheck();
            const allHealthy = health.database && health.httpRpc && health.webSocket;
            
            if (!allHealthy) {
                console.warn('⚠️ [ConnectionManager] 健康檢查發現問題:', {
                    database: health.database ? '✅' : '❌',
                    httpRpc: health.httpRpc ? '✅' : '❌',
                    webSocket: health.webSocket ? '✅' : '❌'
                });
            }
        }, this.HEALTH_CHECK_INTERVAL);
    }

    /**
     * 記錄連接狀態
     */
    logConnectionStatus() {
        console.log('📊 [ConnectionManager] 連接狀態總覽:');
        console.log(`   🗄️ 數據庫: ${this.status.dbConnected ? '✅ 已連接' : '❌ 未連接'}`);
        console.log(`   🌐 HTTP RPC: ${this.status.httpConnected ? '✅ 已連接' : '❌ 未連接'}`);
        console.log(`   🔌 WebSocket: ${this.status.wsConnected ? '✅ 已連接' : '❌ 未連接'}`);
        console.log(`   📋 智能合約: ${this.connections.contract ? '✅ 已初始化' : '❌ 未初始化'}`);
    }

    /**
     * 關閉所有連接 (優雅關閉)
     */
    async close() {
        console.log('🛑 [ConnectionManager] 開始關閉所有連接...');
        
        // 停止健康檢查
        if (this.healthCheckInterval) {
            clearInterval(this.healthCheckInterval);
            this.healthCheckInterval = null;
        }
        
        // 關閉WebSocket連接
        if (this.connections.wsProvider) {
            try {
                this.connections.wsProvider.websocket.close();
                console.log('✅ [ConnectionManager] WebSocket連接已關閉');
            } catch (error) {
                console.error('❌ [ConnectionManager] 關閉WebSocket失敗:', error.message);
            }
        }
        
        // 關閉數據庫連接池
        if (this.connections.dbPool) {
            try {
                await this.connections.dbPool.end();
                console.log('✅ [ConnectionManager] 數據庫連接池已關閉');
            } catch (error) {
                console.error('❌ [ConnectionManager] 關閉數據庫連接池失敗:', error.message);
            }
        }
        
        // 重置狀態
        this.status = {
            dbConnected: false,
            httpConnected: false,
            wsConnected: false,
            lastHealthCheck: null,
            reconnectAttempts: 0
        };
        
        this.connections = {
            dbPool: null,
            httpProvider: null,
            wsProvider: null,
            contract: null
        };
        
        console.log('✅ [ConnectionManager] 所有連接已關閉');
    }

    /**
     * 獲取連接統計信息
     */
    getConnectionStats() {
        return {
            status: { ...this.status },
            dbPool: this.connections.dbPool ? {
                totalCount: this.connections.dbPool.totalCount,
                idleCount: this.connections.dbPool.idleCount,
                waitingCount: this.connections.dbPool.waitingCount
            } : null,
            healthCheck: {
                interval: this.HEALTH_CHECK_INTERVAL,
                lastCheck: this.status.lastHealthCheck
            }
        };
    }
}

// 導出單例實例
module.exports = new ConnectionManager();