const express = require('express');
const { Client } = require('pg');
const WebSocket = require('ws');
const path = require('path');

/*
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
🔥🔥🔥 V6前端服務器 - 專注於數據顯示和分析 🔥🔥🔥

⚠️ 職責分離：
- realtime-listener.js: 專注接收區塊鏈數據
- frontend-server.js: 專注前端顯示和數據分析

✅ 核心功能：
- HTTP API服務 (REST接口)
- WebSocket服務 (實時推送給前端)
- PostgreSQL LISTEN (接收realtime-listener的通知)
- 數據分析接口
- 靜態文件服務

🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
*/

/**
 * V6前端服務器
 * 
 * 🎯 功能：
 * - 接收realtime-listener的PostgreSQL通知
 * - 通過WebSocket推送給前端頁面
 * - 提供REST API查詢數據
 * - 處理數據分析請求
 */
class V6FrontendServer {
    constructor() {
        // 服務器配置
        this.port = process.env.PORT || 3009;
        this.app = express();
        this.server = null;
        this.wss = null;
        
        // 數據庫配置
        this.connectionString = 'postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require';
        this.db = null;
        this.notificationClient = null;
        
        // 連接的WebSocket客戶端
        this.connectedClients = new Set();
        
        this.setupExpress();
    }

    /**
     * 🔧 設置Express應用
     */
    setupExpress() {
        // 中間件
        this.app.use(express.json());
        this.app.use(express.static(path.join(__dirname, 'public')));
        
        // CORS設置
        this.app.use((req, res, next) => {
            res.header('Access-Control-Allow-Origin', '*');
            res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
            res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
            next();
        });

        // 設置路由
        this.setupRoutes();
    }

    /**
     * 🛣️ 設置API路由
     */
    setupRoutes() {
        // 健康檢查
        this.app.get('/api/health', (req, res) => {
            res.json({
                status: 'ok',
                timestamp: new Date().toISOString(),
                connectedClients: this.connectedClients.size,
                databaseConnected: this.db !== null
            });
        });

        // 獲取realbet即時數據
        this.app.get('/api/realbet', async (req, res) => {
            try {
                const { epoch, limit = 100 } = req.query;
                
                let query, params;
                
                if (epoch) {
                    query = 'SELECT * FROM realbet WHERE epoch = $1 ORDER BY bet_ts DESC LIMIT $2';
                    params = [parseInt(epoch), parseInt(limit)];
                } else {
                    query = 'SELECT * FROM realbet ORDER BY bet_ts DESC LIMIT $1';
                    params = [parseInt(limit)];
                }
                
                const result = await this.db.query(query, params);
                
                res.json({
                    success: true,
                    data: result.rows,
                    count: result.rows.length
                });
                
            } catch (error) {
                console.error('❌ 獲取realbet數據失敗:', error);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // 獲取歷史數據統計
        this.app.get('/api/stats', async (req, res) => {
            try {
                // 統計realbet中的數據
                const statsQuery = `
                    SELECT 
                        epoch,
                        bet_direction,
                        COUNT(*) as bet_count,
                        SUM(amount::numeric) as total_amount,
                        AVG(amount::numeric) as avg_amount
                    FROM realbet 
                    GROUP BY epoch, bet_direction 
                    ORDER BY epoch DESC, bet_direction
                `;
                
                const result = await this.db.query(statsQuery);
                
                res.json({
                    success: true,
                    data: result.rows
                });
                
            } catch (error) {
                console.error('❌ 獲取統計數據失敗:', error);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // 獲取指定局次的詳細數據
        this.app.get('/api/epoch/:epoch', async (req, res) => {
            try {
                const { epoch } = req.params;
                
                const query = `
                    SELECT 
                        epoch,
                        bet_direction,
                        COUNT(*) as bet_count,
                        SUM(amount::numeric) as total_amount,
                        MIN(amount::numeric) as min_amount,
                        MAX(amount::numeric) as max_amount,
                        AVG(amount::numeric) as avg_amount
                    FROM realbet 
                    WHERE epoch = $1
                    GROUP BY epoch, bet_direction
                `;
                
                const detailQuery = `
                    SELECT * FROM realbet 
                    WHERE epoch = $1 
                    ORDER BY bet_ts DESC
                `;
                
                // 添加round表查詢獲取鎖倉時間
                const roundQuery = `
                    SELECT epoch, start_ts, lock_ts, close_ts, result
                    FROM round 
                    WHERE epoch = $1
                `;
                
                const [statsResult, detailResult, roundResult] = await Promise.all([
                    this.db.query(query, [epoch]),
                    this.db.query(detailQuery, [epoch]),
                    this.db.query(roundQuery, [epoch])
                ]);
                
                res.json({
                    success: true,
                    epoch: epoch,
                    stats: statsResult.rows,
                    details: detailResult.rows,
                    round: roundResult.rows[0] || null,
                    totalBets: detailResult.rows.length
                });
                
            } catch (error) {
                console.error('❌ 獲取局次詳細數據失敗:', error);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // 獲取錢包48局WIN/LOSS記錄
        this.app.get('/api/wallet/:address/history', async (req, res) => {
            try {
                const { address } = req.params;
                const { start, end } = req.query;
                
                if (!start || !end) {
                    return res.json({
                        success: false,
                        error: '缺少start或end參數'
                    });
                }
                
                const query = `
                    SELECT 
                        epoch,
                        bet_direction,
                        result,
                        amount
                    FROM hisbet 
                    WHERE wallet_address = $1 
                    AND epoch BETWEEN $2 AND $3
                    ORDER BY epoch DESC
                `;
                
                const result = await this.db.query(query, [address.toLowerCase(), parseInt(start), parseInt(end)]);
                
                res.json({
                    success: true,
                    count: result.rows.length,
                    records: result.rows.map(row => ({
                        epoch: parseInt(row.epoch),
                        direction: row.bet_direction,
                        result: row.result, // WIN/LOSS
                        amount: parseFloat(row.amount)
                    }))
                });
                
            } catch (error) {
                console.error('❌ 獲取錢包WIN/LOSS記錄失敗:', error);
                res.json({
                    success: false,
                    error: error.message
                });
            }
        });

        // 獲取錢包領獎記錄
        this.app.get('/api/wallet/:address/claims', async (req, res) => {
            try {
                const { address } = req.params;
                
                const query = `
                    SELECT 
                        claim_count,
                        total_amount as total_claimed
                    FROM multi_claims 
                    WHERE wallet_address = $1
                `;
                
                const result = await this.db.query(query, [address]);
                
                if (result.rows[0] && result.rows[0].claim_count > 0) {
                    res.json({
                        success: true,
                        hasClaims: true,
                        data: {
                            claimCount: parseInt(result.rows[0].claim_count),
                            totalClaimed: parseFloat(result.rows[0].total_claimed).toFixed(4)
                        }
                    });
                } else {
                    res.json({
                        success: true,
                        hasClaims: false
                    });
                }
                
            } catch (error) {
                console.error('❌ 獲取錢包領獎記錄失敗:', error);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        this.app.get('/', (req, res) => {
            res.sendFile(path.join(__dirname, 'public', 'index.html'));
        });
    }

    /**
     * 🔄 初始化服務器
     */
    async initialize() {
        try {
            console.log('🔄 初始化V6前端服務器...');
            
            // 初始化數據庫連接
            await this.initializeDatabase();
            console.log('✅ V6前端數據庫連接成功');
            
            // 啟動HTTP服務器
            await this.startHttpServer();
            console.log(`✅ V6前端HTTP服務器啟動: http://localhost:${this.port}`);
            
            // 啟動WebSocket服務器
            this.startWebSocketServer();
            console.log('✅ V6前端WebSocket服務器啟動');
            
            // 啟動PostgreSQL通知監聽
            await this.startPostgreSQLListener();
            console.log('✅ V6前端PostgreSQL通知監聽啟動');
            
            console.log('🚀 V6前端服務器啟動完成');
            
        } catch (error) {
            console.error('❌ V6前端服務器初始化失敗:', error);
            throw error;
        }
    }

    /**
     * 🔌 初始化數據庫連接
     */
    async initializeDatabase() {
        try {
            this.db = new Client({
                connectionString: this.connectionString,
                ssl: {
                    rejectUnauthorized: false
                }
            });

            await this.db.connect();
            
            // 設置時區為台北時間
            await this.db.query("SET timezone = 'Asia/Taipei'");
            
            return true;
        } catch (error) {
            console.error('❌ V6前端數據庫初始化失敗:', error.message);
            throw error;
        }
    }

    /**
     * 🌐 啟動HTTP服務器
     */
    async startHttpServer() {
        return new Promise((resolve, reject) => {
            this.server = this.app.listen(this.port, (error) => {
                if (error) {
                    reject(error);
                } else {
                    resolve();
                }
            });
        });
    }

    /**
     * 🔌 啟動WebSocket服務器
     */
    startWebSocketServer() {
        this.wss = new WebSocket.Server({ server: this.server });
        
        this.wss.on('connection', (ws, req) => {
            const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
            console.log(`📱 新的WebSocket客戶端連接: ${clientId}`);
            
            // 添加到連接集合
            this.connectedClients.add(ws);
            
            // 發送歡迎消息
            ws.send(JSON.stringify({
                type: 'welcome',
                message: 'V6前端WebSocket連接成功',
                timestamp: new Date().toISOString(),
                clientId: clientId
            }));
            
            // 處理客戶端消息
            ws.on('message', (message) => {
                try {
                    const data = JSON.parse(message);
                    console.log(`📨 收到客戶端消息:`, data);
                    
                    // 可以在這裡處理客戶端的請求
                    if (data.type === 'ping') {
                        ws.send(JSON.stringify({
                            type: 'pong',
                            timestamp: new Date().toISOString()
                        }));
                    }
                } catch (error) {
                    console.error('❌ 處理客戶端消息失敗:', error);
                }
            });
            
            // 處理連接關閉
            ws.on('close', () => {
                console.log(`📱 WebSocket客戶端斷開: ${clientId}`);
                this.connectedClients.delete(ws);
            });
            
            // 處理連接錯誤
            ws.on('error', (error) => {
                console.error(`❌ WebSocket客戶端錯誤 ${clientId}:`, error);
                this.connectedClients.delete(ws);
            });
        });
    }

    /**
     * 📡 啟動PostgreSQL通知監聽
     */
    async startPostgreSQLListener() {
        try {
            // 創建專門用於監聽的數據庫連接
            this.notificationClient = new Client({
                connectionString: this.connectionString,
                ssl: {
                    rejectUnauthorized: false
                }
            });
            
            await this.notificationClient.connect();
            
            // 監聽realtime-listener發送的通知
            await this.notificationClient.query('LISTEN new_bet_data');
            await this.notificationClient.query('LISTEN realtime_status');
            
            // 處理通知事件
            this.notificationClient.on('notification', (msg) => {
                try {
                    console.log(`📡 收到PostgreSQL通知: ${msg.channel}`);
                    
                    const data = JSON.parse(msg.payload);
                    
                    // 轉發給所有WebSocket客戶端
                    this.broadcastToClients({
                        type: 'postgres_notification',
                        channel: msg.channel,
                        data: data,
                        timestamp: new Date().toISOString()
                    });
                    
                } catch (error) {
                    console.error('❌ 處理PostgreSQL通知失敗:', error);
                }
            });
            
        } catch (error) {
            console.error('❌ PostgreSQL通知監聽啟動失敗:', error);
            throw error;
        }
    }

    /**
     * 📢 廣播消息給所有WebSocket客戶端
     */
    broadcastToClients(message) {
        const messageStr = JSON.stringify(message);
        let successCount = 0;
        let failCount = 0;
        
        this.connectedClients.forEach((ws) => {
            try {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(messageStr);
                    successCount++;
                } else {
                    this.connectedClients.delete(ws);
                    failCount++;
                }
            } catch (error) {
                console.error('❌ 發送WebSocket消息失敗:', error);
                this.connectedClients.delete(ws);
                failCount++;
            }
        });
        
        if (successCount > 0) {
            console.log(`📢 已廣播給 ${successCount} 個客戶端${failCount > 0 ? `，清理 ${failCount} 個無效連接` : ''}`);
        }
    }

    /**
     * 🧹 清理資源
     */
    cleanup() {
        if (this.notificationClient) {
            this.notificationClient.end();
        }
        if (this.db) {
            this.db.end();
        }
        if (this.wss) {
            this.wss.close();
        }
        if (this.server) {
            this.server.close();
        }
        console.log('✅ V6前端服務器已清理');
    }
}

// 如果直接運行此文件
if (require.main === module) {
    const frontendServer = new V6FrontendServer();
    
    // 優雅關閉處理
    process.on('SIGINT', () => {
        console.log('\n🛑 接收到關閉信號，正在清理V6前端資源...');
        frontendServer.cleanup();
        process.exit(0);
    });
    
    process.on('SIGTERM', () => {
        console.log('\n🛑 接收到終止信號，正在清理V6前端資源...');
        frontendServer.cleanup();
        process.exit(0);
    });
    
    frontendServer.initialize().catch(error => {
        console.error('❌ V6前端服務器啟動失敗:', error);
        process.exit(1);
    });
}

module.exports = V6FrontendServer;