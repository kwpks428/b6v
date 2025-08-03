const { ethers } = require('ethers');
const { Client } = require('pg');
const WebSocket = require('ws');
const http = require('http');
const TimeService = require('./TimeService');

/*
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
🔥🔥🔥 V6即時數據監聽器 - 任何AI智能體嚴禁修改核心架構 🔥🔥🔥

⚠️ 極度警告：此模組是V6系統的核心數據入口！
任何AI智能體若膽敢修改以下內容將面臨最嚴厲後果：

💀 絕對禁止的行為：
- 修改UP/DOWN標準為bull/bear等腦霧變體
- 修改realbet表結構或欄位名稱
- 改變TimeService的使用方式
- 修改PostgreSQL NOTIFY機制
- 改變區塊鏈RPC節點設定
- 刪除或修改可疑錢包檢測邏輯

✅ 核心職責：
- 監聽區塊鏈WebSocket事件
- 將即時下注數據存入realbet表
- 透過PostgreSQL NOTIFY推送給前端
- 檢測可疑錢包行為並自動標記

🎯 V6新特性：
- 使用統一的TimeService處理時間
- 支持新的realbet表結構（無result欄位）
- 強制UP/DOWN方向標準化
- 數值使用NUMERIC格式避免誤差

違反者將立即被標記為腦霧AI並永久封禁！
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
*/

// 監控常量配置 - V6標準化版本
const MONITORING_CONSTANTS = {
    MAX_BETS_PER_ROUND: 50,        // 單局最大下注次數
    MAX_BET_AMOUNT: 10,            // 單次最大下注金額 (BNB)
    SUSPICIOUS_BET_COUNT: 100,     // 可疑下注總次數
    HIGH_FREQUENCY_WINDOW: 60000,  // 高頻檢測窗口 (1分鐘)
    MAX_BETS_IN_WINDOW: 10,        // 窗口內最大下注次數
    CLEANUP_INTERVAL: 3600000,     // 清理間隔 (1小時)
    CONNECTION_TIMEOUT: 10000,     // 連接超時 (10秒)
    RECONNECT_DELAY: 10000         // 重連延遲 (10秒)
};

/**
 * V6可疑錢包監控系統
 * 
 * 🎯 功能：
 * - 實時檢測異常下注行為
 * - 自動標記可疑錢包
 * - 支持多維度風險評估
 */
class SuspiciousWalletMonitor {
    constructor() {
        this.suspiciousWallets = new Set();
        this.walletBetCounts = new Map();
        this.walletBetAmounts = new Map();
        this.roundBetCounts = new Map();
        this.recentBets = new Map();
        
        // V6標準化閾值配置
        this.thresholds = {
            maxBetsPerRound: MONITORING_CONSTANTS.MAX_BETS_PER_ROUND,
            maxBetAmount: MONITORING_CONSTANTS.MAX_BET_AMOUNT,
            suspiciousBetCount: MONITORING_CONSTANTS.SUSPICIOUS_BET_COUNT,
            highFrequencyWindow: MONITORING_CONSTANTS.HIGH_FREQUENCY_WINDOW,
            maxBetsInWindow: MONITORING_CONSTANTS.MAX_BETS_IN_WINDOW
        };
    }
    
    /**
     * 🔍 檢查錢包是否可疑
     * 
     * @param {string} wallet - 錢包地址
     * @param {string} amount - 下注金額字符串
     * @param {string} epoch - 局次編號
     * @returns {Object} 檢測結果
     */
    checkSuspiciousWallet(wallet, amount, epoch) {
        const amountBNB = parseFloat(amount);
        const now = Date.now();
        let flags = [];
        
        // 1. 檢查單次下注金額
        if (amountBNB > this.thresholds.maxBetAmount) {
            flags.push(`大額下注: ${amountBNB} BNB`);
        }
        
        // 2. 更新並檢查錢包總下注次數
        const currentCount = this.walletBetCounts.get(wallet) || 0;
        this.walletBetCounts.set(wallet, currentCount + 1);
        
        if (currentCount + 1 > this.thresholds.suspiciousBetCount) {
            flags.push(`高頻用戶: ${currentCount + 1} 次下注`);
        }
        
        // 3. 檢查高頻下注（時間窗口內）
        if (!this.recentBets.has(wallet)) {
            this.recentBets.set(wallet, []);
        }
        
        const walletRecentBets = this.recentBets.get(wallet);
        const validBets = walletRecentBets.filter(time => now - time < this.thresholds.highFrequencyWindow);
        validBets.push(now);
        this.recentBets.set(wallet, validBets);
        
        if (validBets.length > this.thresholds.maxBetsInWindow) {
            flags.push(`高頻下注: ${validBets.length} 次/分鐘`);
        }
        
        // 4. 檢查單局下注次數
        const roundKey = `${wallet}_${epoch}`;
        const roundCount = this.roundBetCounts.get(roundKey) || 0;
        this.roundBetCounts.set(roundKey, roundCount + 1);
        
        if (roundCount + 1 > 1) {
            flags.push(`重複下注: 局次${epoch}第${roundCount + 1}次`);
        }
        
        // 5. 更新錢包總金額
        const currentAmount = this.walletBetAmounts.get(wallet) || 0;
        this.walletBetAmounts.set(wallet, currentAmount + amountBNB);
        
        // 判定結果
        if (flags.length > 0) {
            this.suspiciousWallets.add(wallet);
            return {
                isSuspicious: true,
                flags: flags,
                totalBets: currentCount + 1,
                totalAmount: currentAmount + amountBNB
            };
        }
        
        return {
            isSuspicious: false,
            flags: [],
            totalBets: currentCount + 1,
            totalAmount: currentAmount + amountBNB
        };
    }
    
    /**
     * 獲取可疑錢包列表
     */
    getSuspiciousWallets() {
        return Array.from(this.suspiciousWallets);
    }
    
    /**
     * 🧹 清理過期數據
     */
    cleanup() {
        const now = Date.now();
        const oneHourAgo = now - 3600000;
        
        for (const [wallet, times] of this.recentBets.entries()) {
            const validTimes = times.filter(time => time > oneHourAgo);
            if (validTimes.length === 0) {
                this.recentBets.delete(wallet);
            } else {
                this.recentBets.set(wallet, validTimes);
            }
        }
        
        console.log('🧹 V6可疑錢包監控數據清理完成');
    }
}

/**
 * V6即時數據監聽器主類
 * 
 * 🎯 核心功能：
 * - 監聽區塊鏈WebSocket事件
 * - 存儲即時下注到realbet表
 * - PostgreSQL NOTIFY推送給前端
 * - 可疑行為實時檢測
 */
class V6RealtimeListener {
    constructor() {
        // 數據庫連接配置 - Railway環境變數支持
        this.connectionString = process.env.V6_DATABASE_URL || process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require';
        
        // Railway deployment check
        const IS_RAILWAY = process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY_PROJECT_ID;
        if (IS_RAILWAY) {
            console.log('🚀 V6即時監聽器運行在Railway平台');
            console.log('📊 Project:', process.env.RAILWAY_PROJECT_NAME || 'Unknown');
            console.log('🌍 Environment:', process.env.RAILWAY_ENVIRONMENT || 'production');
        }
        
        this.db = null;
        this.suspiciousMonitor = new SuspiciousWalletMonitor();
        
        // 🎯 重複檢查：內存中維護已處理的 epoch+wallet 組合
        this.processedBets = new Map(); // 格式: "epoch_walletAddress" => timestamp
        
        // 區塊鏈連接相關
        this.provider = null;
        this.contract = null;
        this.currentRound = null;
        this.currentLockTimestamp = null;
        this.isConnected = false;
        this.reconnectTimer = null;
        
        // 🚀 新增：直接 WebSocket 服務器（替代 PostgreSQL NOTIFY）
        this.wsPort = 3010;
        this.wsServer = null;
        this.wss = null;
        this.connectedClients = new Set();
        
        this.setupCleanupTimer();
    }

    /**
     * 🔄 初始化系統
     */
    async initialize() {
        try {
            console.log('🔄 初始化V6即時數據監聽器...');
            
            // 初始化數據庫連接
            await this.initializeDatabase();
            console.log('✅ V6數據庫連接成功');
            
            // 🚀 新增：初始化直接 WebSocket 服務器
            await this.initializeWebSocketServer();
            console.log('✅ V6直接WebSocket服務器啟動成功');
            
            // 從realbet恢復內存狀態（防止重啟後重複處理）
            await this.restoreFromRealbet();
            
            // 啟動區塊鏈監聽器
            await this.startBlockchainListener();
            
            console.log('🚀 V6即時數據監聽器啟動完成');
            
        } catch (error) {
            console.error('❌ V6即時數據監聽器初始化失敗:', error);
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
            
            // 驗證連接和時區
            const timeResult = await this.db.query('SELECT NOW() as current_time, current_setting(\'timezone\') as timezone');
            console.log(`📅 V6數據庫時區: ${timeResult.rows[0].timezone}`);
            console.log(`🕐 V6數據庫當前時間: ${TimeService.formatTaipeiTime(timeResult.rows[0].current_time)}`);
            
            return true;
        } catch (error) {
            console.error('❌ V6數據庫初始化失敗:', error.message);
            throw error;
        }
    }

    /**
     * 🚀 初始化直接 WebSocket 服務器（替代 PostgreSQL NOTIFY）
     */
    async initializeWebSocketServer() {
        try {
            // 創建 HTTP 服務器
            this.wsServer = http.createServer();
            
            // 創建 WebSocket 服務器
            this.wss = new WebSocket.Server({ 
                server: this.wsServer,
                path: '/ws'
            });
            
            // 設置 WebSocket 連接處理
            this.wss.on('connection', (ws, req) => {
                console.log(`🔗 新的前端連接: ${req.socket.remoteAddress}`);
                
                // 添加到連接集合
                this.connectedClients.add(ws);
                
                // 發送歡迎消息
                ws.send(JSON.stringify({
                    type: 'welcome',
                    message: 'V6直接WebSocket連接成功',
                    timestamp: new Date().toISOString(),
                    clientCount: this.connectedClients.size
                }));
                
                // 處理客戶端消息
                ws.on('message', (message) => {
                    try {
                        const data = JSON.parse(message);
                        console.log('📨 收到前端消息:', data);
                        
                        // 可以在這裡處理前端發送的消息
                        if (data.type === 'ping') {
                            ws.send(JSON.stringify({
                                type: 'pong',
                                timestamp: new Date().toISOString()
                            }));
                        }
                    } catch (error) {
                        console.error('❌ 處理前端消息失敗:', error);
                    }
                });
                
                // 處理連接關閉
                ws.on('close', () => {
                    console.log('❌ 前端連接關閉');
                    this.connectedClients.delete(ws);
                    console.log(`📊 剩餘連接數: ${this.connectedClients.size}`);
                });
                
                // 處理連接錯誤
                ws.on('error', (error) => {
                    console.error('❌ WebSocket連接錯誤:', error);
                    this.connectedClients.delete(ws);
                });
            });
            
            // 啟動服務器
            return new Promise((resolve, reject) => {
                this.wsServer.listen(this.wsPort, (error) => {
                    if (error) {
                        reject(error);
                    } else {
                        console.log(`🚀 V6直接WebSocket服務器啟動在端口 ${this.wsPort}`);
                        console.log(`📡 WebSocket連接地址: ws://localhost:${this.wsPort}/ws`);
                        resolve();
                    }
                });
            });
            
        } catch (error) {
            console.error('❌ WebSocket服務器初始化失敗:', error);
            throw error;
        }
    }

    /**
     * 📡 直接廣播消息給所有連接的前端客戶端
     */
    broadcastToClients(message) {
        if (this.connectedClients.size === 0) {
            console.log('⚠️ 沒有連接的前端客戶端');
            return;
        }
        
        const messageStr = JSON.stringify(message);
        let successCount = 0;
        let failCount = 0;
        
        this.connectedClients.forEach((ws) => {
            try {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(messageStr);
                    successCount++;
                } else {
                    // 移除已關閉的連接
                    this.connectedClients.delete(ws);
                    failCount++;
                }
            } catch (error) {
                console.error('❌ 廣播消息失敗:', error);
                this.connectedClients.delete(ws);
                failCount++;
            }
        });
        
        console.log(`📡 廣播完成 - 成功:${successCount}, 失敗:${failCount}, 總連接:${this.connectedClients.size}`);
    }

    /**
     * 🔄 從realbet恢復內存狀態（防止重啟後重複處理）
     */
    async restoreFromRealbet() {
        try {
            // 只恢復最近3局的數據到內存，使用當前時間作為時間戳
            const query = `
                SELECT DISTINCT epoch, wallet_address 
                FROM realbet 
                ORDER BY epoch DESC 
                LIMIT 1000
            `;
            
            const result = await this.db.query(query);
            let restoredCount = 0;
            const now = Date.now();
            
            for (const row of result.rows) {
                const betKey = `${row.epoch}_${row.wallet_address.toLowerCase()}`;
                this.processedBets.set(betKey, now);
                restoredCount++;
            }
            
            console.log(`🔄 已從realbet恢復 ${restoredCount} 個已處理記錄到內存`);
            
        } catch (error) {
            console.error('❌ 從realbet恢復內存狀態失敗:', error);
            // 恢復失敗不影響啟動
        }
    }

    /**
     * 🔗 啟動區塊鏈監聽器
     */
    async startBlockchainListener() {
        if (this.isConnected) {
            console.log('⚠️ 區塊鏈監聽器已在運行');
            return;
        }

        try {
            console.log('🔗 開始連接區塊鏈...');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止更改此RPC節點！⚠️⚠️⚠️
            // 這是高級drpc.org節點，每秒可處理20000請求，極其穩定昂貴
            // 如有連接問題，請檢查監聽邏輯，絕對不准修改節點URL
            // 🔥🔥🔥 任何人擅自修改此節點URL將承擔嚴重後果！🔥🔥🔥
            // 🚨🚨🚨 此警告不得刪除、修改或忽視！🚨🚨🚨
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止更改此RPC節點！⚠️⚠️⛔
            this.provider = new ethers.WebSocketProvider('wss://lb.drpc.org/bsc/Ahc3I-33qkfGuwXSahR3XfPDRmd6WZsR8JbErqRhf0fE');
            this.contract = new ethers.Contract('0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA', require('./abi.json'), this.provider);
            
            // 設置連接事件監聽
            this.setupConnectionEvents();
            
            // 等待連接建立
            await this.waitForConnection();
            
            // 獲取當前局次信息
            await this.loadCurrentRoundInfo();
            
            // 設置區塊鏈事件監聽器
            this.setupBlockchainEvents();
            
            // 通知前端連接狀態
            await this.notifyConnectionStatus(true);
            
        } catch (error) {
            console.error('❌ 區塊鏈監聽器啟動失敗:', error);
            this.isConnected = false;
            this.scheduleReconnect();
        }
    }

    /**
     * 🔌 設置WebSocket連接事件
     */
    setupConnectionEvents() {
        this.provider.websocket.on('open', () => {
            console.log('✅ 區塊鏈WebSocket連接成功');
            this.isConnected = true;
        });
        
        this.provider.websocket.on('close', (code, reason) => {
            console.log(`❌ 區塊鏈WebSocket連接關閉: ${code} - ${reason}`);
            this.isConnected = false;
            this.notifyConnectionStatus(false);
            this.scheduleReconnect();
        });
        
        this.provider.websocket.on('error', (error) => {
            console.error('❌ 區塊鏈WebSocket錯誤:', error);
            this.isConnected = false;
            this.notifyConnectionStatus(false);
            this.scheduleReconnect();
        });
    }

    /**
     * ⏳ 等待WebSocket連接建立
     */
    async waitForConnection() {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('連接超時'));
            }, MONITORING_CONSTANTS.CONNECTION_TIMEOUT);
            
            this.provider.websocket.on('open', () => {
                clearTimeout(timeout);
                resolve();
            });
            
            this.provider.websocket.on('error', (error) => {
                clearTimeout(timeout);
                reject(error);
            });
        });
    }

    /**
     * 🔄 更新當前局次完整信息（實時訂閱版本）
     * 
     * @param {string} epoch - 目標局次
     */
    async updateCurrentRoundInfo(epoch) {
        try {
            console.log(`🔄 更新局次信息: ${epoch}`);
            
            // 獲取局次完整數據
            const roundData = await this.contract.rounds(epoch);
            
            // 更新當前局次狀態
            this.currentRound = epoch;
            this.currentLockTimestamp = Number(roundData.lockTimestamp);
            
            // 準備發送給前端的完整局次信息
            const roundInfo = {
                type: 'round_update',
                epoch: epoch,
                startTimestamp: Number(roundData.startTimestamp),
                lockTimestamp: Number(roundData.lockTimestamp),
                closeTimestamp: Number(roundData.closeTimestamp),
                lockPrice: ethers.formatUnits(roundData.lockPrice, 8),
                closePrice: ethers.formatUnits(roundData.closePrice, 8),
                totalAmount: ethers.formatEther(roundData.totalAmount),
                bullAmount: ethers.formatEther(roundData.bullAmount),
                bearAmount: ethers.formatEther(roundData.bearAmount),
                rewardBaseCalAmount: ethers.formatEther(roundData.rewardBaseCalAmount),
                rewardAmount: ethers.formatEther(roundData.rewardAmount),
                oracleCalled: roundData.oracleCalled,
                timestamp: TimeService.getCurrentTaipeiTime()
            };
            
            // 判斷局次狀態
            let status = 'unknown';
            if (roundData.startTimestamp > 0 && roundData.lockTimestamp === 0n) {
                status = 'betting'; // 可下注階段
            } else if (roundData.lockTimestamp > 0 && roundData.closeTimestamp === 0n) {
                status = 'locked'; // 已鎖倉，等待結算
            } else if (roundData.closeTimestamp > 0) {
                status = 'ended'; // 已結束
            } else {
                status = 'pending'; // 尚未開始
            }
            
            roundInfo.status = status;
            
            console.log(`📊 局次 ${epoch} 狀態: ${status}, 鎖倉時間: ${this.currentLockTimestamp}`);
            
            // 廣播給前端
            console.log('📤 發送完整round_update數據:', roundInfo);
            this.broadcastToClients(roundInfo);
            console.log('✅ round_update已廣播，客戶端數量:', this.connectedClients.size);
            
        } catch (error) {
            console.error(`❌ 更新局次 ${epoch} 信息失敗:`, error);
        }
    }

    /**
     * 📊 載入當前局次信息（初始化版本）
     */
    async loadCurrentRoundInfo() {
        try {
            const currentEpoch = await this.contract.currentEpoch();
            
            // 檢查當前局次是否可以下注（鎖倉時間為0表示還未鎖倉）
            const currentRound = await this.contract.rounds(currentEpoch);
            const currentLockTimestamp = Number(currentRound.lockTimestamp);
            
            if (currentLockTimestamp === 0) {
                // 當前局次還可以下注
                console.log(`📍 當前可下注局次: ${currentEpoch.toString()}`);
                await this.updateCurrentRoundInfo(currentEpoch.toString());
            } else {
                // 當前局次已鎖倉，下注目標是下一局
                const nextEpoch = currentEpoch + 1n;
                console.log(`📍 當前運行局次: ${currentEpoch.toString()} (已鎖倉)`);
                console.log(`📍 下注目標局次: ${nextEpoch.toString()}`);
                await this.updateCurrentRoundInfo(nextEpoch.toString());
            }
            
        } catch (error) {
            console.error('❌ 載入當前局次信息失敗:', error);
        }
    }

    /**
     * 🎧 設置區塊鏈事件監聽器
     */
    setupBlockchainEvents() {
        if (!this.contract) return;
        
        // 清除現有監聽器
        this.contract.removeAllListeners();
        
        // 監聽UP下注事件
        this.contract.on('BetBull', async (...args) => {
            const event = args[args.length - 1];
            const [sender, epoch, amount] = args;
            
            console.log(`📈 UP下注事件 - 局次:${epoch}, 金額:${ethers.formatEther(amount)}, 錢包:${sender}`);
            console.log(`📈 交易詳情 - TxHash:${event.transactionHash}, Block:${event.blockNumber}`);
            
            await this.handleBetEvent(sender, epoch, amount, event, 'UP', '📈');
        });
        
        // 監聽DOWN下注事件
        this.contract.on('BetBear', async (...args) => {
            const event = args[args.length - 1];
            const [sender, epoch, amount] = args;
            
            console.log(`📉 DOWN下注事件 - 局次:${epoch}, 金額:${ethers.formatEther(amount)}, 錢包:${sender}`);
            console.log(`📉 交易詳情 - TxHash:${event.transactionHash}, Block:${event.blockNumber}`);
            
            await this.handleBetEvent(sender, epoch, amount, event, 'DOWN', '📉');
        });
        
        // 監聽新局開始事件
        this.contract.on('StartRound', async (epoch) => {
            console.log('🚀 新局開始:', epoch.toString());
            console.log(`   前一局: ${this.currentRound} → 新局: ${epoch.toString()}`);
            
            // 獲取新局的完整信息
            await this.updateCurrentRoundInfo(epoch.toString());
            
            // 新局開始時，自動清理舊的realbet數據（保留最近3局）
            await this.cleanupOldRealbet();
        });
        
        // 監聽局次鎖倉事件
        this.contract.on('LockRound', async (epoch) => {
            console.log('🔒 局次鎖倉:', epoch.toString());
            
            // 當前局次鎖倉時，用戶需要下注到下一局
            if (epoch.toString() === this.currentRound) {
                const nextEpoch = epoch + 1n;
                await this.updateCurrentRoundInfo(nextEpoch.toString());
            }
            
            // 局次鎖倉後清空該局的內存記錄（該局已結束下注）
            await this.clearMemoryForEpoch(epoch.toString());
            
            await this.notifyRoundLock(epoch.toString());
        });
        
        console.log('✅ V6區塊鏈事件監聽器設置完成');
    }

    /**
     * 🎯 處理下注事件的核心邏輯
     * 數據流順序：數據進來 → 檢查重複 → 前端顯示 → 數據庫記錄
     */
    async handleBetEvent(sender, epoch, amount, event, direction, emoji) {
        try {
            // 🔥 V6強制標準化：確保direction必須是UP或DOWN
            if (!['UP', 'DOWN'].includes(direction)) {
                throw new Error(`Invalid direction: ${direction}. Must be UP or DOWN`);
            }
            
            // 🎯 步驟1: 檢查重複 - 使用內存快速檢查
            const betKey = `${epoch.toString()}_${sender.toLowerCase()}`;
            if (this.processedBets.has(betKey)) {
                console.log(`⚠️ 重複下注已忽略: ${sender} 局次${epoch}`);
                return; // 直接返回，不處理重複數據
            }
            
            // 標記為已處理（記錄時間戳用於清理）
            this.processedBets.set(betKey, Date.now());
            
            // 創建下注數據 - V6標準格式
            const betData = this.createBetData(sender, epoch, amount, event, direction);
            
            console.log(`${emoji} ${direction}下注 局次${epoch}:`, betData.amount, 'BNB');
            
            // 可疑錢包檢查
            const suspiciousCheck = this.suspiciousMonitor.checkSuspiciousWallet(
                sender, 
                betData.amount, 
                epoch.toString()
            );
            
            // 處理可疑活動
            if (suspiciousCheck.isSuspicious) {
                await this.handleSuspiciousActivity(sender, suspiciousCheck, epoch, direction, betData.amount);
            }
            
            // 🎯 步驟2: 立即通知前端顯示（優先級最高）
            await this.notifyFrontendImmediately(betData, suspiciousCheck);
            
            // 🎯 步驟3: 同步保存到realbet暫存表（生命週期僅3局）
            await this.saveBetToDatabase(betData, suspiciousCheck);
            
        } catch (error) {
            console.error('❌ 處理下注事件失敗:', error);
        }
    }

    /**
     * 📝 創建V6標準格式的下注數據
     */
    createBetData(sender, epoch, amount, event, direction) {
        // 🔥 V6強制標準化：確保direction必須是UP或DOWN
        if (!['UP', 'DOWN'].includes(direction)) {
            throw new Error(`Invalid direction: ${direction}. Must be UP or DOWN`);
        }
        
        // 🎯 V6核心改進：使用TimeService統一時間處理
        const betData = {
            epoch: epoch.toString(),
            bet_ts: TimeService.getCurrentTaipeiTime(),  // V6: 統一台北時間格式
            wallet_address: sender,
            bet_direction: direction,  // V6: 強制UP/DOWN
            amount: ethers.formatEther(amount)  // V6: 數值格式避免誤差
        };
        
        console.log(`✅ V6下注數據創建完成 - ${betData.wallet_address} 局次${betData.epoch}`);
        return betData;
    }

    /**
     * 🚨 處理可疑錢包活動
     */
    async handleSuspiciousActivity(sender, suspiciousCheck, epoch, direction, amount) {
        console.log(`🚨 檢測到可疑錢包活動!`);
        console.log(`   錢包地址: ${sender}`);
        console.log(`   可疑標記: ${suspiciousCheck.flags.join(', ')}`);
        console.log(`   總下注次數: ${suspiciousCheck.totalBets}`);
        console.log(`   總下注金額: ${suspiciousCheck.totalAmount.toFixed(4)} BNB`);
        
        try {
            // 檢查錢包是否已有備註
            const existingNote = await this.getWalletNote(sender);
            if (!existingNote) {
                // 自動標記可疑錢包
                const suspiciousNote = `🚨 V6自動檢測可疑活動: ${suspiciousCheck.flags.join(', ')} | 檢測時間: ${TimeService.getCurrentTaipeiTime()}`;
                await this.updateWalletNote(sender, suspiciousNote);
                console.log(`✅ 已自動標記可疑錢包: ${sender}`);
            }
        } catch (error) {
            console.error('❌ 標記可疑錢包失敗:', error);
        }
        
        // 通知前端可疑活動
        await this.notifySuspiciousActivity({
            wallet: sender,
            epoch: epoch.toString(),
            direction,
            amount: amount,
            flags: suspiciousCheck.flags,
            totalBets: suspiciousCheck.totalBets,
            totalAmount: suspiciousCheck.totalAmount,
            timestamp: TimeService.getCurrentTaipeiTime()
        });
    }

    /**
     * 📡 立即通知前端顯示（優先級最高）- 使用直接 WebSocket 廣播
     */
    async notifyFrontendImmediately(betData, suspiciousCheck) {
        try {
            const notificationData = {
                type: 'new_bet',
                wallet: betData.wallet_address,
                epoch: betData.epoch,
                direction: betData.bet_direction,
                amount: betData.amount,
                timestamp: betData.bet_ts,
                suspicious: suspiciousCheck.isSuspicious,
                suspiciousFlags: suspiciousCheck.isSuspicious ? suspiciousCheck.flags : undefined
            };
            
            // 🚀 新架構：直接 WebSocket 廣播（替代 PostgreSQL NOTIFY）
            this.broadcastToClients({
                channel: 'new_bet_data',
                data: notificationData
            });
            
            console.log(`🚀 已直接廣播給前端: ${betData.wallet_address} 局次${betData.epoch}`);
            
        } catch (error) {
            console.error('❌ 直接廣播給前端失敗:', error);
        }
    }

    /**
     * 💾 保存到realbet「持久化內存」表（避免重啟丟失數據）
     */
    async saveBetToDatabase(betData, suspiciousCheck) {
        try {
            // realbet = 持久化內存，防止重啟丟失數據
            // 生命週期3局，歷史爬蟲抓取完成後清理
            const query = `
                INSERT INTO realbet (
                    epoch, bet_ts, wallet_address, bet_direction, amount
                ) VALUES ($1, $2, $3, $4, $5)
            `;
            
            const values = [
                betData.epoch,
                betData.bet_ts,
                betData.wallet_address,
                betData.bet_direction,
                betData.amount
            ];
            
            await this.db.query(query, values);
            console.log(`💾 已存入持久化內存(realbet): ${betData.wallet_address} 局次${betData.epoch}`);
            
        } catch (error) {
            console.error('❌ 存入持久化內存失敗:', error);
            throw error;
        }
    }

    /**
     * 📋 獲取錢包備註
     */
    async getWalletNote(wallet) {
        try {
            const query = 'SELECT note FROM wallet_note WHERE wallet_address = $1';
            const result = await this.db.query(query, [wallet]);
            return result.rows[0]?.note || '';
        } catch (error) {
            console.error('❌ 獲取錢包備註失敗:', error);
            return '';
        }
    }

    /**
     * 📝 更新錢包備註
     */
    async updateWalletNote(wallet, note) {
        try {
            const query = `
                INSERT INTO wallet_note (wallet_address, note, updated_ts)
                VALUES ($1, $2, NOW())
                ON CONFLICT (wallet_address)
                DO UPDATE SET note = EXCLUDED.note, updated_ts = NOW()
            `;
            
            await this.db.query(query, [wallet.toLowerCase(), note]);
            return true;
        } catch (error) {
            console.error(`❌ 更新錢包備註失敗:`, error);
            return false;
        }
    }

    /**
     * 🔔 PostgreSQL NOTIFY - 發送通知
     */
    async notify(channel, payload) {
        try {
            const escapedPayload = payload.replace(/'/g, "''");
            await this.db.query(`NOTIFY ${channel}, '${escapedPayload}'`);
        } catch (error) {
            console.error(`❌ 發送PostgreSQL通知失敗 (${channel}):`, error);
        }
    }

    /**
     * 📡 通知前端連接狀態 - 使用直接 WebSocket 廣播
     */
    async notifyConnectionStatus(connected) {
        const statusData = {
            type: 'connection_status',
            connected: connected,
            timestamp: TimeService.getCurrentTaipeiTime()
        };
        
        // 🚀 新架構：直接 WebSocket 廣播
        this.broadcastToClients({
            channel: 'realtime_status',
            data: statusData
        });
    }


    /**
     * 📡 通知前端局次鎖倉 - 使用直接 WebSocket 廣播
     */
    async notifyRoundLock(epoch) {
        const lockData = {
            type: 'round_lock',
            epoch: epoch,
            timestamp: TimeService.getCurrentTaipeiTime()
        };
        
        // 🚀 新架構：直接 WebSocket 廣播
        this.broadcastToClients({
            channel: 'realtime_status',
            data: lockData
        });
    }

    /**
     * 📡 通知前端可疑活動 - 使用直接 WebSocket 廣播
     */
    async notifySuspiciousActivity(data) {
        const suspiciousData = {
            type: 'suspicious_activity',
            ...data
        };
        
        // 🚀 新架構：直接 WebSocket 廣播
        this.broadcastToClients({
            channel: 'realtime_status',
            data: suspiciousData
        });
    }

    /**
     * 🔄 安排重新連接
     */
    scheduleReconnect() {
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
        }
        
        this.reconnectTimer = setTimeout(() => {
            console.log('🔄 嘗試重新連接區塊鏈...');
            this.startBlockchainListener();
        }, MONITORING_CONSTANTS.RECONNECT_DELAY);
    }

    /**
     * 🧹 設置清理定時器
     */
    setupCleanupTimer() {
        setInterval(() => {
            this.suspiciousMonitor.cleanup();
            this.cleanupProcessedBets();
        }, MONITORING_CONSTANTS.CLEANUP_INTERVAL);
    }

    /**
     * 🗑️ 清空指定局次的內存記錄（局次鎖倉後調用）
     */
    async clearMemoryForEpoch(epoch) {
        const epochStr = epoch.toString();
        let clearedCount = 0;
        
        // 清除該局次的所有內存記錄
        for (const [betKey, timestamp] of this.processedBets.entries()) {
            if (betKey.startsWith(`${epochStr}_`)) {
                this.processedBets.delete(betKey);
                clearedCount++;
            }
        }
        
        if (clearedCount > 0) {
            console.log(`🗑️ 局次${epochStr}鎖倉，已清空 ${clearedCount} 個內存記錄，剩餘 ${this.processedBets.size} 個記錄`);
        }
    }

    /**
     * 🧹 清理過期的已處理下注記錄（保留機制，以防萬一）
     */
    cleanupProcessedBets() {
        // 簡化：只清理很舊的記錄，正常情況下局次鎖倉時已清空
        const now = Date.now();
        const oneHourAgo = now - (60 * 60 * 1000); // 1小時前
        let cleanedCount = 0;
        
        for (const [betKey, timestamp] of this.processedBets.entries()) {
            if (timestamp < oneHourAgo) {
                this.processedBets.delete(betKey);
                cleanedCount++;
            }
        }
        
        if (cleanedCount > 0) {
            console.log(`🧹 清理了 ${cleanedCount} 個過期內存記錄（兜底清理），剩餘 ${this.processedBets.size} 個記錄`);
        }
    }

    /**
     * 🗑️ 清理指定局次的realbet數據（歷史爬蟲完成後調用）
     */
    async cleanupRealbetForEpochs(epochs) {
        try {
            if (!epochs || epochs.length === 0) {
                console.log('⚠️ 沒有指定要清理的局次');
                return;
            }

            // 清理數據庫中的realbet記錄
            const placeholders = epochs.map((_, index) => `$${index + 1}`).join(',');
            const query = `DELETE FROM realbet WHERE epoch IN (${placeholders})`;
            
            const result = await this.db.query(query, epochs);
            console.log(`🗑️ 已從realbet清理 ${result.rowCount} 條記錄，局次: ${epochs.join(', ')}`);

            // 同時清理內存中對應的記錄（通常局次鎖倉時已清空，這裡是兜底）
            let memoryCleanedCount = 0;
            for (const epoch of epochs) {
                for (const [betKey, timestamp] of this.processedBets.entries()) {
                    if (betKey.startsWith(`${epoch}_`)) {
                        this.processedBets.delete(betKey);
                        memoryCleanedCount++;
                    }
                }
            }
            
            if (memoryCleanedCount > 0) {
                console.log(`🗑️ 兜底清理了 ${memoryCleanedCount} 個內存記錄，局次: ${epochs.join(', ')}`);
            }
            
            return {
                databaseCleaned: result.rowCount,
                memoryCleaned: memoryCleanedCount
            };
            
        } catch (error) {
            console.error('❌ 清理realbet數據失敗:', error);
            throw error;
        }
    }

    /**
     * 🗑️ 清理舊局次數據（保留最近3局）
     */
    async cleanupOldRealbet() {
        try {
            if (!this.currentRound) {
                console.log('⚠️ 當前局次未知，跳過清理');
                return;
            }

            const currentEpoch = parseInt(this.currentRound);
            const keepEpochs = [currentEpoch, currentEpoch - 1, currentEpoch - 2]; // 保留最近3局
            
            // 清理超過3局的舊數據
            const query = `
                DELETE FROM realbet 
                WHERE epoch < $1
            `;
            
            const result = await this.db.query(query, [currentEpoch - 2]);
            
            if (result.rowCount > 0) {
                console.log(`🗑️ 自動清理了 ${result.rowCount} 條舊realbet記錄（保留最近3局: ${keepEpochs.join(', ')}）`);
                
                // 內存清理已在局次鎖倉時處理，這裡不需要重複清理
            }
            
        } catch (error) {
            console.error('❌ 自動清理舊realbet數據失敗:', error);
        }
    }

    /**
     * 📊 獲取系統狀態
     */
    getStatus() {
        return {
            isConnected: this.isConnected,
            currentRound: this.currentRound,
            currentLockTimestamp: this.currentLockTimestamp,
            suspiciousWallets: this.suspiciousMonitor.getSuspiciousWallets(),
            databaseConnected: this.db !== null,
            processedBetsCount: this.processedBets.size // 內存中已處理的下注數量
        };
    }

    /**
     * 🔍 查看內存中的處理記錄（調試用）
     */
    getProcessedBetsDebugInfo() {
        const debugInfo = {
            totalCount: this.processedBets.size,
            records: [],
            byEpoch: {}
        };

        // 按局次分組統計
        for (const [betKey, timestamp] of this.processedBets.entries()) {
            const [epoch, wallet] = betKey.split('_');
            const timeStr = new Date(timestamp).toLocaleString('zh-TW', {timeZone: 'Asia/Taipei'});
            
            const record = {
                epoch: epoch,
                wallet: wallet,
                processedAt: timeStr,
                timestamp: timestamp
            };
            
            debugInfo.records.push(record);
            
            if (!debugInfo.byEpoch[epoch]) {
                debugInfo.byEpoch[epoch] = 0;
            }
            debugInfo.byEpoch[epoch]++;
        }

        // 按時間排序
        debugInfo.records.sort((a, b) => b.timestamp - a.timestamp);
        
        return debugInfo;
    }

    /**
     * 🖨️ 打印內存狀態（調試用）
     */
    printMemoryStatus() {
        const debugInfo = this.getProcessedBetsDebugInfo();
        
        console.log('\n📊 內存處理記錄狀態:');
        console.log(`   總記錄數: ${debugInfo.totalCount}`);
        console.log('   按局次分布:', debugInfo.byEpoch);
        
        if (debugInfo.records.length > 0) {
            console.log('   最近10筆記錄:');
            debugInfo.records.slice(0, 10).forEach((record, index) => {
                console.log(`   ${index + 1}. 局次${record.epoch} ${record.wallet.substring(0, 8)}... [${record.processedAt}]`);
            });
        }
        console.log('');
    }

    /**
     * 🧹 清理資源
     */
    cleanup() {
        if (this.contract) {
            this.contract.removeAllListeners();
        }
        if (this.provider) {
            this.provider.destroy();
        }
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
        }
        
        // 🚀 新增：清理 WebSocket 服務器
        if (this.wss) {
            console.log('🧹 正在關閉WebSocket服務器...');
            this.wss.close();
        }
        if (this.wsServer) {
            this.wsServer.close();
        }
        this.connectedClients.clear();
        
        if (this.db) {
            this.db.end();
        }
        // 清理內存中的已處理記錄
        this.processedBets.clear();
        this.isConnected = false;
        console.log('✅ V6即時數據監聽器已清理');
    }
}

// 如果直接運行此文件
if (require.main === module) {
    const listener = new V6RealtimeListener();
    
    // 優雅關閉處理
    process.on('SIGINT', () => {
        console.log('\n🛑 接收到關閉信號，正在清理V6資源...');
        listener.cleanup();
        process.exit(0);
    });
    
    process.on('SIGTERM', () => {
        console.log('\n🛑 接收到終止信號，正在清理V6資源...');
        listener.cleanup();
        process.exit(0);
    });
    
    listener.initialize().catch(error => {
        console.error('❌ V6啟動失敗:', error);
        process.exit(1);
    });
}

module.exports = V6RealtimeListener;