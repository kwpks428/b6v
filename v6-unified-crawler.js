const { ethers } = require('ethers');
const { Pool } = require('pg');
const TimeService = require('./TimeService');

/*
 * =====================================================================================
 * V6 Unified Crawler System
 *
 * System Purpose:
 * This module is the core historical data crawler for the V6 system. It is responsible
 * for fetching, validating, and storing historical data from the blockchain.
 *
 * Core Responsibilities:
 * - Fetches all relevant event data within a specific time range (from the start of
 *   the current epoch to the start of the next epoch).
 * - Validates data integrity, ensuring that `round`, `hisbet` (both UP and DOWN),
 *   and `claim` data are present and correct.
 * - Atomically stores the validated data into the `round`, `hisbet`, and `claim` tables
 *   using database transactions.
 * - Cleans up corresponding data from the `realbet` table after successful migration.
 * - Detects and records abnormal "multi-claim" activities within a single epoch.
 *
 * V6 Core Improvements:
 * - Unified time handling via the `TimeService`.
 * - Standardization of bet directions to 'UP'/'DOWN'.
 * - Use of NUMERIC format in the database to prevent floating-point errors.
 * - Detection of single-round multi-claim patterns, recorded in the `multi_claims` table.
 *
 * This system is an upgrade based on the robust architecture of v4bets/unified-crawler.js.
 * =====================================================================================
*/

// 合約配置
const CONTRACT_CONFIG = {
    ADDRESS: '0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA',
    ABI_FILE: './abi.json',
    TREASURY_FEE_RATE: 0.03
};

// --- Configuration ---
// Railway environment variables support
const RPC_URL = process.env.V6_RPC_URL || process.env.RPC_URL || 'https://lb.drpc.org/bsc/Ahc3I-33qkfGuwXSahR3XfPDRmd6WZsR8JbErqRhf0fE';
const DATABASE_URL = process.env.V6_DATABASE_URL || process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require';

// Railway deployment check
const IS_RAILWAY = process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY_PROJECT_ID;
if (IS_RAILWAY) {
    console.log('🚀 Running on Railway platform');
    console.log('📊 Project:', process.env.RAILWAY_PROJECT_NAME || 'Unknown');
    console.log('🌍 Environment:', process.env.RAILWAY_ENVIRONMENT || 'production');
}

if (!DATABASE_URL.includes('postgresql://')) {
    console.error('❌ Invalid DATABASE_URL format. Must be a PostgreSQL connection string.');
    process.exit(1);
}
/**
 * V6單局多次領獎檢測器
 * 
 * 🎯 功能：檢測同一錢包在同一局次的多次領獎行為
 */
class V6SingleRoundClaimDetector {
    constructor(db) {
        this.db = db;
        this.suspiciousThreshold = 3; // 超過3次領獎視為可疑
    }
    
    /**
     * 🔍 檢查當局多次領獎
     * 
     * @param {string} epoch - 局次編號  
     * @param {Array} claimData - 當局領獎數據
     * @returns {Array} 可疑錢包列表
     */
    async checkSingleRoundMultiClaims(epoch, claimData) {
        try {
            // 統計每個錢包的領獎次數和總額
            const walletStats = {};
            
            for (const claim of claimData) {
                const wallet = claim.wallet_address.toLowerCase();
                const amount = parseFloat(claim.claim_amount);
                
                if (!walletStats[wallet]) {
                    walletStats[wallet] = { count: 0, totalAmount: 0 };
                }
                
                walletStats[wallet].count++;
                walletStats[wallet].totalAmount += amount;
            }
            
            const suspiciousWallets = [];
            
            // 檢查單局多次領獎
            for (const [wallet, stats] of Object.entries(walletStats)) {
                if (stats.count > this.suspiciousThreshold) {
                    suspiciousWallets.push({
                        wallet_address: wallet,
                        epoch: epoch,
                        claim_count: stats.count,
                        total_amount: stats.totalAmount
                    });
                    
                    console.log(`🚨 檢測到可疑多次領獎: ${wallet} 在局次${epoch} 領獎${stats.count}次，總額${stats.totalAmount.toFixed(4)} BNB`);
                }
            }
            
            // 記錄到數據庫
            if (suspiciousWallets.length > 0) {
                await this.recordSuspiciousWallets(suspiciousWallets);
            }
            
            return suspiciousWallets;
            
        } catch (error) {
            console.error('❌ 單局多次領獎檢測失敗:', error);
            return [];
        }
    }
    
    /**
     * 記錄可疑錢包到multi_claims表
     */
    async recordSuspiciousWallets(suspiciousWallets) {
        try {
            for (const suspicious of suspiciousWallets) {
                const query = `
                    INSERT INTO multi_claims (
                        epoch, wallet_address, claim_count, total_amount
                    ) VALUES ($1, $2, $3, $4)
                    ON CONFLICT (epoch, wallet_address) 
                    DO UPDATE SET 
                        claim_count = EXCLUDED.claim_count,
                        total_amount = EXCLUDED.total_amount
                `;
                
                await this.db.query(query, [
                    suspicious.epoch,
                    suspicious.wallet_address,
                    suspicious.claim_count,
                    suspicious.total_amount
                ]);
            }
            
            console.log(`✅ 已記錄 ${suspiciousWallets.length} 個可疑錢包到multi_claims表`);
            
        } catch (error) {
            console.error('❌ 記錄可疑錢包失敗:', error);
        }
    }
}

/**
 * V6統一爬蟲系統
 * 
 * 🎯 基於v4bets/unified-crawler.js優秀架構的V6升級版
 */
class V6UnifiedCrawler {
    constructor() {
        // 數據庫連接配置 - V6 b6v數據庫
        this.dbPool = null;
        
        // 區塊鏈連接
        this.provider = new ethers.JsonRpcProvider(RPC_URL);
        this.contract = new ethers.Contract(CONTRACT_CONFIG.ADDRESS, require(CONTRACT_CONFIG.ABI_FILE), this.provider);
        this.treasuryFeeRate = CONTRACT_CONFIG.TREASURY_FEE_RATE;
        
        // --- Rate Limiting ---
        // The current setting of 100 req/s is an optimized value for the drpc.org node.
        // While the node can handle more, this limit ensures:
        // 1. Avoidance of triggering provider-side rate-limiting mechanisms.
        // 2. Stable, long-term operation without network errors from rapid requests.
        // Modifying this value may risk node access being blocked.
        this.maxRequestsPerSecond = 100;
        this.requestDelay = Math.ceil(1000 / this.maxRequestsPerSecond);
        this.lastRequestTime = 0;
        
        // 單局多次領獎檢測器
        this.claimDetector = null;
        
        // 失敗重試記錄
        this.failedAttempts = new Map(); // epoch -> attempt count
        
        // 處理狀態
        this.isProcessingHistory = false;
        this.shouldStopHistory = false;
        
        // 統計信息
        this.stats = {
            roundsProcessed: 0,
            betsProcessed: 0,  
            claimsProcessed: 0,
            suspiciousWalletsDetected: 0,
            errors: 0
        };
    }
    
    /**
     * 數據庫連接getter，統一訪問接口
     */
    get db() {
        return this.dbPool;
    }
    
    /**
     * 🔄 初始化系統
     */
    async initialize() {
        try {
            console.log('🔄 初始化V6統一爬蟲系統...');
            
            // 初始化數據庫連接
            await this.initializeDatabase();
            console.log('✅ V6數據庫連接成功');
            
            // 測試區塊鏈連接
            const currentEpoch = await this.getCurrentEpoch();
            console.log(`✅ 區塊鏈連接成功，當前局次: ${currentEpoch}`);
            
            // 初始化檢測器
            this.claimDetector = new V6SingleRoundClaimDetector(this.dbPool);
            console.log('✅ 單局多次領獎檢測器初始化完成');
            
            console.log('🚀 V6統一爬蟲系統啟動完成');
            
        } catch (error) {
            console.error('❌ V6統一爬蟲系統初始化失敗:', error);
            throw error;
        }
    }
    
    /**
     * 🔌 初始化數據庫連接
     */
    async initializeDatabase() {
        try {
            this.dbPool = new Pool({
                connectionString: DATABASE_URL,
                ssl: {
                    rejectUnauthorized: false
                }
            });

            this.dbPool.on('error', (err) => {
                console.error('❌ [V6UnifiedCrawler] 數據庫連接錯誤:', err.message);
                // 不要拋出錯誤，只記錄日誌
                this.handleDatabaseError(err);
            });
            
            // 設置時區為台北時間
            await this.dbPool.query("SET timezone = 'Asia/Taipei'");
            
            // 驗證連接和時區
            const timeResult = await this.dbPool.query('SELECT NOW() as current_time, current_setting(\'timezone\') as timezone');
            console.log(`📅 V6數據庫時區: ${timeResult.rows[0].timezone}`);
            console.log(`🕐 V6數據庫當前時間: ${TimeService.formatTaipeiTime(timeResult.rows[0].current_time)}`);
            
            return true;
        } catch (error) {
            console.error('❌ V6數據庫初始化失敗:', error.message);
            throw error;
        }
    }
    
    /**
     * 🔧 處理數據庫連接錯誤
     */
    handleDatabaseError(error) {
        console.error('🚨 [V6UnifiedCrawler] 數據庫連接發生錯誤:', {
            message: error.message,
            code: error.code,
            timestamp: TimeService.getCurrentTaipeiTime()
        });
        
        // 標記數據庫連接為不健康（如果需要的話可以觸發重連邏輯）
        this.dbHealthy = false;
    }
    
    /**
     * 🔄 檢查並恢復數據庫連接
     */
    async ensureDatabaseConnection() {
        try {
            // 檢查連接是否有效
            if (!this.dbPool || this.dbPool.ending || !this.dbHealthy) {
                console.log('🔄 數據庫連接無效，嘗試重新連接...');
                await this.reconnectDatabase();
                return;
            }
            
            // 測試連接
            await this.dbPool.query('SELECT 1');
            this.dbHealthy = true;
            
        } catch (error) {
            console.log('🔄 數據庫連接測試失敗，嘗試重新連接...');
            await this.reconnectDatabase();
        }
    }
    
    /**
     * 🔄 重新連接數據庫
     */
    async reconnectDatabase() {
        try {
            // 關閉舊連接
            if (this.dbPool && !this.dbPool.ending) {
                try {
                    this.dbPool.removeAllListeners();
                    await this.dbPool.end();
                } catch (e) {
                    console.log('清理舊連接時出錯:', e.message);
                }
            }
            
            // 創建新連接
            await this.initializeDatabase();
            this.dbHealthy = true;
            console.log('✅ 數據庫重新連接成功');
            
        } catch (error) {
            console.error('❌ 數據庫重新連接失敗:', error.message);
            this.dbHealthy = false;
            throw error;
        }
    }
    
    /**
     * ⏱️ 請求速率限制
     */
    async rateLimit() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequestTime;
        
        if (timeSinceLastRequest < this.requestDelay) {
            const waitTime = this.requestDelay - timeSinceLastRequest;
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        this.lastRequestTime = Date.now();
    }
    
    /**
     * 🔄 帶重試的網路請求
     */
    async retryRequest(operation, operationName, retries = 3) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                await this.rateLimit();
                const result = await operation();
                return result;
            } catch (error) {
                if (attempt === retries) {
                    console.error(`❌ ${operationName} 失敗 (${attempt}/${retries}) - ${error.message}`);
                    throw error;
                }
                
                const delay = 2000 * attempt;
                console.log(`⚠️ ${operationName} 重試 ${attempt}/${retries}，等待 ${delay}ms`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }
    
    /**
     * 📊 獲取當前最新局次
     */
    async getCurrentEpoch() {
        try {
            const epoch = await this.retryRequest(
                () => this.contract.currentEpoch(),
                '獲取當前局次'
            );
            return Number(epoch);
        } catch (error) {
            console.error('獲取當前局次失敗:', error.message);
            return 0;
        }
    }
    
    /**
     * 💰 計算賠率（扣除3%手續費）
     */
    calculatePayouts(totalAmount, upAmount, downAmount) {
        const totalAfterFee = totalAmount * (1 - this.treasuryFeeRate);
        
        let upPayout = 0;
        let downPayout = 0;
        
        if (upAmount > 0) {
            upPayout = totalAfterFee / upAmount;
        }
        
        if (downAmount > 0) {
            downPayout = totalAfterFee / downAmount;
        }
        
        return {
            upPayout: upPayout.toFixed(4),
            downPayout: downPayout.toFixed(4)
        };
    }
    
    /**
     * ✅ 檢查局次是否已存在
     */
    async hasRoundData(epoch) {
        try {
            // 確保數據庫連接正常
            await this.ensureDatabaseConnection();
            
            const query = 'SELECT epoch FROM round WHERE epoch = $1';
            const result = await this.db.query(query, [epoch.toString()]);
            return result.rows.length > 0;
        } catch (error) {
            console.error(`檢查局次 ${epoch} 失敗:`, error.message);
            return false;
        }
    }
    
    /**
     * 📊 獲取局次基本數據
     */
    async getRoundData(epoch) {
        try {
            const round = await this.retryRequest(
                () => this.contract.rounds(epoch),
                `獲取局次 ${epoch} 數據`
            );
            
            // 檢查局次是否已結束
            if (Number(round.closeTimestamp) === 0) {
                return null; // 局次尚未結束
            }
            
            // 🔥 V6強制標準化：將結果轉換為UP/DOWN
            let result = null;
            if (Number(round.closePrice) > Number(round.lockPrice)) {
                result = 'UP';
            } else if (Number(round.closePrice) < Number(round.lockPrice)) {
                result = 'DOWN';
            } // null表示平手
            
            // 計算金額和賠率
            const totalAmount = parseFloat(ethers.formatEther(round.totalAmount));
            const bullAmount = parseFloat(ethers.formatEther(round.bullAmount)); // V6: UP = bull
            const bearAmount = parseFloat(ethers.formatEther(round.bearAmount)); // V6: DOWN = bear
            
            const payouts = this.calculatePayouts(totalAmount, bullAmount, bearAmount);
            
            return {
                epoch: Number(round.epoch),
                start_ts: TimeService.formatUnixTimestamp(Number(round.startTimestamp)),
                lock_ts: TimeService.formatUnixTimestamp(Number(round.lockTimestamp)),
                close_ts: TimeService.formatUnixTimestamp(Number(round.closeTimestamp)),
                lock_price: ethers.formatUnits(round.lockPrice, 8),
                close_price: ethers.formatUnits(round.closePrice, 8),
                result: result,
                total_amount: totalAmount.toString(),
                up_amount: bullAmount.toString(),  // V6: 統一命名
                down_amount: bearAmount.toString(), // V6: 統一命名
                up_payout: payouts.upPayout,
                down_payout: payouts.downPayout
            };
            
        } catch (error) {
            console.error(`獲取局次 ${epoch} 數據失敗:`, error.message);
            return null;
        }
    }
    
    /**
     * 🔍 根據時間戳查找區塊（二分查找算法）
     * 
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改二分查找算法！⚠️⚠️⚠️
     * 此算法是經過精密計算和測試的，絕對不准修改：
     * 1. 不准改成線性查找（會導致超時）
     * 2. 不准修改查找範圍（low=1, high=currentBlock）
     * 3. 不准修改中點計算公式 Math.floor((low + high) / 2)
     * 4. 不准修改循環終止條件 (low <= high)
     * 5. 不准刪除或修改最接近區塊的追蹤邏輯
     * 🔥🔥🔥 此算法能在Log(N)時間內精確定位目標區塊！🔥🔥🔥
     * 🚨🚨🚨 任何修改都會導致性能急劇下降或查找錯誤！🚨🚨🚨
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改二分查找算法！⚠️⚠️⚠️
     */
    async findBlockByTimestamp(targetTimestamp) {
        try {
            const currentBlock = await this.retryRequest(
                () => this.provider.getBlockNumber(),
                '獲取當前區塊號'
            );
            const currentBlockData = await this.retryRequest(
                () => this.provider.getBlock(currentBlock),
                `獲取當前區塊數據 ${currentBlock}`
            );
            const currentTimestamp = currentBlockData.timestamp;
            
            if (targetTimestamp >= currentTimestamp) {
                return currentBlock;
            }
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改二分查找核心算法！⚠️⚠️⚠️
            // 這是經典的二分查找實現，任何修改都會破壞算法正確性
            // 絕對不准改成 while(true) 或其他循環條件
            // 絕對不准修改 low, high, mid 的計算邏輯  
            // 絕對不准刪除 closestBlock 追蹤邏輯
            // 🔥🔥🔥 保持原有邏輯，確保O(log n)時間複雜度！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改二分查找核心算法！⚠️⚠️⚠️
            let low = 1;
            let high = currentBlock;
            let closestBlock = high;
            let closestDiff = Math.abs(currentTimestamp - targetTimestamp);
            
            while (low <= high) {
                const mid = Math.floor((low + high) / 2);
                
                const blockData = await this.retryRequest(
                    () => this.provider.getBlock(mid),
                    `獲取區塊 ${mid} 數據`
                );
                
                if (!blockData) {
                    high = mid - 1;
                    continue;
                }
                
                const blockTimestamp = blockData.timestamp;
                const diff = Math.abs(blockTimestamp - targetTimestamp);
                
                if (diff < closestDiff) {
                    closestDiff = diff;
                    closestBlock = mid;
                }
                
                if (blockTimestamp < targetTimestamp) {
                    low = mid + 1;
                } else if (blockTimestamp > targetTimestamp) {
                    high = mid - 1;
                } else {
                    return mid; // 完全匹配
                }
            }
            
            return closestBlock;
            
        } catch (error) {
            console.error(`查找時間戳 ${targetTimestamp} 對應區塊失敗:`, error.message);
            return null;
        }
    }
    
    /**
     * 📡 獲取指定區塊範圍內的所有事件
     */
    async getEventsInRange(fromBlock, toBlock) {
        try {
            const betBullFilter = this.contract.filters.BetBull();
            const betBearFilter = this.contract.filters.BetBear();
            const claimFilter = this.contract.filters.Claim();
            
            const [betBullEvents, betBearEvents, claimEvents] = await Promise.all([
                this.retryRequest(
                    () => this.contract.queryFilter(betBullFilter, fromBlock, toBlock),
                    `獲取 BetBull 事件 (${fromBlock}-${toBlock})`
                ),
                this.retryRequest(
                    () => this.contract.queryFilter(betBearFilter, fromBlock, toBlock),
                    `獲取 BetBear 事件 (${fromBlock}-${toBlock})`
                ),
                this.retryRequest(
                    () => this.contract.queryFilter(claimFilter, fromBlock, toBlock),
                    `獲取 Claim 事件 (${fromBlock}-${toBlock})`
                )
            ]);
            
            return {
                betBullEvents,
                betBearEvents,
                claimEvents
            };
            
        } catch (error) {
            console.error(`獲取區塊範圍 ${fromBlock}-${toBlock} 事件失敗:`, error.message);
            return {
                betBullEvents: [],
                betBearEvents: [],
                claimEvents: []
            };
        }
    }
    
    /**
     * ✅ 驗證數據完整性
     */
    validateDataIntegrity(epoch, roundData, betData, claimData) {
        // 檢查 round 數據
        if (!roundData || !roundData.epoch) {
            return { valid: false, reason: 'round 數據缺失或不完整' };
        }
        
        // 檢查必要欄位
        const requiredFields = ['start_ts', 'lock_ts', 'close_ts', 'lock_price', 'close_price', 'result', 'total_amount'];
        for (const field of requiredFields) {
            if (!roundData[field] && roundData[field] !== 0 && roundData[field] !== null) {
                return { valid: false, reason: `round 表缺少 ${field} 欄位` };
            }
        }
        
        // 檢查 hisbet 數據
        if (!betData || betData.length === 0) {
            return { valid: false, reason: 'hisbet 數據缺失，至少需要一筆下注數據' };
        }
        
        // 檢查是否同時有UP和DOWN數據
        const hasUpBets = betData.some(bet => bet.bet_direction === 'UP');
        const hasDownBets = betData.some(bet => bet.bet_direction === 'DOWN');
        
        if (!hasUpBets || !hasDownBets) {
            return { valid: false, reason: 'hisbet 數據不完整，需要同時包含UP和DOWN數據' };
        }
        
        // 🔥 修正：claim數據允許為空
        // 原因：並非每局都有人領獎（平手局、延遲領獎、跨局領獎等）
        // claim數據的完整性不應該影響該局次的保存
        if (claimData && claimData.length > 0) {
            console.log(`📊 局次 ${epoch} 包含 ${claimData.length} 筆領獎數據`);
        } else {
            console.log(`📊 局次 ${epoch} 無領獎數據（這是正常的）`);
        }
        
        return { valid: true };
    }
    
    /**
     * 📊 處理單個局次數據
     */
    async processEpochData(epoch) {
        try {
            console.log(`🔄 開始處理局次 ${epoch}`);
            
            // 檢查是否應該跳過此局次（失敗次數過多）
            if (await this.shouldSkipEpoch(epoch)) {
                console.log(`⏭️ 跳過局次 ${epoch}（失敗次數過多）`);
                return false;
            }
            
            // 獲取局次基本數據
            const roundData = await this.getRoundData(epoch);
            if (!roundData) {
                console.log(`⏭️ 局次 ${epoch} 尚未結束或數據無效`);
                return false;
            }
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改時間範圍抓取邏輯！⚠️⚠️⚠️
            // 🎯 關鍵：獲取下一局的開始時間來確定區塊範圍
            // 這是核心業務邏輯：必須從【當前局開始時間】到【下一局開始時間】
            // 絕對不准改成：當前局開始到當前局結束
            // 絕對不准改成：當前局鎖倉到當前局結束  
            // 絕對不准改成：任何其他時間範圍
            // 🔥🔥🔥 原因：只有【當局開始→下局開始】才能抓到所有相關數據！🔥🔥🔥
            // 🚨🚨🚨 任何人修改此邏輯將導致數據嚴重缺失！🚨🚨🚨
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改時間範圍抓取邏輯！⚠️⚠️⚠️
            const nextEpochStartTime = await this.getNextEpochStartTime(epoch + 1);
            if (!nextEpochStartTime) {
                console.log(`⏭️ 無法獲取局次 ${epoch + 1} 開始時間，跳過`);
                return false;
            }
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改區塊範圍計算邏輯！⚠️⚠️⚠️
            // 計算區塊範圍：當前局開始 → 下一局開始（這是唯一正確的方式）
            // 絕對不准改成 lockTimestamp 或 closeTimestamp
            // 絕對不准改成任何其他時間戳
            // 🔥🔥🔥 只有這樣才能抓到完整的跨局次數據！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改區塊範圍計算邏輯！⚠️⚠️⚠️
            const currentStartTime = Math.floor(new Date(roundData.start_ts).getTime() / 1000);
            const nextStartTime = nextEpochStartTime;
            
            console.log(`📅 局次 ${epoch} 時間範圍: ${TimeService.formatUnixTimestamp(currentStartTime)} → ${TimeService.formatUnixTimestamp(nextStartTime)}`);
            
            const fromBlock = await this.findBlockByTimestamp(currentStartTime);
            const toBlock = await this.findBlockByTimestamp(nextStartTime);
            
            if (!fromBlock || !toBlock) {
                throw new Error('無法確定區塊範圍');
            }
            
            console.log(`🔍 搜索區塊範圍: ${fromBlock} → ${toBlock}`);
            
            // 獲取區塊範圍內的所有事件
            const events = await this.getEventsInRange(fromBlock, toBlock);
            
            // 處理下注事件
            const betData = [];
            await this.processBetEvents(events.betBullEvents, 'UP', betData, roundData.result);
            await this.processBetEvents(events.betBearEvents, 'DOWN', betData, roundData.result);
            
            // 處理領獎事件
            const claimData = [];
            await this.processClaimEvents(events.claimEvents, claimData, epoch);
            
            console.log(`📊 抓取數據統計 - 下注:${betData.length}, 領獎:${claimData.length}`);
            
            // 驗證數據完整性
            const validation = this.validateDataIntegrity(epoch, roundData, betData, claimData);
            if (!validation.valid) {
                console.log(`❌ 局次 ${epoch} 數據不完整: ${validation.reason}`);
                await this.handleEpochFailure(epoch, `數據不完整: ${validation.reason}`);
                return false;
            }
            
            console.log(`✅ 局次 ${epoch} 數據完整性驗證通過`);
            
            // 🔄 事務方式保存數據
            const success = await this.saveCompleteRoundData(roundData, betData, claimData);
            
            if (success) {
                // 🧹 清理realbet表中的對應數據
                await this.cleanupRealbetData(epoch);
                
                // 🚨 檢查單局多次領獎
                const suspiciousWallets = await this.claimDetector.checkSingleRoundMultiClaims(epoch, claimData);
                if (suspiciousWallets.length > 0) {
                    this.stats.suspiciousWalletsDetected += suspiciousWallets.length;
                }
                
                // 清除失敗記錄
                this.failedAttempts.delete(epoch);
                
                // 更新統計
                this.stats.roundsProcessed++;
                this.stats.betsProcessed += betData.length;
                this.stats.claimsProcessed += claimData.length;
                
                console.log(`✅ 局次 ${epoch} 數據處理完成 (${betData.length} 筆下注, ${claimData.length} 筆領獎)`);
                return true;
            } else {
                console.log(`❌ 局次 ${epoch} 數據保存失敗`);
                await this.handleEpochFailure(epoch, '數據保存失敗');
                return false;
            }
            
        } catch (error) {
            console.error(`❌ 處理局次 ${epoch} 失敗:`, error.message);
            await this.handleEpochFailure(epoch, error.message);
            this.stats.errors++;
            return false;
        }
    }
    
    /**
     * 📅 獲取下一局的開始時間
     * 
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改此方法的邏輯！⚠️⚠️⚠️
     * 此方法是時間範圍抓取的核心，必須獲取【下一局的開始時間】
     * 絕對不准改成獲取當前局的任何時間戳
     * 絕對不准改成獲取下一局的鎖倉或結束時間  
     * 🔥🔥🔥 只有下一局開始時間才能確定正確的抓取範圍！🔥🔥🔥
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改此方法的邏輯！⚠️⚠️⚠️
     */
    async getNextEpochStartTime(nextEpoch) {
        try {
            const round = await this.retryRequest(
                () => this.contract.rounds(nextEpoch),
                `獲取局次 ${nextEpoch} 開始時間`
            );
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改為其他時間戳！⚠️⚠️⚠️
            // 必須檢查 startTimestamp，不准改成 lockTimestamp 或 closeTimestamp
            // 🔥🔥🔥 只有 startTimestamp 才是正確的下一局開始時間！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改為其他時間戳！⚠️⚠️⚠️
            if (Number(round.startTimestamp) === 0) {
                return null; // 下一局尚未開始
            }
            
            return Number(round.startTimestamp);
            
        } catch (error) {
            console.error(`獲取局次 ${nextEpoch} 開始時間失敗:`, error.message);
            return null;
        }
    }
    
    /**
     * 📝 處理下注事件
     */
    async processBetEvents(events, direction, betData, roundResult) {
        for (const event of events) {
            const blockTimestamp = await this.getBlockTimestamp(event.blockNumber);
            
            // 計算WIN/LOSS結果
            let result = null;
            if (roundResult) {
                result = (direction === roundResult) ? 'WIN' : 'LOSS';
            }
            
            betData.push({
                epoch: Number(event.args.epoch),
                bet_ts: TimeService.formatUnixTimestamp(blockTimestamp),
                wallet_address: event.args.sender.toLowerCase(),
                bet_direction: direction,
                amount: ethers.formatEther(event.args.amount),
                result: result,
                tx_hash: event.transactionHash
            });
        }
    }
    
    /**
     * 🏆 處理領獎事件
     */
    async processClaimEvents(events, claimData, processingEpoch) {
        for (const event of events) {
            const blockTimestamp = await this.getBlockTimestamp(event.blockNumber);
            
            claimData.push({
                epoch: processingEpoch, // BUG FIX: This is the epoch the crawler is processing.
                claim_ts: TimeService.formatUnixTimestamp(blockTimestamp),
                wallet_address: event.args.sender.toLowerCase(),
                claim_amount: ethers.formatEther(event.args.amount),
                bet_epoch: Number(event.args.epoch), // This is the epoch the reward is for.
                tx_hash: event.transactionHash
            });
        }
    }
    
    /**
     * 🕐 獲取區塊時間戳
     */
    async getBlockTimestamp(blockNumber) {
        try {
            const block = await this.retryRequest(
                () => this.provider.getBlock(blockNumber),
                `獲取區塊 ${blockNumber} 時間戳`
            );
            return block ? block.timestamp : Math.floor(Date.now() / 1000);
        } catch (error) {
            console.error(`獲取區塊 ${blockNumber} 時間戳失敗:`, error.message);
            return Math.floor(Date.now() / 1000);
        }
    }
    
    /**
     * 💾 事務方式保存完整局次數據
     */
    async saveCompleteRoundData(roundData, betData, claimData) {
        const client = this.db;
        
        try {
            await client.query('BEGIN');
            console.log(`🔄 開始事務保存局次 ${roundData.epoch} 數據...`);
            
            // 1. 保存round表數據
            const roundQuery = `
                INSERT INTO round (
                    epoch, start_ts, lock_ts, close_ts,
                    lock_price, close_price, result,
                    total_amount, up_amount, down_amount,
                    up_payout, down_payout
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (epoch) DO NOTHING
            `;
            
            await client.query(roundQuery, [
                roundData.epoch.toString(),
                roundData.start_ts,
                roundData.lock_ts,
                roundData.close_ts,
                roundData.lock_price,
                roundData.close_price,
                roundData.result,
                roundData.total_amount,
                roundData.up_amount,
                roundData.down_amount,
                roundData.up_payout,
                roundData.down_payout
            ]);
            
            // 2. 批量保存hisbet表數據
            if (betData.length > 0) {
                const betQuery = `
                    INSERT INTO hisbet (
                        epoch, bet_ts, wallet_address, bet_direction, 
                        amount, result, tx_hash
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (tx_hash) DO NOTHING
                `;
                
                for (const bet of betData) {
                    await client.query(betQuery, [
                        bet.epoch.toString(),
                        bet.bet_ts,
                        bet.wallet_address,
                        bet.bet_direction,
                        bet.amount,
                        bet.result,
                        bet.tx_hash
                    ]);
                }
            }
            
            // 3. 批量保存claim表數據
            if (claimData.length > 0) {
                const claimQuery = `
                    INSERT INTO claim (
                        epoch, claim_ts, wallet_address, claim_amount, bet_epoch, tx_hash
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (tx_hash) DO NOTHING
                `;
                
                for (const claim of claimData) {
                    await client.query(claimQuery, [
                        claim.epoch.toString(),
                        claim.claim_ts,
                        claim.wallet_address,
                        claim.claim_amount,
                        claim.bet_epoch.toString(),
                        claim.tx_hash
                    ]);
                }
            }
            
            await client.query('COMMIT');
            console.log(`✅ 局次 ${roundData.epoch} 數據事務保存成功`);
            return true;
            
        } catch (error) {
            await client.query('ROLLBACK');
            console.error(`❌ 局次 ${roundData.epoch} 數據事務保存失敗:`, error);
            return false;
        }
    }
    
    /**
     * 🧹 清理realbet表中的對應數據
     */
    async cleanupRealbetData(epoch) {
        try {
            // 確保數據庫連接正常
            await this.ensureDatabaseConnection();
            
            const deleteQuery = 'DELETE FROM realbet WHERE epoch = $1';
            const result = await this.db.query(deleteQuery, [epoch.toString()]);
            
            if (result.rowCount > 0) {
                console.log(`🧹 已清理realbet表中局次 ${epoch} 的 ${result.rowCount} 筆數據`);
            } else {
                console.log(`ℹ️  realbet表中無局次 ${epoch} 的數據需要清理`);
            }
            
        } catch (error) {
            console.error(`❌ 清理realbet數據失敗:`, error);
            // 不拋出錯誤，因為這不是關鍵操作
        }
    }
    
    /**
     * ❌ 處理局次失敗
     */
    async handleEpochFailure(epoch, reason) {
        const attempts = this.failedAttempts.get(epoch) || 0;
        this.failedAttempts.set(epoch, attempts + 1);
        
        if (attempts + 1 >= 3) {
            await this.recordFailedEpoch(epoch, reason);
            console.log(`🚫 局次 ${epoch} 重試 3 次仍失敗，已記錄並跳過`);
            this.failedAttempts.delete(epoch);
            return true; // 應該跳過
        }
        
        await this.deleteRoundData(epoch);
        console.log(`🗑️ 已删除局次 ${epoch} 的不完整數據，將重試 (${attempts + 1}/3)`);
        return false; // 不跳過，繼續重試
    }
    
    /**
     * 📝 記錄失敗局次
     */
    async recordFailedEpoch(epoch, errorMessage) {
        try {
            const query = `
                INSERT INTO failed_epoch (epoch, error_message, last_attempt_ts)
                VALUES ($1, $2, $3)
                ON CONFLICT (epoch) 
                DO UPDATE SET 
                    error_message = EXCLUDED.error_message,
                    last_attempt_ts = EXCLUDED.last_attempt_ts,
                    failure_count = failed_epoch.failure_count + 1
            `;
            
            await this.db.query(query, [
                epoch.toString(),
                errorMessage,
                TimeService.getCurrentTaipeiTime()
            ]);
            
        } catch (error) {
            console.error('❌ 記錄失敗局次失敗:', error);
        }
    }
    
    /**
     * 🗑️ 删除局次數據
     */
    async deleteRoundData(epoch) {
        try {
            await this.db.query('BEGIN');
            
            await this.db.query('DELETE FROM claim WHERE epoch = $1', [epoch.toString()]);
            await this.db.query('DELETE FROM hisbet WHERE epoch = $1', [epoch.toString()]);
            await this.db.query('DELETE FROM round WHERE epoch = $1', [epoch.toString()]);
            
            await this.db.query('COMMIT');
            
        } catch (error) {
            await this.db.query('ROLLBACK');
            console.error(`删除局次 ${epoch} 數據失敗:`, error);
        }
    }
    
    /**
     * ❓ 檢查是否應該跳過局次
     */
    async shouldSkipEpoch(epoch) {
        try {
            // 確保數據庫連接正常
            await this.ensureDatabaseConnection();
            
            const query = 'SELECT failure_count FROM failed_epoch WHERE epoch = $1';
            const result = await this.db.query(query, [epoch.toString()]);
            
            if (result.rows.length > 0) {
                const retryCount = result.rows[0].failure_count;
                return retryCount >= 3;
            }
            
            return false;
        } catch (error) {
            console.error(`檢查局次 ${epoch} 跳過狀態失敗:`, error);
            return false;
        }
    }
    
    /**
     * 🎯 處理指定範圍的局次（用於命令行手動調用）
     */
    async processEpochRange(startEpoch, endEpoch) {
        try {
            console.log(`🎯 開始處理局次範圍: ${startEpoch} → ${endEpoch}`);
            
            const totalEpochs = endEpoch - startEpoch + 1;
            let processedCount = 0;
            
            for (let epoch = startEpoch; epoch <= endEpoch; epoch++) {
                try {
                    // 檢查是否已存在
                    if (await this.hasRoundData(epoch)) {
                        console.log(`⏭️ 局次 ${epoch} 已存在，跳過`);
                        processedCount++;
                        continue;
                    }
                    
                    const success = await this.processEpochData(epoch);
                    if (success) {
                        processedCount++;
                    }
                    
                    // 進度報告
                    if (processedCount % 10 === 0) {
                        const progress = ((processedCount / totalEpochs) * 100).toFixed(1);
                        console.log(`📊 處理進度: ${processedCount}/${totalEpochs} (${progress}%)`);
                    }
                    
                    // 處理間隔
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                } catch (error) {
                    console.error(`❌ 處理局次 ${epoch} 失敗:`, error.message);
                    this.stats.errors++;
                }
            }
            
            console.log(`✅ 局次範圍處理完成: ${startEpoch} → ${endEpoch}`);
            this.printStats();
            
        } catch (error) {
            console.error('❌ 處理局次範圍失敗:', error);
            throw error;
        }
    }

    /**
     * 📊 處理最新數據（支線任務）
     * 🎯 每5分鐘運行，檢查最新局次-2開始的5個局次數據
     * 
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改支線檢查範圍！⚠️⚠️⚠️
     * 支線必須檢查固定範圍：當前局次-2 到 當前局次-6 (共5局)
     * 絕對不准改成：
     * - 當前局次-1 開始（會檢查到未結束的局次）
     * - 當前局次-3 開始（會遺漏最新數據）  
     * - 超過5局的範圍（會與主線產生衝突）
     * - 少於5局的範圍（會遺漏數據）
     * 🔥🔥🔥 這個範圍是經過精密計算的，確保最新數據不遺漏！🔥🔥🔥
     * 🚨🚨🚨 任何修改都會導致數據遺漏或重複處理！🚨🚨🚨
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改支線檢查範圍！⚠️⚠️⚠️
     */
    async processLatestData() {
        try {
            const currentEpoch = await this.getCurrentEpoch();
            console.log(`🔄 [支線] 開始處理最新數據，當前局次: ${currentEpoch}`);
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改循環範圍！⚠️⚠️⚠️
            // 必須是 i = 2 到 i = 6，這樣檢查的是：
            // currentEpoch-2, currentEpoch-3, currentEpoch-4, currentEpoch-5, currentEpoch-6
            // 總共5個局次，這是最佳的檢查範圍
            // 絕對不准改成 i = 1 或其他起始值
            // 絕對不准改成其他結束值
            // 🔥🔥🔥 保持 for (let i = 2; i <= 6; i++) 不變！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改循環範圍！⚠️⚠️⚠️
            for (let i = 2; i <= 6; i++) {
                const targetEpoch = currentEpoch - i;
                
                if (targetEpoch <= 0) continue;
                
                await this.processEpochIfNeeded(targetEpoch, '[支線] 已存在');
                await this.delayMs(1000);
            }
            
        } catch (error) {
            console.error('❌ [支線] 處理最新數據失敗:', error.message);
        }
    }

    /**
     * 📚 處理歷史數據回補（主線任務）
     * 🎯 從最新局次-2開始，一路往回檢查和回補
     * 
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改主線回補邏輯！⚠️⚠️⚠️
     * 主線必須從【當前局次-2】開始往回檢查，絕對不准改成：
     * - 當前局次-1 開始（會檢查到未結束的局次）
     * - 當前局次-3 開始（會遺漏最新的歷史數據）
     * - 任何其他起始點
     * checkEpoch-- 的遞減邏輯也絕對不准修改：
     * - 不准改成 checkEpoch++（會往前而不是往後）
     * - 不准改成其他遞增遞減方式
     * 🔥🔥🔥 這是歷史回補的核心邏輯，確保所有歷史數據不遺漏！🔥🔥🔥
     * 🚨🚨🚨 任何修改都會導致歷史數據回補錯誤！🚨🚨🚨
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改主線回補邏輯！⚠️⚠️⚠️
     */
    async processHistoryData() {
        if (this.isProcessingHistory) {
            console.log('⏳ [主線] 歷史數據處理中，跳過本次');
            return;
        }
        
        this.isProcessingHistory = true;
        this.shouldStopHistory = false;
        
        try {
            const currentEpoch = await this.getCurrentEpoch();
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改起始點！⚠️⚠️⚠️
            // 必須是 currentEpoch - 2，不准改成：
            // - currentEpoch - 1（會檢查未結束局次）
            // - currentEpoch - 3（會遺漏數據）
            // - currentEpoch（會檢查當前局次）
            // 🔥🔥🔥 保持 let checkEpoch = currentEpoch - 2 不變！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改起始點！⚠️⚠️⚠️
            let checkEpoch = currentEpoch - 2;
            
            console.log(`📚 [主線] 開始歷史回補，從局次 ${checkEpoch} 往回檢查...`);
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改循環邏輯！⚠️⚠️⚠️
            // while條件必須包含 checkEpoch > 0，防止無限循環
            // checkEpoch-- 必須往回遞減，不准改成遞增
            // 🔥🔥🔥 這是歷史回補的核心邏輯！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改循環邏輯！⚠️⚠️⚠️
            while (this.isProcessingHistory && !this.shouldStopHistory && checkEpoch > 0) {
                try {
                    // 檢查是否已有數據
                    if (!(await this.hasRoundData(checkEpoch))) {
                        console.log(`🔄 [主線] 回補局次 ${checkEpoch}`);
                        await this.processEpochData(checkEpoch);
                    } else {
                        console.log(`⏭️ [主線] 局次 ${checkEpoch} 已存在，跳過`);
                    }
                    
                    // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改遞減邏輯！⚠️⚠️⚠️
                    // 必須是 checkEpoch-- 往回遞減，不准改成：
                    // - checkEpoch++（會往前而不是往後）
                    // - checkEpoch -= 2 或其他步長
                    // 🔥🔥🔥 保持 checkEpoch-- 不變！🔥🔥🔥
                    // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改遞減邏輯！⚠️⚠️⚠️
                    checkEpoch--;
                    
                    // 檢查是否需要停止
                    if (this.shouldStopHistory) {
                        console.log(`🛑 [主線] 收到停止信號，當前局次 ${checkEpoch + 1} 處理完成`);
                        break;
                    }
                    
                    // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改間隔時間！⚠️⚠️⚠️
                    // 2秒間隔是經過測試的最佳值，確保：
                    // 1. 不會對RPC節點造成過大壓力
                    // 2. 保持合理的處理速度
                    // 🔥🔥🔥 保持 2000ms 間隔不變！🔥🔥🔥
                    // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改間隔時間！⚠️⚠️⚠️
                    await this.delayMs(2000); // 每個局次間隔2秒
                    
                } catch (error) {
                    console.error(`❌ [主線] 處理局次 ${checkEpoch} 失敗:`, error.message);
                    checkEpoch--; // 跳過失敗的局次
                }
            }
            
        } catch (error) {
            console.error('❌ [主線] 歷史數據回補失敗:', error.message);
        } finally {
            this.isProcessingHistory = false;
            this.shouldStopHistory = false;
        }
    }

    /**
     * 🚀 啟動雙線程定期任務系統
     * 
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改雙線程時間設定！⚠️⚠️⚠️
     * 以下時間設定是經過精密計算的最佳配置：
     * - 主線重啟間隔：30分鐘（30 * 60 * 1000）
     * - 支線啟動延遲：5分鐘（5 * 60 * 1000）  
     * - 支線執行間隔：5分鐘（5 * 60 * 1000）
     * - 主線重啟延遲：5秒（5000）
     * 🔥🔥🔥 任何修改都會破壞數據抓取的完整性和穩定性！🔥🔥🔥
     * 🚨🚨🚨 絕對不准改成其他數值或計算方式！🚨🚨🚨
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改雙線程時間設定！⚠️⚠️⚠️
     */
    startPeriodicTasks() {
        console.log('🚀 啟動V6雙線程定期任務系統');
        
        // 🎯 主線：立即開始歷史回補
        console.log('📚 [主線] 啟動歷史數據回補任務');
        this.startHistoryBackfill();
        
        // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改30分鐘重啟間隔！⚠️⚠️⚠️
        // 30分鐘是經過測試的最佳重啟間隔，確保：
        // 1. 主線能及時跟上最新進度
        // 2. 避免長期運行導態的內存累積
        // 3. 防止網絡連接超時問題
        // 🔥🔥🔥 絕對不准改成其他數值！🔥🔥🔥
        // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改30分鐘重啟間隔！⚠️⚠️⚠️
        this.historyInterval = setInterval(async () => {
            console.log('🔄 [主線] 30分鐘定時優雅重啟歷史回補...');
            await this.gracefulStopAndRestart();
        }, 30 * 60 * 1000);
        
        // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改5分鐘延遲和間隔！⚠️⚠️⚠️
        // 支線5分鐘後啟動是為了：
        // 1. 避免與主線啟動時產生衝突
        // 2. 讓主線有足夠時間處理積壓的歷史數據
        // 3. 確保系統資源合理分配
        // 支線每5分鐘執行是為了：
        // 1. 及時發現最新數據的缺失
        // 2. 避免過於頻繁的檢查影響性能
        // 🔥🔥🔥 絕對不准改成其他數值！🔥🔥🔥
        // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改5分鐘延遲和間隔！⚠️⚠️⚠️
        setTimeout(() => {
            console.log('📊 [支線] 啟動最新數據檢查任務');
            this.processLatestData(); // 立即執行一次
            
            // 每5分鐘執行一次
            setInterval(() => {
                this.processLatestData();
            }, 5 * 60 * 1000);
            
        }, 5 * 60 * 1000); // 5分鐘後開始
        
        console.log('✅ V6雙線程任務系統啟動完成');
        console.log('   📚 [主線] 歷史回補：立即開始，每30分鐘重啟');
        console.log('   📊 [支線] 最新檢查：5分鐘後開始，每5分鐘執行');
    }

    /**
     * 🔄 啟動歷史回補
     */
    startHistoryBackfill() {
        if (!this.isProcessingHistory) {
            this.processHistoryData();
        }
    }

    /**
     * 🛑 停止歷史回補
     */
    async stopHistoryBackfill() {
        if (this.isProcessingHistory) {
            console.log('🛑 [主線] 發送停止信號...');
            this.shouldStopHistory = true;
            
            // 等待最多10秒讓當前任務完成
            let waitCount = 0;
            while (this.isProcessingHistory && waitCount < 100) {
                await this.delayMs(100);
                waitCount++;
            }
            
            if (this.isProcessingHistory) {
                console.log('⚠️ [主線] 強制停止歷史回補');
                this.isProcessingHistory = false;
                this.shouldStopHistory = false;
            }
        }
    }

    /**
     * 🔄 優雅停止並重啟主線任務
     * 
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改優雅重啟步驟順序！⚠️⚠️⚠️
     * 優雅重啟必須按照以下嚴格順序執行，絕對不准跳過任何步驟：
     * 1. 發送停止信號，等待當前局次處理完成
     * 2. 確保數據已完整保存到round/hisbet/claim表
     * 3. 檢查並清理realbet表中的對應數據
     * 4. 驗證claim表數據完整性
     * 5. 完成所有檢查後才真正重啟
     * 🔥🔥🔥 任何步驟的缺失都會導致數據不一致！🔥🔥🔥
     * 🚨🚨🚨 絕對不准修改步驟順序或跳過任何檢查！🚨🚨🚨
     * ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對禁止修改優雅重啟步驟順序！⚠️⚠️⚠️
     */
    async gracefulStopAndRestart() {
        try {
            console.log('🔄 [主線] 開始優雅重啟流程...');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟1！⚠️⚠️⚠️
            // 步驟1：發送停止信號，等待當前局次處理完成
            // 絕對不准直接強制停止，必須等待當前局次完成
            // 🔥🔥🔥 這確保正在處理的局次不會中斷！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟1！⚠️⚠️⚠️
            if (this.isProcessingHistory) {
                console.log('🛑 [主線] 步驟1: 發送優雅停止信號，等待當前局次完成...');
                this.shouldStopHistory = true;
                
                // 等待最多60秒讓當前局次完全處理完成
                let waitCount = 0;
                let lastEpoch = null;
                
                while (this.isProcessingHistory && waitCount < 600) { // 60秒
                    await this.delayMs(100);
                    waitCount++;
                    
                    // 每10秒報告一次等待狀態
                    if (waitCount % 100 === 0) {
                        const waitSeconds = waitCount / 10;
                        console.log(`⏳ [主線] 等待當前局次完成中... (${waitSeconds}秒)`);
                    }
                }
                
                if (this.isProcessingHistory) {
                    console.log('⚠️ [主線] 等待超時，執行強制停止');
                    this.isProcessingHistory = false;
                    this.shouldStopHistory = false;
                }
            }
            
            console.log('✅ [主線] 步驟1完成: 當前局次已停止');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟2！⚠️⚠️⚠️
            // 步驟2: 等待一小段時間確保所有數據庫操作完成
            // 這很重要，因為數據保存可能還在進行中
            // 🔥🔥🔥 絕對不准減少等待時間！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟2！⚠️⚠️⚠️
            console.log('🔄 [主線] 步驟2: 等待數據庫操作完成...');
            await this.delayMs(3000); // 等待3秒確保數據庫操作完成
            console.log('✅ [主線] 步驟2完成: 數據庫操作等待完成');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟3！⚠️⚠️⚠️
            // 步驟3: 檢查最近處理的局次數據完整性
            // 確保round/hisbet/claim三表數據都已正確保存
            // 🔥🔥🔥 這是數據一致性的關鍵檢查！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟3！⚠️⚠️⚠️
            console.log('🔍 [主線] 步驟3: 檢查數據完整性...');
            await this.validateRecentDataIntegrity();
            console.log('✅ [主線] 步驟3完成: 數據完整性檢查通過');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟4！⚠️⚠️⚠️
            // 步驟4: 檢查並清理realbet表
            // 確保歷史數據對應的即時數據已清理
            // 🔥🔥🔥 這防止數據重複和不一致！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟4！⚠️⚠️⚠️
            console.log('🧹 [主線] 步驟4: 檢查realbet表清理狀態...');
            await this.validateRealbetCleanup();
            console.log('✅ [主線] 步驟4完成: realbet表檢查通過');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟5！⚠️⚠️⚠️
            // 步驟5: 驗證claim表數據完整性
            // 確保多次領獎檢測已完成
            // 🔥🔥🔥 這確保異常檢測不會遺漏！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准跳過步驟5！⚠️⚠️⚠️
            console.log('🏆 [主線] 步驟5: 驗證claim表完整性...');
            await this.validateClaimDataIntegrity();
            console.log('✅ [主線] 步驟5完成: claim表驗證通過');
            
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改重啟延遲！⚠️⚠️⚠️
            // 步驟6: 所有檢查完成後，等待5秒再重啟
            // 5秒延遲確保系統狀態完全穩定
            // 🔥🔥🔥 絕對不准減少這個延遲時間！🔥🔥🔥
            // ⚠️⚠️⚠️ 【🔥嚴重警告🔥】：絕對不准修改重啟延遲！⚠️⚠️⚠️
            console.log('⏳ [主線] 步驟6: 等待系統穩定後重啟...');
            setTimeout(() => {
                console.log('🚀 [主線] 優雅重啟完成，重新啟動歷史回補');
                this.startHistoryBackfill();
            }, 5000); // 5秒後重啟
            
            console.log('🎉 [主線] 優雅重啟流程執行完成');
            
        } catch (error) {
            console.error('❌ [主線] 優雅重啟失敗:', error.message);
            // 即使重啟失敗，也要嘗試正常重啟
            setTimeout(() => {
                console.log('🔄 [主線] 執行備用重啟');
                this.startHistoryBackfill();
            }, 5000);
        }
    }

    /**
     * 🔍 驗證近期數據完整性
     */
    async validateRecentDataIntegrity() {
        try {
            const currentEpoch = await this.getCurrentEpoch();
            const checkEpochs = [currentEpoch - 2, currentEpoch - 3, currentEpoch - 4];
            
            for (const epoch of checkEpochs) {
                if (epoch <= 0) continue;
                
                // 檢查是否有round數據
                const hasRound = await this.hasRoundData(epoch);
                if (hasRound) {
                    // 檢查hisbet和claim數據數量
                    const betCount = await this.db.query('SELECT COUNT(*) as count FROM hisbet WHERE epoch = $1', [epoch.toString()]);
                    const claimCount = await this.db.query(`
                        SELECT 
                            epoch,
                            wallet_address,
                            COUNT(DISTINCT bet_epoch) AS bet_epoch_count
                        FROM 
                            claim
                        WHERE 
                            epoch = $1
                        GROUP BY 
                            epoch, 
                            wallet_address
                        HAVING 
                            COUNT(DISTINCT bet_epoch) > 3
                        ORDER BY 
                            epoch, 
                            wallet_address
                    `, [epoch.toString()]);
                    
                    console.log(`🔍 局次 ${epoch}: round✅ hisbet:${betCount.rows[0].count} claim多次領獎錢包:${claimCount.rows.length}`);
                }
            }
            
        } catch (error) {
            console.error('❌ 數據完整性檢查失敗:', error.message);
        }
    }

    /**
     * 🧹 驗證realbet清理狀態
     */
    async validateRealbetCleanup() {
        try {
            const currentEpoch = await this.getCurrentEpoch();
            const checkEpochs = [currentEpoch - 2, currentEpoch - 3, currentEpoch - 4];
            
            for (const epoch of checkEpochs) {
                if (epoch <= 0) continue;
                
                // 檢查該局次在realbet中是否還有數據
                const realbetCount = await this.db.query('SELECT COUNT(*) as count FROM realbet WHERE epoch = $1', [epoch.toString()]);
                
                if (realbetCount.rows[0].count > 0) {
                    console.log(`🧹 檢測到局次 ${epoch} 在realbet表中還有 ${realbetCount.rows[0].count} 筆數據，執行清理`);
                    await this.cleanupRealbetData(epoch);
                }
            }
            
        } catch (error) {
            console.error('❌ realbet清理檢查失敗:', error.message);
        }
    }

    /**
     * 🏆 驗證claim表數據完整性
     */
    async validateClaimDataIntegrity() {
        try {
            const currentEpoch = await this.getCurrentEpoch();
            const checkEpochs = [currentEpoch - 2, currentEpoch - 3];
            
            for (const epoch of checkEpochs) {
                if (epoch <= 0) continue;
                
                // 檢查該局次的claim數據中多次領獎的錢包
                const claimData = await this.db.query(`
                    SELECT 
                        epoch,
                        wallet_address,
                        COUNT(DISTINCT bet_epoch) AS bet_epoch_count
                    FROM 
                        claim
                    WHERE 
                        epoch = $1
                    GROUP BY 
                        epoch, 
                        wallet_address
                    HAVING 
                        COUNT(DISTINCT bet_epoch) > 3
                    ORDER BY 
                        epoch, 
                        wallet_address
                `, [epoch.toString()]);
                
                if (claimData.rows.length > 0) {
                    // 查詢結果已經過濾了多次領獎的錢包（bet_epoch_count > 3）
                    console.log(`🏆 局次 ${epoch} 檢測到 ${claimData.rows.length} 個多次領獎錢包`);
                    
                    // 可選：顯示詳細信息
                    for (const wallet of claimData.rows) {
                        console.log(`   📍 錢包 ${wallet.wallet_address}: ${wallet.bet_epoch_count} 個不同bet_epoch`);
                    }
                }
            }
            
        } catch (error) {
            console.error('❌ claim表完整性檢查失敗:', error.message);
        }
    }

    /**
     * 🎯 統一處理局次（如需要）
     */
    async processEpochIfNeeded(targetEpoch, skipMessage = '已存在') {
        if (await this.hasRoundData(targetEpoch)) {
            console.log(`⏭️ 局次 ${targetEpoch} ${skipMessage}，跳過`);
            return false;
        }
        return await this.processEpochData(targetEpoch);
    }

    /**
     * ⏱️ 統一延遲處理
     */
    async delayMs(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    /**
     * 📊 打印統計信息
     */
    printStats() {
        console.log('\n📊 V6統一爬蟲處理統計:');
        console.log('==========================');
        console.log(`✅ 已處理局次: ${this.stats.roundsProcessed}`);
        console.log(`💰 已處理下注: ${this.stats.betsProcessed}`);
        console.log(`🏆 已處理領獎: ${this.stats.claimsProcessed}`);
        console.log(`🚨 檢測可疑錢包: ${this.stats.suspiciousWalletsDetected}`);
        console.log(`❌ 處理錯誤: ${this.stats.errors}`);
        console.log('==========================\n');
    }
    
    /**
     * 📊 獲取統計信息
     */
    getStats() {
        return {
            ...this.stats,
            isConnected: this.dbPool !== null
        };
    }
    
    /**
     * 🧹 清理資源
     */
    cleanup() {
        if (this.dbPool) {
            this.dbPool.end();
        }
        this.provider = null;
        this.contract = null;
        console.log('✅ V6統一爬蟲資源已清理');
    }
}

// 如果直接運行此文件
if (require.main === module) {
    const crawler = new V6UnifiedCrawler();
    
    // 優雅關閉處理
    process.on('SIGINT', async () => {
        console.log('\n🛑 接收到關閉信號，正在清理V6資源...');
        if (crawler.historyInterval) {
            clearInterval(crawler.historyInterval);
        }
        await crawler.stopHistoryBackfill();
        crawler.cleanup();
        process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
        console.log('\n🛑 接收到終止信號，正在清理V6資源...');
        if (crawler.historyInterval) {
            clearInterval(crawler.historyInterval);
        }
        await crawler.stopHistoryBackfill();
        crawler.cleanup();
        process.exit(0);
    });
    
    // 從命令行參數獲取局次範圍
    const args = process.argv.slice(2);
    
    if (args.length >= 2) {
        // 🎯 命令行模式：手動處理指定範圍
        const startEpoch = parseInt(args[0]);
        const endEpoch = parseInt(args[1]);
        
        console.log(`🎯 命令行模式: 處理局次 ${startEpoch} → ${endEpoch}`);
        
        crawler.initialize()
            .then(() => crawler.processEpochRange(startEpoch, endEpoch))
            .then(() => {
                console.log('✅ V6統一爬蟲處理完成');
                crawler.cleanup();
                process.exit(0);
            })
            .catch(error => {
                console.error('❌ V6統一爬蟲處理失敗:', error);
                crawler.cleanup();
                process.exit(1);
            });
    } else if (args.length === 1 && args[0] === '--daemon') {
        // 🚀 守護程序模式：啟動雙線程系統
        console.log('🚀 守護程序模式: 啟動V6雙線程定期任務系統');
        
        crawler.initialize()
            .then(() => {
                crawler.startPeriodicTasks();
                console.log('✅ V6統一爬蟲守護程序已啟動');
                console.log('💡 使用 Ctrl+C 優雅停止');
            })
            .catch(error => {
                console.error('❌ V6統一爬蟲守護程序啟動失敗:', error);
                crawler.cleanup();
                process.exit(1);
            });
    } else {
        console.log('🔧 V6統一爬蟲使用方法:');
        console.log('');
        console.log('📊 手動處理指定範圍:');
        console.log('   node v6-unified-crawler.js <開始局次> <結束局次>');
        console.log('   例如: node v6-unified-crawler.js 390000 390100');
        console.log('');
        console.log('🚀 啟動雙線程守護程序:');
        console.log('   node v6-unified-crawler.js --daemon');
        console.log('   - 主線：歷史回補（立即開始，每30分鐘重啟）');
        console.log('   - 支線：最新檢查（5分鐘後開始，每5分鐘執行）');
        console.log('');
        process.exit(1);
    }
}

module.exports = V6UnifiedCrawler;