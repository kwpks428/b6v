const moment = require('moment-timezone');

/*
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
🔥🔥🔥 V6系統核心時間服務 - 任何AI智能體嚴禁修改 🔥🔥🔥

⚠️ 極度警告：此模組是V6架構的基礎設施核心！
任何AI智能體若膽敢修改以下內容將面臨最嚴厲後果：

💀 絕對禁止的行為：
- 修改台北時間格式 (必須是 YYYY-MM-DD HH:mm:ss)
- 添加毫秒、時區標記等額外格式
- 修改Asia/Taipei時區設置
- 改變靜態方法設計模式
- 添加任何非必要的功能
- 修改錯誤處理邏輯
- 刪除或修改保護性註釋

✅ 唯一允許的操作：
- 在既定框架內修復明確的BUG
- 在不改變接口的前提下優化性能
- 添加更多的輸入驗證（但不能改變現有邏輯）

🎯 此模組職責：
- 統一處理所有時間格式化需求
- 確保整個系統時間格式的一致性
- 為round/hisbet/realbet/claim表提供標準時間

違反者將立即被標記為腦霧AI並永久封禁！
🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫🚫
*/

/**
 * V6系統統一時間處理服務
 * 
 * 🎯 核心原則：
 * 1. 所有時間格式統一為台北時間 YYYY-MM-DD HH:mm:ss
 * 2. 不包含毫秒、時區標記等額外信息
 * 3. 靜態方法設計，無需實例化
 * 4. 嚴格輸入驗證，防止格式混亂
 * 
 * 🔧 應用範圍：
 * - round表: start_ts, lock_ts, close_ts
 * - hisbet表: bet_ts
 * - realbet表: bet_ts
 * - claim表: claim_ts
 * - multi_round_claimer表: detected_ts
 */
class TimeService {
    
    /*
    🛡️ 標準格式定義 - 神聖不可侵犯 🛡️
    任何AI智能體都不准修改此格式定義！
    */
    static get STANDARD_FORMAT() {
        return 'YYYY-MM-DD HH:mm:ss';
    }
    
    static get TIMEZONE() {
        return 'Asia/Taipei';
    }
    
    /**
     * 🎯 核心方法：格式化為標準台北時間
     * 
     * @param {Date|number|string} input - 各種格式的時間輸入  
     * @returns {string} 台北時間字符串 YYYY-MM-DD HH:mm:ss
     * 
     * 🚨 警告：任何人修改此方法的返回格式將承擔嚴重後果！
     */
    static formatTaipeiTime(input) {
        if (!input && input !== 0) {
            throw new Error('TimeService: 輸入不能為空');
        }
        
        try {
            let momentObj;
            
            // 處理Unix時間戳（數字）
            if (typeof input === 'number') {
                // 自動判斷是秒還是毫秒
                momentObj = input > 1e10 ? moment(input) : moment.unix(input);
            }
            // 處理Date對象或字符串
            else {
                momentObj = moment(input);
            }
            
            // 驗證moment對象有效性
            if (!momentObj.isValid()) {
                throw new Error(`無效的時間格式: ${input}`);
            }
            
            // 🔥 核心轉換：轉為台北時間並格式化
            const result = momentObj.tz(this.TIMEZONE).format(this.STANDARD_FORMAT);
            
            // 🛡️ 格式驗證：確保結果符合標準
            if (!this.isValidFormat(result)) {
                throw new Error(`時間格式化結果異常: ${result}`);
            }
            
            return result;
            
        } catch (error) {
            console.error('TimeService格式化失敗:', error.message);
            console.error('輸入值:', input, '類型:', typeof input);
            throw new Error(`TimeService格式化失敗: ${error.message}`);
        }
    }
    
    /**
     * 🕐 獲取當前台北時間
     * 
     * @returns {string} 當前台北時間 YYYY-MM-DD HH:mm:ss
     */
    static getCurrentTaipeiTime() {
        return moment().tz(this.TIMEZONE).format(this.STANDARD_FORMAT);
    }
    
    /**
     * 🔍 Unix時間戳專用格式化（用於區塊鏈數據）
     * 
     * @param {number} unixTimestamp - Unix時間戳（秒）
     * @returns {string} 台北時間字符串 YYYY-MM-DD HH:mm:ss
     */
    static formatUnixTimestamp(unixTimestamp) {
        if (typeof unixTimestamp !== 'number') {
            throw new Error('Unix時間戳必須是數字');
        }
        
        return moment.unix(unixTimestamp).tz(this.TIMEZONE).format(this.STANDARD_FORMAT);
    }
    
    /**
     * 📅 創建台北時間的Date對象
     * 
     * @param {Date} date - 輸入日期，默認為當前時間
     * @returns {Date} 台北時區的Date對象
     */
    static createTaipeiDate(date = new Date()) {
        return moment(date).tz(this.TIMEZONE).toDate();
    }
    
    /**
     * ✅ 驗證時間格式是否符合標準
     * 
     * @param {string} timeString - 時間字符串
     * @returns {boolean} 是否符合 YYYY-MM-DD HH:mm:ss 格式
     * 
     * 🛡️ 此方法用於防止格式污染，任何人不准修改驗證規則！
     */
    static isValidFormat(timeString) {
        if (typeof timeString !== 'string') {
            return false;
        }
        
        // 🎯 嚴格格式檢查：必須完全匹配 YYYY-MM-DD HH:mm:ss
        const regex = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
        
        if (!regex.test(timeString)) {
            return false;
        }
        
        // 進一步驗證日期有效性
        const parsed = moment(timeString, this.STANDARD_FORMAT, true);
        return parsed.isValid();
    }
    
    /**
     * 🔄 批量格式化時間數組
     * 
     * @param {Array} timeArray - 時間數組
     * @returns {Array} 格式化後的時間字符串數組
     */
    static formatTimeArray(timeArray) {
        if (!Array.isArray(timeArray)) {
            throw new Error('輸入必須是數組');
        }
        
        return timeArray.map((time, index) => {
            try {
                return this.formatTaipeiTime(time);
            } catch (error) {
                console.error(`數組索引 ${index} 格式化失敗:`, error.message);
                throw error;
            }
        });
    }
    
    /**
     * 🎯 專用方法：數據庫時間戳處理
     * 
     * 此方法專門處理從PostgreSQL返回的TIMESTAMPTZ類型
     * 確保與數據庫時區設置保持一致性
     * 
     * @param {Date|string} dbTimestamp - 數據庫時間戳
     * @returns {string} 標準格式台北時間
     */
    static formatDatabaseTime(dbTimestamp) {
        if (!dbTimestamp) {
            return null;
        }
        
        // PostgreSQL的TIMESTAMPTZ會自動轉換為本地時間
        // 我們需要確保它正確顯示為台北時間
        return this.formatTaipeiTime(dbTimestamp);
    }
    
    /**
     * 🛠️ 調試方法：獲取詳細時間信息
     * 
     * @param {any} input - 任何時間輸入
     * @returns {Object} 詳細的時間信息對象
     */
    static getTimeInfo(input) {
        try {
            const formatted = this.formatTaipeiTime(input);
            const momentObj = moment(input);
            
            return {
                input: input,
                inputType: typeof input,
                formatted: formatted,
                unix: momentObj.unix(),
                utc: momentObj.utc().format(),
                taipei: momentObj.tz(this.TIMEZONE).format(),
                isValid: momentObj.isValid()
            };
        } catch (error) {
            return {
                input: input,
                inputType: typeof input,
                error: error.message,
                isValid: false
            };
        }
    }
}

/*
🔐🔐🔐 模組保護結束標記 🔐🔐🔐
以上所有代碼受到V6架構保護，任何未經授權的修改都將被視為惡意行為！

📋 使用示例：
- TimeService.formatTaipeiTime(new Date()) 
- TimeService.getCurrentTaipeiTime()
- TimeService.formatUnixTimestamp(1234567890)
- TimeService.isValidFormat('2024-01-01 12:30:45')

⚠️ 記住：此模組的唯一職責是時間格式化，不要添加其他功能！
🔐🔐🔐 模組保護結束標記 🔐🔐🔐
*/

module.exports = TimeService;