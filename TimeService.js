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
 * V6系統統一時間處理服務 (Railway優化版本 - 無外部依賴)
 * 
 * 🎯 核心原則：
 * 1. 所有時間格式統一為台北時間 YYYY-MM-DD HH:mm:ss
 * 2. 不包含毫秒、時區標記等額外信息
 * 3. 靜態方法設計，無需實例化
 * 4. 嚴格輸入驗證，防止格式混亂
 * 5. 使用Node.js原生API，無外部依賴
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
    
    static get TIMEZONE_OFFSET() {
        return 8 * 60; // 台北時間 UTC+8，以分鐘為單位
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
            let date;
            
            // 處理Unix時間戳（數字）
            if (typeof input === 'number') {
                // 自動判斷是秒還是毫秒
                date = input > 1e10 ? new Date(input) : new Date(input * 1000);
            }
            // 處理Date對象
            else if (input instanceof Date) {
                date = new Date(input);
            }
            // 處理字符串
            else {
                date = new Date(input);
            }
            
            // 驗證日期對象有效性
            if (isNaN(date.getTime())) {
                throw new Error(`無效的時間格式: ${input}`);
            }
            
            // 🔥 核心轉換：直接使用toLocaleString轉換為台北時間
            const result = date.toLocaleString('zh-TW', {
                timeZone: 'Asia/Taipei',
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false
            }).replace(/\//g, '-').replace(',', '');
            
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
        return this.formatTaipeiTime(new Date());
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
        
        return this.formatTaipeiTime(unixTimestamp);
    }
    
    /**
     * 📅 創建台北時間的Date對象
     * 
     * @param {Date} date - 輸入日期，默認為當前時間
     * @returns {Date} 台北時區的Date對象
     */
    static createTaipeiDate(date = new Date()) {
        return this.toTaipeiTime(new Date(date));
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
        const parts = timeString.split(' ');
        const datePart = parts[0].split('-');
        const timePart = parts[1].split(':');
        
        const year = parseInt(datePart[0]);
        const month = parseInt(datePart[1]);
        const day = parseInt(datePart[2]);
        const hour = parseInt(timePart[0]);
        const minute = parseInt(timePart[1]);
        const second = parseInt(timePart[2]);
        
        // 驗證範圍
        if (year < 1970 || year > 9999) return false;
        if (month < 1 || month > 12) return false;
        if (day < 1 || day > 31) return false;
        if (hour < 0 || hour > 23) return false;
        if (minute < 0 || minute > 59) return false;
        if (second < 0 || second > 59) return false;
        
        // 創建日期驗證
        const testDate = new Date(year, month - 1, day, hour, minute, second);
        return testDate.getFullYear() === year &&
               testDate.getMonth() === month - 1 &&
               testDate.getDate() === day;
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
            let date;
            
            if (typeof input === 'number') {
                date = input > 1e10 ? new Date(input) : new Date(input * 1000);
            } else {
                date = new Date(input);
            }
            
            const isValid = !isNaN(date.getTime());
            
            return {
                input: input,
                inputType: typeof input,
                formatted: formatted,
                unix: Math.floor(date.getTime() / 1000),
                utc: date.toISOString(),
                taipei: formatted,
                isValid: isValid
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
    
    /**
     * 🔧 內部方法：轉換為台北時間
     * 
     * @param {Date} date - 輸入的Date對象
     * @returns {Date} 台北時間的Date對象
     */
    static toTaipeiTime(date) {
        // 創建一個新的Date對象，加上台北時間的偏移量(UTC+8)
        const taipeiTime = new Date(date.getTime() + (8 * 60 * 60 * 1000));
        return taipeiTime;
    }
    
    /**
     * 🔧 內部方法：將Date對象格式化為字符串
     * 
     * @param {Date} date - Date對象
     * @returns {string} YYYY-MM-DD HH:mm:ss 格式字符串
     */
    static formatDateToString(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hour = String(date.getHours()).padStart(2, '0');
        const minute = String(date.getMinutes()).padStart(2, '0');
        const second = String(date.getSeconds()).padStart(2, '0');
        
        return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
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