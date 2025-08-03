# V6 Data Extraction System - 部署和測試指南

## 📋 系統概述

V6系統包含兩個核心服務：
- **hisbet**: 歷史數據爬蟲（v6-unified-crawler.js）
- **realbet**: 即時數據監聽器（realtime-listener.js）

## 🚀 快速啟動

### 1. 安裝依賴

```bash
# 進入項目目錄
cd /Users/kw/Desktop/pkskw/v4bets/b6v

# 安裝歷史數據爬蟲依賴
npm install ethers@^6.15.0 pg@^8.11.3

# 如果要運行即時監聽器，額外安裝
npm install ws@^8.20.0
```

### 2. 環境變數設置

```bash
# 可選：設置自定義數據庫URL
export V6_DATABASE_URL="your_postgresql_connection_string"

# 可選：設置自定義RPC節點
export V6_RPC_URL="your_rpc_url"
```

### 3. 啟動服務

#### 歷史數據爬蟲 (hisbet)
```bash
# 使用啟動腳本（推薦）
./start-hisbet.sh

# 或者直接啟動
node v6-unified-crawler.js --daemon

# 手動處理指定範圍
node v6-unified-crawler.js 395000 395100
```

#### 即時數據監聽器 (realbet)
```bash
# 使用啟動腳本（推薦）
./start-realbet.sh

# 或者直接啟動
node realtime-listener.js
```

## 🧪 測試功能

### 基本功能測試
```bash
# 測試系統基本功能
node test-crawler.js

# 測試範圍處理功能
node test-crawler.js --range
```

### 功能驗證檢查項目

#### hisbet（歷史數據爬蟲）
- [ ] 數據庫連接成功
- [ ] 獲取當前局次
- [ ] 處理單個局次數據
- [ ] 雙線程系統運行（主線+支線）
- [ ] 數據完整性驗證
- [ ] 自動清理和優雅重啟

#### realbet（即時數據監聽器）
- [ ] WebSocket連接區塊鏈
- [ ] 監聽下注事件（BetBull/BetBear）
- [ ] 可疑錢包檢測
- [ ] WebSocket服務器啟動（端口3010）
- [ ] 數據推送給前端
- [ ] 重複下注檢測

## 📊 監控和日誌

### 系統狀態檢查
```bash
# 檢查hisbet運行狀態
curl -s http://localhost:3008/api/health 2>/dev/null || echo "hisbet not running"

# 檢查realbet WebSocket服務器
curl -s http://localhost:3010 2>/dev/null || echo "realbet not running"

# 檢查數據庫連接
psql $V6_DATABASE_URL -c "SELECT NOW();"
```

### 日誌監控
- 🚀 啟動信息：服務初始化成功
- ✅ 數據處理：局次處理成功/失敗
- 📊 統計信息：處理的局次/下注/領獎數量
- 🚨 警告信息：可疑錢包檢測
- ❌ 錯誤信息：連接失敗、數據錯誤

## 🛠️ 故障排除

### 常見問題

1. **數據庫連接失敗**
   ```bash
   # 檢查環境變數
   echo $V6_DATABASE_URL
   
   # 測試連接
   psql $V6_DATABASE_URL -c "SELECT 1;"
   ```

2. **區塊鏈連接失敗**
   ```bash
   # 檢查RPC節點
   curl -X POST $V6_RPC_URL \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
   ```

3. **端口衝突**
   ```bash
   # 檢查端口使用情況
   lsof -i :3008  # hisbet前端服務器
   lsof -i :3010  # realbet WebSocket服務器
   ```

### 重啟服務
```bash
# 優雅停止（Ctrl+C）然後重新啟動
./start-hisbet.sh
./start-realbet.sh
```

## 📈 性能優化

### 歷史數據爬蟲
- 每30分鐘自動重啟主線任務
- 每5分鐘檢查最新數據
- 速率限制：100請求/秒
- 自動錯誤重試機制

### 即時數據監聽器
- WebSocket自動重連
- 內存中重複檢測
- 可疑錢包實時標記
- 舊數據自動清理

## 🚧 部署注意事項

1. **Node.js版本**: 需要18.0.0或更高
2. **內存要求**: 建議至少512MB
3. **網絡要求**: 穩定的區塊鏈RPC連接
4. **數據庫**: PostgreSQL 12+
5. **時區設置**: 系統會自動設置為台北時間

## 📱 前端連接

### WebSocket連接（realbet）
```javascript
const ws = new WebSocket('ws://localhost:3010/ws');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'new_bet') {
    console.log('新下注:', data);
  }
};
```

### HTTP API（hisbet）
```bash
# 獲取系統健康狀態
curl http://localhost:3008/api/health

# 獲取預測數據
curl http://localhost:3008/api/prediction
```