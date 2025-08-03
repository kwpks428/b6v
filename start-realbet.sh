#!/bin/bash

echo "🚀 啟動V6即時數據監聽器..."

# 檢查Node.js版本
node_version=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$node_version" -lt 18 ]; then
    echo "❌ 需要Node.js 18或更高版本，當前版本: $(node -v)"
    exit 1
fi

# 檢查依賴是否已安裝
if [ ! -d "node_modules" ]; then
    echo "📦 安裝依賴包..."
    # 使用realbet專用的package.json
    cp realbet-package.json package.json
    npm install
fi

# 設置環境變數
export V6_RPC_URL="wss://lb.drpc.org/bsc/Ahc3I-33qkfGuwXSahR3XfPDRmd6WZsR8JbErqRhf0fE"

# 如果沒有設置數據庫URL，使用默認值
if [ -z "$V6_DATABASE_URL" ] && [ -z "$DATABASE_URL" ]; then
    echo "⚠️ 使用默認數據庫連接（Neon）"
    export V6_DATABASE_URL="postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
fi

echo "📊 環境配置:"
echo "   WebSocket RPC: $V6_RPC_URL"
echo "   DATABASE_URL: ${V6_DATABASE_URL:0:50}..."
echo "   WebSocket服務器端口: 3010"

echo ""
echo "🔄 啟動即時數據監聽..."
echo "   📡 監聽PancakeSwap事件（BetBull/BetBear/StartRound）"
echo "   🚨 可疑錢包實時檢測"
echo "   📤 WebSocket服務器 ws://localhost:3010/ws"
echo ""
echo "💡 使用 Ctrl+C 優雅停止"
echo ""

# 啟動即時監聽器
node realtime-listener.js