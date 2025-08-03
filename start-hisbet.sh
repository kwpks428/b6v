#!/bin/bash

echo "🚀 啟動V6歷史數據爬蟲系統..."

# 檢查Node.js版本
node_version=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$node_version" -lt 18 ]; then
    echo "❌ 需要Node.js 18或更高版本，當前版本: $(node -v)"
    exit 1
fi

# 檢查依賴是否已安裝
if [ ! -d "node_modules" ]; then
    echo "📦 安裝依賴包..."
    npm install
fi

# 設置環境變數
export V6_RPC_URL="https://lb.drpc.org/bsc/Ahc3I-33qkfGuwXSahR3XfPDRmd6WZsR8JbErqRhf0fE"

# 如果沒有設置數據庫URL，使用默認值
if [ -z "$V6_DATABASE_URL" ] && [ -z "$DATABASE_URL" ]; then
    echo "⚠️ 使用默認數據庫連接（Neon）"
    export V6_DATABASE_URL="postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
fi

echo "📊 環境配置:"
echo "   RPC_URL: $V6_RPC_URL"
echo "   DATABASE_URL: ${V6_DATABASE_URL:0:50}..."

echo ""
echo "🔄 啟動雙線程守護程序模式..."
echo "   📚 主線：歷史回補（立即開始，每30分鐘重啟）"
echo "   📊 支線：最新檢查（5分鐘後開始，每5分鐘執行）"
echo ""
echo "💡 使用 Ctrl+C 優雅停止"
echo ""

# 啟動守護程序
node v6-unified-crawler.js --daemon