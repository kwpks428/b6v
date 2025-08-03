const V6UnifiedCrawler = require('./v6-unified-crawler.js');

/**
 * 測試V6統一爬蟲的基本功能
 */
async function testCrawlerBasics() {
    console.log('🧪 開始測試V6統一爬蟲基本功能...\n');
    
    const crawler = new V6UnifiedCrawler();
    
    try {
        // 測試1: 初始化
        console.log('📋 測試1: 系統初始化');
        await crawler.initialize();
        console.log('✅ 初始化成功\n');
        
        // 測試2: 獲取當前局次
        console.log('📋 測試2: 獲取當前局次');
        const currentEpoch = await crawler.getCurrentEpoch();
        console.log(`✅ 當前局次: ${currentEpoch}\n`);
        
        // 測試3: 檢查數據庫連接
        console.log('📋 測試3: 檢查數據庫連接');
        const stats = crawler.getStats();
        console.log(`✅ 數據庫連接狀態: ${stats.isConnected ? '已連接' : '未連接'}\n`);
        
        // 測試4: 測試單個局次處理（最近的已結束局次）
        console.log('📋 測試4: 測試單個局次數據抓取');
        const testEpoch = currentEpoch - 3; // 測試3局前的數據
        console.log(`🎯 測試局次: ${testEpoch}`);
        
        // 檢查是否已存在
        const hasData = await crawler.hasRoundData(testEpoch);
        if (hasData) {
            console.log(`⚠️ 局次 ${testEpoch} 數據已存在，跳過處理`);
        } else {
            console.log(`🔄 開始處理局次 ${testEpoch}...`);
            const success = await crawler.processEpochData(testEpoch);
            console.log(`${success ? '✅' : '❌'} 局次 ${testEpoch} 處理${success ? '成功' : '失敗'}`);
        }
        
        // 測試5: 顯示統計信息
        console.log('\n📋 測試5: 系統統計信息');
        crawler.printStats();
        
        console.log('🎉 所有基本功能測試完成！');
        
    } catch (error) {
        console.error('❌ 測試失敗:', error.message);
        console.error('詳細錯誤:', error);
    } finally {
        // 清理資源
        crawler.cleanup();
        console.log('🧹 測試資源已清理');
    }
}

/**
 * 測試範圍處理功能
 */
async function testRangeProcessing() {
    console.log('\n🧪 開始測試範圍處理功能...\n');
    
    const crawler = new V6UnifiedCrawler();
    
    try {
        await crawler.initialize();
        
        const currentEpoch = await crawler.getCurrentEpoch();
        const startEpoch = currentEpoch - 10;
        const endEpoch = currentEpoch - 5;
        
        console.log(`🎯 測試範圍: ${startEpoch} → ${endEpoch} (共${endEpoch - startEpoch + 1}局)`);
        
        await crawler.processEpochRange(startEpoch, endEpoch);
        
        console.log('🎉 範圍處理測試完成！');
        
    } catch (error) {
        console.error('❌ 範圍處理測試失敗:', error.message);
    } finally {
        crawler.cleanup();
    }
}

// 主程序
async function main() {
    const args = process.argv.slice(2);
    
    if (args.includes('--range')) {
        await testRangeProcessing();
    } else {
        await testCrawlerBasics();
    }
    
    process.exit(0);
}

// 執行測試
if (require.main === module) {
    main().catch(error => {
        console.error('💥 測試程序異常:', error);
        process.exit(1);
    });
}

module.exports = { testCrawlerBasics, testRangeProcessing };