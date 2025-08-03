const { Client } = require('pg');

async function fastBatchMigration() {
    const neonClient = new Client({
        connectionString: 'postgresql://neondb_owner:npg_QnreOCZz48UL@ep-wispy-meadow-a19m39a6-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require',
        ssl: { rejectUnauthorized: false }
    });

    const supabaseClient = new Client({
        host: 'aws-0-ap-southeast-1.pooler.supabase.com',
        port: 6543,
        database: 'postgres',
        user: 'postgres.bhzbpidlhmspazioirwd',
        password: '0000',
        ssl: { rejectUnauthorized: false }
    });

    try {
        await neonClient.connect();
        await supabaseClient.connect();
        console.log('✅ 數據庫連接成功');

        // 清空Supabase現有數據
        console.log('🗑️ 清空Supabase現有數據...');
        await supabaseClient.query('TRUNCATE TABLE multi_claim_wallets, claim, hisbet, round CASCADE');

        // 創建累積領獎分析表
        await supabaseClient.query(`
            CREATE TABLE IF NOT EXISTS multi_claim_wallets (
                claim_epoch BIGINT NOT NULL,
                wallet_address VARCHAR NOT NULL,
                bet_epochs_count INTEGER NOT NULL,
                total_amount NUMERIC NOT NULL,
                PRIMARY KEY(claim_epoch, wallet_address)
            )
        `);

        console.log('⚡ 開始批量遷移...');
        const startTime = Date.now();

        // 1. 批量遷移 round 表
        console.log('📊 遷移 round 表...');
        const roundData = await neonClient.query('SELECT * FROM round ORDER BY epoch');
        console.log(`   從 Neon 讀取 ${roundData.rows.length} 筆 round 記錄`);

        if (roundData.rows.length > 0) {
            // 構建批量插入SQL
            const roundValues = roundData.rows.map((r, index) => {
                const baseIndex = index * 12;
                return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9}, $${baseIndex + 10}, $${baseIndex + 11}, $${baseIndex + 12})`;
            }).join(',');

            const roundParams = roundData.rows.flatMap(r => [
                r.epoch, r.start_ts, r.lock_ts, r.close_ts,
                r.lock_price, r.close_price, r.result, r.total_amount,
                r.up_amount, r.down_amount, r.up_payout, r.down_payout
            ]);

            const roundInsertSQL = `
                INSERT INTO round (epoch, start_ts, lock_ts, close_ts, lock_price, close_price, 
                                 result, total_amount, up_amount, down_amount, up_payout, down_payout)
                VALUES ${roundValues}
            `;

            await supabaseClient.query(roundInsertSQL, roundParams);
            console.log(`   ✅ round 表遷移完成: ${roundData.rows.length} 筆`);
        }

        // 2. 批量遷移 hisbet 表
        console.log('📈 遷移 hisbet 表...');
        const hisbetData = await neonClient.query('SELECT * FROM hisbet ORDER BY epoch, bet_ts');
        console.log(`   從 Neon 讀取 ${hisbetData.rows.length} 筆 hisbet 記錄`);

        if (hisbetData.rows.length > 0) {
            // 分批處理，每批1000筆
            const batchSize = 1000;
            let processed = 0;

            for (let i = 0; i < hisbetData.rows.length; i += batchSize) {
                const batch = hisbetData.rows.slice(i, i + batchSize);
                
                const hisbetValues = batch.map((_, index) => {
                    const baseIndex = index * 7;
                    return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7})`;
                }).join(',');

                const hisbetParams = batch.flatMap(h => [
                    h.epoch, h.bet_ts, h.wallet_address, h.bet_direction,
                    h.amount, h.result, h.tx_hash
                ]);

                const hisbetInsertSQL = `
                    INSERT INTO hisbet (epoch, bet_ts, wallet_address, bet_direction, amount, result, tx_hash)
                    VALUES ${hisbetValues}
                `;

                await supabaseClient.query(hisbetInsertSQL, hisbetParams);
                processed += batch.length;
                console.log(`   📥 hisbet 進度: ${processed}/${hisbetData.rows.length} (${((processed/hisbetData.rows.length)*100).toFixed(1)}%)`);
            }
            console.log(`   ✅ hisbet 表遷移完成: ${hisbetData.rows.length} 筆`);
        }

        // 3. 批量遷移 claim 表並分析多次領獎
        console.log('💰 遷移 claim 表並分析多次領獎...');
        const claimData = await neonClient.query('SELECT * FROM claim ORDER BY epoch, claim_ts');
        console.log(`   從 Neon 讀取 ${claimData.rows.length} 筆 claim 記錄`);

        if (claimData.rows.length > 0) {
            // 分批處理claim數據
            const batchSize = 1000;
            let processed = 0;

            for (let i = 0; i < claimData.rows.length; i += batchSize) {
                const batch = claimData.rows.slice(i, i + batchSize);
                
                const claimValues = batch.map((_, index) => {
                    const baseIndex = index * 6;
                    return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6})`;
                }).join(',');

                const claimParams = batch.flatMap(c => [
                    c.epoch, c.claim_ts, c.wallet_address, c.claim_amount,
                    c.bet_epoch, c.tx_hash
                ]);

                const claimInsertSQL = `
                    INSERT INTO claim (epoch, claim_ts, wallet_address, claim_amount, bet_epoch, tx_hash)
                    VALUES ${claimValues}
                `;

                await supabaseClient.query(claimInsertSQL, claimParams);
                processed += batch.length;
                console.log(`   📥 claim 進度: ${processed}/${claimData.rows.length} (${((processed/claimData.rows.length)*100).toFixed(1)}%)`);
            }
            console.log(`   ✅ claim 表遷移完成: ${claimData.rows.length} 筆`);
        }

        // 4. 分析多次領獎（直接在Supabase中執行SQL分析）
        console.log('🔍 分析多次領獎情況...');
        
        // 先檢查各種累積領獎情況的分佈
        const distributionSQL = `
            SELECT 
                bet_epoch_count,
                count(*) as wallet_count,
                sum(total_amount) as total_bnb
            FROM (
                SELECT 
                    epoch,
                    wallet_address,
                    COUNT(DISTINCT bet_epoch) AS bet_epoch_count,
                    SUM(claim_amount::numeric) as total_amount
                FROM claim 
                GROUP BY epoch, wallet_address
            ) t
            GROUP BY bet_epoch_count
            ORDER BY bet_epoch_count DESC
        `;
        
        const distribution = await supabaseClient.query(distributionSQL);
        console.log('📊 累積領獎局數分佈:');
        distribution.rows.forEach(row => {
            console.log(`   一次領取${row.bet_epoch_count}局獎金: ${row.wallet_count}個錢包, 總計${parseFloat(row.total_bnb || 0).toFixed(4)}BNB`);
        });

        const multiClaimSQL = `
            INSERT INTO multi_claim_wallets (claim_epoch, wallet_address, bet_epochs_count, total_amount)
            SELECT 
                epoch AS claim_epoch,
                wallet_address,
                COUNT(DISTINCT bet_epoch) AS bet_epochs_count,
                SUM(claim_amount::numeric) AS total_amount
            FROM claim 
            GROUP BY epoch, wallet_address
            HAVING COUNT(DISTINCT bet_epoch) > 3
            ORDER BY epoch, wallet_address
        `;

        const multiClaimResult = await supabaseClient.query(multiClaimSQL);
        console.log(`🚨 發現 ${multiClaimResult.rowCount} 個累積領獎案例（3局以上）`);

        // 顯示統計結果
        const endTime = Date.now();
        const duration = ((endTime - startTime) / 1000).toFixed(2);
        
        console.log(`\n🎉 批量遷移完成！耗時: ${duration} 秒`);

        // 驗證結果
        const finalStats = await Promise.all([
            supabaseClient.query('SELECT COUNT(*) as count FROM round'),
            supabaseClient.query('SELECT COUNT(*) as count FROM hisbet'), 
            supabaseClient.query('SELECT COUNT(*) as count FROM claim'),
            supabaseClient.query('SELECT COUNT(*) as count FROM multi_claim_wallets')
        ]);

        console.log('\n📊 遷移結果統計:');
        console.log(`   round: ${finalStats[0].rows[0].count} 筆`);
        console.log(`   hisbet: ${finalStats[1].rows[0].count} 筆`);
        console.log(`   claim: ${finalStats[2].rows[0].count} 筆`);
        console.log(`   multi_claim_wallets: ${finalStats[3].rows[0].count} 筆`);

        // 顯示前10個累積領獎案例
        const topCases = await supabaseClient.query(`
            SELECT claim_epoch, wallet_address, bet_epochs_count, total_amount 
            FROM multi_claim_wallets 
            ORDER BY bet_epochs_count DESC, total_amount DESC 
            LIMIT 10
        `);

        if (topCases.rows.length > 0) {
            console.log('\n🏆 前10名累積領獎案例:');
            topCases.rows.forEach((row, index) => {
                console.log(`   ${index + 1}. 在局次${row.claim_epoch}領取 ${row.wallet_address}: 累積${row.bet_epochs_count}局獎金 總計${parseFloat(row.total_amount).toFixed(4)}BNB`);
            });
        }

    } catch (error) {
        console.error('❌ 批量遷移失敗:', error.message);
        console.error('詳細錯誤:', error);
    } finally {
        await neonClient.end();
        await supabaseClient.end();
    }
}

fastBatchMigration();