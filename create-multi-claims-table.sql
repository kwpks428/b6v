-- 創建 multi_claims 表
-- 用於記錄單局多次領獎的可疑錢包

CREATE TABLE IF NOT EXISTS multi_claims (
    epoch VARCHAR(20) NOT NULL,
    wallet_address VARCHAR(42) NOT NULL,
    claim_count INTEGER NOT NULL,
    total_amount NUMERIC(20,8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (epoch, wallet_address)
);

-- 創建索引以提升查詢性能
CREATE INDEX IF NOT EXISTS idx_multi_claims_wallet ON multi_claims (wallet_address);
CREATE INDEX IF NOT EXISTS idx_multi_claims_epoch ON multi_claims (epoch);
CREATE INDEX IF NOT EXISTS idx_multi_claims_count ON multi_claims (claim_count);

-- 添加註釋
COMMENT ON TABLE multi_claims IS '記錄單局多次領獎的可疑錢包';
COMMENT ON COLUMN multi_claims.epoch IS '局次編號';
COMMENT ON COLUMN multi_claims.wallet_address IS '錢包地址';
COMMENT ON COLUMN multi_claims.claim_count IS '該局次該錢包的領獎次數';
COMMENT ON COLUMN multi_claims.total_amount IS '該局次該錢包的總領獎金額';
COMMENT ON COLUMN multi_claims.created_at IS '記錄創建時間';