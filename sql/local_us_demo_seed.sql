-- 本地美股演练数据：真实股票代码，固定模拟价格（不是实时行情）。
-- 本批数据按单一模拟币种 USD 解释，价格和账户金额均以整数美分存储。
-- 代码/名称参考：https://dorseywright.nasdaq.com/reports/symbolcloud
-- 补充参考：https://investors.thewaltdisneycompany.com/
-- https://www.investor.nexteraenergy.com/investor-resources/faqs
-- https://www.linde.com/investors
-- https://www.sec.gov/Archives/edgar/data/1053507/000105350726000154/pressreleaseq32026.htm
-- https://investor.exxonmobil.com/earnings/balance-sheet
-- 先导入 trade_accounts_schema.sql 和 broker_instruments.sql。
-- 用法：./scripts/mysql.sh import sql/local_us_demo_seed.sql
-- 重复导入只补缺失行，保留已有名称、价格、启用状态和账户余额。
-- 原 broker_instruments_seed.sql 的 SIM.* 测试样本继续保留，不在此覆盖。
-- 订单、资金流水和 Outbox 事件由业务接口生成，不在种子中伪造。
SET NAMES utf8mb4;
START TRANSACTION;

INSERT INTO accounts
    (account_id, account_name, initial_budget_cent, available_cent, frozen_cent, spent_cent)
VALUES
    (1,    'demo-account',       10000,   10000, 0, 0),
    (1001, 'us-demo-main',     1000000, 1000000, 0, 0),
    (1002, 'us-demo-small',     250000,  250000, 0, 0),
    (1003, 'us-demo-low-funds',   5000,    5000, 0, 0)
ON DUPLICATE KEY UPDATE
    account_id = accounts.account_id;

INSERT INTO broker_instruments
    (symbol, display_name, simulated_price_cent, enabled)
VALUES
    -- 科技、互联网与汽车：保留首批 8 只标的。
    ('AAPL',  'Apple Inc.',                   20000, TRUE),
    ('MSFT',  'Microsoft Corporation',        40000, TRUE),
    ('AMZN',  'Amazon.com, Inc.',             18000, TRUE),
    ('GOOGL', 'Alphabet Inc. Class A',        15000, TRUE),
    ('META',  'Meta Platforms, Inc.',         50000, TRUE),
    ('NVDA',  'NVIDIA Corporation',          12000, TRUE),
    ('TSLA',  'Tesla, Inc.',                 25000, TRUE),
    ('AMD',   'Advanced Micro Devices, Inc.', 10000, TRUE),
    -- 消费与零售：可口可乐、沃尔玛、麦当劳。
    ('KO',    'The Coca-Cola Company',        6000, TRUE),
    ('WMT',   'Walmart Inc.',                 8000, TRUE),
    ('MCD',   'McDonald''s Corporation',     30000, TRUE),
    -- 金融与支付：摩根大通、Visa。
    ('JPM',   'JPMorgan Chase & Co.',        20000, TRUE),
    ('V',     'Visa Inc.',                  25000, TRUE),
    -- 能源、医疗、工业：埃克森美孚、强生、卡特彼勒。
    ('XOM',   'ExxonMobil Holdings Corporation', 10000, TRUE),
    ('JNJ',   'Johnson & Johnson',          15000, TRUE),
    ('CAT',   'Caterpillar Inc.',           30000, TRUE),
    -- 娱乐、电力、地产、材料：迪士尼、新纪元能源、美国电塔、林德。
    ('DIS',   'The Walt Disney Company',    10000, TRUE),
    ('NEE',   'NextEra Energy, Inc.',        7000, TRUE),
    ('AMT',   'American Tower Corporation', 20000, TRUE),
    ('LIN',   'Linde plc',                  40000, TRUE)
ON DUPLICATE KEY UPDATE
    symbol = broker_instruments.symbol;

COMMIT;
