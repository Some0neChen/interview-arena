-- Fictional prices in the project's single simulated currency, measured in cents.
-- Company names are display labels only; SIM.* codes are not exchange tickers.
-- 24-company Nasdaq-100 sample, checked on 2026-10-28; not the full index.
-- Company selection: https://stockanalysis.com/list/nasdaq-100-stocks/
-- No live quotes or index weighting is implemented.
-- Run after broker_instruments.sql, in the same chosen broker database.
-- Re-import adds missing symbols but preserves existing names, prices and flags.
SET NAMES utf8mb4;

INSERT INTO broker_instruments
    (symbol, display_name, simulated_price_cent, enabled)
VALUES
    -- Consumer technology, cloud platforms and media.
    ('SIM.APPLE',      '苹果（模拟）',       280, TRUE),
    ('SIM.MICROSOFT',  '微软（模拟）',       500, TRUE),
    ('SIM.GOOGLE',     '谷歌（模拟）',       800, TRUE),
    ('SIM.AMAZON',     '亚马逊（模拟）',     350, TRUE),
    ('SIM.META',       'Meta（模拟）',       480, TRUE),
    ('SIM.NETFLIX',    '奈飞（模拟）',       420, TRUE),

    -- Semiconductors, memory and chipmaking equipment.
    ('SIM.NVIDIA',     '英伟达（模拟）',     650, TRUE),
    ('SIM.AMD',        'AMD（模拟）',        320, TRUE),
    ('SIM.BROADCOM',   '博通（模拟）',       560, TRUE),
    ('SIM.QUALCOMM',   '高通（模拟）',       260, TRUE),
    ('SIM.INTEL',      '英特尔（模拟）',     180, TRUE),
    ('SIM.MICRON',     '美光（模拟）',       240, TRUE),
    ('SIM.ASML',       '阿斯麦（模拟）',     900, TRUE),

    -- Networking, software and data analytics.
    ('SIM.CISCO',      '思科（模拟）',       200, TRUE),
    ('SIM.ADOBE',      'Adobe（模拟）',      460, TRUE),
    ('SIM.PALANTIR',   'Palantir（模拟）',   380, TRUE),

    -- Electric vehicles, retail, food and beverages.
    ('SIM.TESLA',      '特斯拉（模拟）',     600, TRUE),
    ('SIM.COSTCO',     '开市客（模拟）',     700, TRUE),
    ('SIM.PEPSICO',    '百事（模拟）',       220, TRUE),
    ('SIM.STARBUCKS',  '星巴克（模拟）',     160, TRUE),

    -- Biotechnology and industrial businesses.
    ('SIM.AMGEN',      '安进（模拟）',       440, TRUE),
    ('SIM.GILEAD',     '吉利德（模拟）',     300, TRUE),
    ('SIM.LINDE',      '林德（模拟）',       520, TRUE),
    ('SIM.HONEYWELL',  '霍尼韦尔（模拟）',   340, TRUE),

    -- Test fixtures, not Nasdaq-100 companies.
    ('SIM.FILL',       '正常成交标的（模拟）', 280, TRUE),
    ('SIM.DISABLED',   '停用标的（模拟）',     300, FALSE)
ON DUPLICATE KEY UPDATE
    symbol = broker_instruments.symbol;

-- SIM.UNKNOWN is intentionally absent, for unsupported-symbol tests.
-- This file supplies data only: waiting/filling/rejection requires broker code.
-- SIM.HOLD and SIM.REJECT behavior remains a separate simulation-rule task.
