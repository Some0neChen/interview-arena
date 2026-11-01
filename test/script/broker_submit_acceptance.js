#!/usr/bin/env node
'use strict';

// Build trade_broker, submit_order_acceptance and broker_submit_smoke first.
// Only the uniquely owned temporary database is writable by this harness.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');

if (process.argv.includes('--help')) {
    console.log('Usage: node test/script/broker_submit_acceptance.js [build-directory]');
    console.log('Requires MySQL:3306, free port 3666, Node.js and built test binaries.');
    console.log('Build: cmake --build build --target trade_broker submit_order_acceptance -j2');
    console.log('Client: bash test/script/broker_submit_smoke.sh --build-only');
    console.log('Uses the existing admin helper and its TRADE_TEST_MYSQL_ADMIN_* credentials.');
    console.log('Creates/revokes one temporary grant for the existing a@% application user.');
    console.log('Exit: 0 pass, 1 assertion failure, 2 setup/cleanup failure.');
    process.exit(0);
}
if (process.argv.length > 3) throw new Error('Expected at most one build-directory argument');
const root = path.resolve(__dirname, '../..');
const build = path.resolve(process.argv[2] || path.join(root, 'build'));
const database = `tradewritefaultbroker${Date.now()}${process.pid}`;
assert.match(database, /^tradewritefault[a-z0-9]{7,50}$/);
const reportDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-acceptance-'));
const report = { database, build, checks: [], cleanup: {}, scope: 'Submit/Get/CancelOrder, single broker, pool size 1; no matching/settlement' };
let created = false;
let granted = false;
let server;
let serverExit;
let serverLog;
let startNumber = 0;
let baseline;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const quote = value => `'${String(value).replaceAll("'", "''")}'`;

function sql(statement, schema = database) {
    const args = ['--admin-sql'];
    if (schema) args.push(schema);
    const result = spawnSync(path.join(build, 'submit_order_acceptance'), args, {
        input: statement, encoding: 'utf8', timeout: 10000
    });
    if (result.error) throw result.error;
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
}

function originalSnapshot() {
    const tables = sql("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'arena' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME", '');
    if (!tables) return '';
    const names = tables.split('\n');
    for (const name of names) assert.match(name, /^[a-zA-Z0-9_]+$/);
    return sql(`CHECKSUM TABLE ${names.map(name => `arena.\`${name}\``).join(', ')}`, '');
}

function request(overrides = {}) {
    return { namespace: 'review-main', order: '100', account: '1', symbol: 'SIM.APPLE', price: '300', quantity: '10', ...overrides };
}

function call(req, operation = 'submit') {
    return new Promise((resolve, reject) => {
        const args = operation === 'submit' ? [
            '127.0.0.1:3666', req.namespace, String(req.order), String(req.account),
            req.symbol, String(req.price), String(req.quantity)
        ] : ['127.0.0.1:3666', operation, req.namespace, String(req.order)];
        const child = spawn(path.join(build, 'broker_submit_smoke'), args);
        let stdout = '';
        let stderr = '';
        const timer = setTimeout(() => child.kill('SIGKILL'), 8000);
        child.stdout.on('data', data => { stdout += data; });
        child.stderr.on('data', data => { stderr += data; });
        child.once('error', error => { clearTimeout(timer); reject(error); });
        child.once('close', (code, signal) => {
            clearTimeout(timer);
            try {
                assert.equal(signal, null, `Client terminated: ${signal}`);
                assert.ok(code === 0 || code === 1, stderr);
                resolve(JSON.parse(stdout));
            } catch (error) { reject(error); }
        });
    });
}

function row(req) {
    const value = sql(`SELECT JSON_OBJECT('id', broker_order_id, 'namespace', gateway_namespace,
        'order', gateway_order_id, 'account', account_id, 'symbol', symbol, 'quantity', quantity,
        'price', limit_price_cent, 'state', state, 'version', broker_version,
        'filled', filled_quantity, 'fill_price', fill_price_cent, 'reason', reject_reason,
        'created_at', created_at, 'updated_at', updated_at)
        FROM broker_orders WHERE gateway_namespace = ${quote(req.namespace)} AND gateway_order_id = ${quote(req.order)}`);
    return value ? JSON.parse(value) : null;
}

function expectReply(reply, code, state) {
    assert.equal(reply.grpc_code, code, JSON.stringify(reply));
    if (code === 0) {
        assert.equal(reply.state, state, JSON.stringify(reply));
        assert.ok(reply.broker_order_id > 0, JSON.stringify(reply));
    }
}

function expectSnapshot(reply, stored) {
    const fields = {
        broker_order_id: 'id', gateway_namespace: 'namespace', gateway_order_id: 'order',
        account_id: 'account', symbol: 'symbol', quantity: 'quantity', limit_price_cent: 'price',
        state: 'state', broker_version: 'version', filled_quantity: 'filled',
        fill_price_cent: 'fill_price', reject_reason: 'reason'
    };
    for (const [wire, column] of Object.entries(fields)) {
        assert.equal(reply[wire], stored[column], `Snapshot mismatch: ${wire}`);
    }
}

async function check(name, body) {
    try {
        const details = await body();
        report.checks.push({ name, passed: true, details: details ?? null });
        console.log(`PASS: ${name}`);
    } catch (error) {
        report.checks.push({ name, passed: false, details: error.message });
        console.log(`FAIL: ${name}: ${error.message}`);
    }
}

async function portOpen(port) {
    return await new Promise(resolve => {
        const socket = net.connect({ host: '127.0.0.1', port });
        const finish = value => { socket.destroy(); resolve(value); };
        socket.once('connect', () => finish(true));
        socket.once('error', () => finish(false));
        socket.setTimeout(300, () => finish(false));
    });
}

async function startServer() {
    assert.equal(await portOpen(3666), false, 'Port 3666 occupied; no existing process was stopped');
    server = spawn(path.join(build, 'trade_broker'), [], {
        cwd: reportDirectory, env: { ...process.env, TRADE_MYSQL_DATABASE: database },
        stdio: ['ignore', 'pipe', 'pipe']
    });
    serverLog = fs.createWriteStream(path.join(reportDirectory, `server-${++startNumber}.log`));
    server.stdout.pipe(serverLog, { end: false });
    server.stderr.pipe(serverLog, { end: false });
    serverExit = new Promise(resolve => {
        server.once('error', error => resolve({ error: error.message }));
        server.once('close', (code, signal) => resolve({ code, signal }));
    });
    for (let i = 0; i < 50; ++i) {
        if (await portOpen(3666)) {
            const connected = sql(`SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER = 'a' AND DB = '${database}'`, '');
            if (Number(connected) > 0) return;
        }
        assert.equal(server.exitCode, null, 'Broker exited before startup completed');
        await sleep(100);
    }
    throw new Error('Broker did not connect to its isolated database within startup budget');
}

async function stopServer() {
    if (!server) return;
    const begin = Date.now();
    if (server.exitCode === null && server.signalCode === null) server.kill('SIGTERM');
    let timer;
    const timeout = new Promise(resolve => { timer = setTimeout(() => resolve(null), 8000); });
    let result = await Promise.race([serverExit, timeout]);
    clearTimeout(timer);
    if (result === null) {
        server.kill('SIGKILL');
        result = await serverExit;
        result.forced = true;
    }
    await new Promise(resolve => serverLog.end(resolve));
    server = undefined;
    assert.equal(result.code, 0, JSON.stringify(result));
    return { ...result, elapsed_ms: Date.now() - begin };
}

async function main() {
    try {
        for (const binary of ['trade_broker', 'submit_order_acceptance', 'broker_submit_smoke']) {
            fs.accessSync(path.join(build, binary), fs.constants.X_OK);
        }
        assert.ok(await portOpen(3306), 'Start MySQL on 3306 first');
        assert.equal(await portOpen(3666), false, 'Port 3666 is already occupied');
        baseline = originalSnapshot();
        report.original_before = baseline;
        sql(`CREATE DATABASE ${database} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin`, '');
        created = true;
        sql(`GRANT ALL PRIVILEGES ON ${database}.* TO 'a'@'%'`, '');
        granted = true;
        sql(fs.readFileSync(path.join(root, 'sql/broker_orders.sql'), 'utf8'));
        sql(fs.readFileSync(path.join(root, 'sql/broker_instruments.sql'), 'utf8'));
        // This controlled fixture contains one SET NAMES and one INSERT; not a generic SQL parser.
        const seed = fs.readFileSync(path.join(root, 'sql/broker_instruments_seed.sql'), 'utf8');
        for (const statement of seed.replace(/^\s*--.*$/gm, '').split(';').map(s => s.trim()).filter(Boolean)) sql(statement);
        report.schema_collation = sql(`SELECT COLLATION_NAME FROM information_schema.columns WHERE TABLE_SCHEMA='${database}' AND TABLE_NAME='broker_orders' AND COLUMN_NAME='gateway_namespace'`, '');
        await startServer();

        const base = request();
        const first = await call(base);
        expectReply(first, 0, 1);
        assert.equal(row(base).id, first.broker_order_id, 'RPC did not persist in isolated database');
        await check('normal order persists ACCEPTED with no fill/reject fields', () => {
            const actual = row(base);
            assert.equal(actual.symbol, base.symbol);
            assert.equal(actual.account, 1);
            assert.equal(actual.price, 300);
            assert.equal(actual.quantity, 10);
            assert.equal(actual.state, 1);
            assert.equal(actual.version, 1);
            assert.equal(actual.filled, 0);
            assert.equal(actual.fill_price, 0);
            assert.equal(actual.reason, '');
            return actual;
        });
        await check('same key and parameters return the original order unchanged', async () => {
            const before = row(base);
            const reply = await call(base);
            expectReply(reply, 0, 1);
            assert.equal(reply.broker_order_id, first.broker_order_id);
            assert.deepEqual(row(base), before);
        });
        for (const [field, value] of [['account', '2'], ['symbol', 'SIM.MICROSOFT'], ['price', '301'], ['quantity', '11']]) {
            await check(`same key but different ${field} -> ALREADY_EXISTS`, async () => {
                const before = row(base);
                expectReply(await call(request({ [field]: value })), 6);
                assert.deepEqual(row(base), before);
            });
        }
        for (const [name, symbol, reason] of [
            ['disabled', 'SIM.DISABLED', 'symbol disabled'],
            ['unknown', 'SIM.UNKNOWN', 'unsupported symbol']
        ]) {
            await check(`${name} symbol persists REJECTED and repeats idempotently`, async () => {
                const req = request({ order: name === 'disabled' ? '200' : '201', symbol });
                const reply = await call(req);
                expectReply(reply, 0, 3);
                const before = row(req);
                assert.equal(before.reason, reason);
                assert.equal(before.filled, 0);
                assert.equal(before.fill_price, 0);
                assert.equal(before.version, 1);
                assert.equal((await call(req)).broker_order_id, reply.broker_order_id);
                assert.deepEqual(row(req), before);
            });
        }
        await check('low limit price is ACCEPTED, not a business rejection', async () => {
            const req = request({ order: '202', price: '1' });
            expectReply(await call(req), 0, 1);
            assert.equal(row(req).filled, 0);
        });
        await check('instrument disabled later does not rewrite an existing order', async () => {
            const before = row(base);
            sql("UPDATE broker_instruments SET enabled=FALSE WHERE symbol='SIM.APPLE'");
            try {
                expectReply(await call(base), 0, 1);
                assert.deepEqual(row(base), before);
                expectReply(await call(request({ order: '203' })), 0, 3);
            } finally { sql("UPDATE broker_instruments SET enabled=TRUE WHERE symbol='SIM.APPLE'"); }
        });
        await check('different namespaces may reuse the gateway order number', async () => {
            const reply = await call(request({ namespace: 'review-other' }));
            expectReply(reply, 0, 1);
            assert.notEqual(reply.broker_order_id, first.broker_order_id);
        });
        await check('fresh schema keeps case-distinct namespaces separate', async () => {
            const a = await call(request({ namespace: 'ReviewCase', order: '204' }));
            const b = await call(request({ namespace: 'reviewcase', order: '204' }));
            expectReply(a, 0, 1);
            expectReply(b, 0, 1);
            assert.notEqual(a.broker_order_id, b.broker_order_id, JSON.stringify({ a, b }));
        });
        await check('case-mismatched instrument is unsupported', async () => {
            expectReply(await call(request({ order: '205', symbol: 'sim.apple' })), 0, 3);
        });
        const invalid = [
            ['empty namespace', { namespace: '' }], ['zero order', { order: '0' }],
            ['zero account', { account: '0' }], ['empty symbol', { symbol: '' }],
            ['zero price', { price: '0' }], ['zero quantity', { quantity: '0' }],
            ['amount overflow', { price: '18446744073709551615', quantity: '2' }],
            ['namespace exceeds column limit', { namespace: 'x'.repeat(256) }]
        ];
        for (const [name, overrides] of invalid) {
            await check(`${name} -> INVALID_ARGUMENT without a new row`, async () => {
                const count = sql('SELECT COUNT(*) FROM broker_orders');
                const reply = await call(request({ order: '300', ...overrides }));
                assert.equal(sql('SELECT COUNT(*) FROM broker_orders'), count);
                expectReply(reply, 3);
            });
        }
        await check('connection remains usable after rejected/invalid requests', async () => {
            expectReply(await call(request({ order: '301' })), 0, 1);
        });
        await check('eight concurrent duplicates create exactly one order (pool size 1)', async () => {
            const req = request({ order: '400' });
            const outcomes = await Promise.allSettled(Array.from({ length: 8 }, () => call(req)));
            const replies = outcomes.map(item => {
                if (item.status === 'rejected') throw item.reason;
                return item.value;
            });
            for (const reply of replies) expectReply(reply, 0, 1);
            assert.equal(new Set(replies.map(reply => reply.broker_order_id)).size, 1);
            assert.equal(sql("SELECT COUNT(*) FROM broker_orders WHERE gateway_namespace='review-main' AND gateway_order_id=400"), '1');
        });
        await check('forced INSERT failure creates no order; same-key retry recovers', async () => {
            const req = request({ order: '500' });
            sql('ALTER TABLE broker_orders ADD CONSTRAINT test_reject_500 CHECK (gateway_order_id <> 500)');
            try {
                expectReply(await call(req), 13);
                assert.equal(row(req), null);
            } finally { sql('ALTER TABLE broker_orders DROP CHECK test_reject_500'); }
            expectReply(await call(req), 0, 1);
        });
        await check('replay returns persisted FILLED state without resetting it', async () => {
            const req = request({ order: '600' });
            expectReply(await call(req), 0, 1);
            sql("UPDATE broker_orders SET state=2, broker_version=2, filled_quantity=quantity, fill_price_cent=280 WHERE gateway_namespace='review-main' AND gateway_order_id=600");
            const before = row(req);
            expectReply(await call(req), 0, 2);
            assert.deepEqual(row(req), before);
        });
        for (const state of [0, 255]) {
            await check(`persisted invalid state ${state} must not be returned as RPC success`, async () => {
                const req = request({ order: String(700 + state) });
                expectReply(await call(req), 0, 1);
                sql(`UPDATE broker_orders SET state=${state} WHERE gateway_namespace='review-main' AND gateway_order_id=${req.order}`);
                expectReply(await call(req), 13);
            });
        }
        await check('GetOrder returns the complete snapshot without modifying the row', async () => {
            const before = row(base);
            const reply = await call(base, 'get');
            expectReply(reply, 0, 1);
            expectSnapshot(reply, before);
            assert.deepEqual(row(base), before);
        });
        await check('GetOrder missing key -> NOT_FOUND', async () => {
            expectReply(await call(request({ order: '99999' }), 'get'), 5);
        });
        await check('GetOrder zero key -> INVALID_ARGUMENT', async () => {
            expectReply(await call(request({ order: '0' }), 'get'), 3);
        });
        await check('GetOrder distinguishes namespaces sharing an order number', async () => {
            const req = request({ namespace: 'review-other' });
            const reply = await call(req, 'get');
            expectReply(reply, 0, 1);
            expectSnapshot(reply, row(req));
            assert.notEqual(reply.broker_order_id, first.broker_order_id);
        });
        await check('CancelOrder changes ACCEPTED once; repeated cancel is unchanged', async () => {
            const req = request({ order: '850' });
            expectReply(await call(req), 0, 1);
            const before = row(req);
            const reply = await call(req, 'cancel');
            expectReply(reply, 0, 4);
            const after = row(req);
            assert.equal(after.state, 4);
            assert.equal(after.version, before.version + 1);
            assert.equal(after.filled, 0);
            assert.equal(after.fill_price, 0);
            expectSnapshot(reply, after);
            const repeated = await call(req, 'cancel');
            expectReply(repeated, 0, 4);
            expectSnapshot(repeated, after);
            assert.deepEqual(row(req), after);
        });
        for (const [order, state, label] of [['600', 2, 'FILLED'], ['201', 3, 'REJECTED']]) {
            await check(`CancelOrder returns existing ${label} without mutation`, async () => {
                const req = request({ order });
                const before = row(req);
                const reply = await call(req, 'cancel');
                expectReply(reply, 0, state);
                expectSnapshot(reply, before);
                assert.deepEqual(row(req), before);
            });
        }
        await check('CancelOrder missing key -> NOT_FOUND without creating a row', async () => {
            const req = request({ order: '99998' });
            const reply = await call(req, 'cancel');
            assert.equal(row(req), null);
            expectReply(reply, 5);
        });
        await check('CancelOrder zero key -> INVALID_ARGUMENT', async () => {
            expectReply(await call(request({ order: '0' }), 'cancel'), 3);
        });
        await check('idle broker exits normally', stopServer);
        await startServer();
        await check('restart preserves the original order ID and state', async () => {
            const reply = await call(base);
            expectReply(reply, 0, 1);
            assert.equal(reply.broker_order_id, first.broker_order_id);
        });
        // Reproduce the user's existing binary collation without changing the source schema or arena.
        sql('ALTER TABLE broker_orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin');
        await check('binary collation separates case-distinct namespaces', async () => {
            const a = await call(request({ namespace: 'BinaryCase', order: '800' }));
            const b = await call(request({ namespace: 'binarycase', order: '800' }));
            expectReply(a, 0, 1);
            expectReply(b, 0, 1);
            assert.notEqual(a.broker_order_id, b.broker_order_id);
        });
    } catch (error) {
        report.setup_or_runtime_error = error.stack;
        process.exitCode = 2;
        console.error(error);
    } finally {
        const clean = async (name, action) => {
            try { report.cleanup[name] = (await action()) ?? true; }
            catch (error) { report.cleanup[name] = { error: error.message }; process.exitCode = 2; }
        };
        await clean('server', stopServer);
        if (granted) await clean('grant', () => {
            sql(`REVOKE ALL PRIVILEGES ON ${database}.* FROM 'a'@'%'`, '');
            assert.equal(sql(`SELECT COUNT(*) FROM mysql.db WHERE Db='${database}'`, ''), '0');
        });
        if (created) await clean('database', () => {
            sql(`DROP DATABASE ${database}`, '');
            assert.equal(sql(`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='${database}'`, ''), '0');
        });
        if (baseline !== undefined) await clean('original_unchanged', () => {
            report.original_after = originalSnapshot();
            assert.equal(report.original_after, baseline);
        });
        report.passed = report.checks.filter(item => item.passed).length;
        report.failed = report.checks.length - report.passed;
        if (!process.exitCode) process.exitCode = report.failed ? 1 : 0;
        report.exit_code = process.exitCode;
        fs.writeFileSync(path.join(reportDirectory, 'results.json'), JSON.stringify(report, null, 2) + '\n');
        console.log(`RESULT: ${report.passed}/${report.checks.length} passed; exit ${process.exitCode}`);
        console.log(`Report: ${reportDirectory}/results.json`);
    }
}
main().catch(error => { console.error(error); process.exitCode = 2; });
