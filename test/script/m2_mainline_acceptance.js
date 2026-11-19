#!/usr/bin/env node
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const {spawn, spawnSync} = require('node:child_process');

const root = path.resolve(__dirname, '../..');
const build = path.resolve(process.env.M2_BUILD_DIR || path.join(root, 'build'));
if (process.argv.includes('--help')) {
    console.log(`Usage: node test/script/m2_mainline_acceptance.js [--lost-reply|--cancel-fill|--remaining|--snapshot-rules|--backoff]
Builds trade_server, trade_broker, submit_order_acceptance and a small real RPC client.
Requires an already configured build with ARENA_BUILD_ACCEPTANCE_TESTS=ON,
Node.js, a C++ compiler, pkg-config grpc++/protobuf and local MySQL on port 3306.
Ports 13401 and 3666 must be free. No existing process will be stopped.
Uses two uniquely named temporary databases; never writes the arena database.
Admin credentials: TRADE_TEST_MYSQL_ADMIN_USER/PASSWORD (existing test helper defaults).
Application user 'a' must already exist with the password expected by the servers.
M2_BUILD_DIR overrides build/. Report and process logs are retained under /tmp.
Default: mainline/restart suite, NOT precise dropped-reply or cancel/fill race injection.
--lost-reply: test-only gRPC facade commits through the real broker Repository, withholds
the successful reply, then restores the real broker and restarts the production gateway.
--cancel-fill: real broker Repository with two connections; test-only trigger barrier and
performance_schema evidence fix the two row-lock acquisition orders, then production settlement.
--remaining: local SQL race, settlement COMMIT fault, precise pre-commit process crash,
in-flight shutdown and invalid broker snapshots; also requires free ports 13307/13308.
Snapshot cases keep running after assertion failures, and the suite exits nonzero if any fail.
--snapshot-rules: directly exercise the real local Repository with complete broker snapshots;
check version/content rejection, replay safety and non-FILLED data without a running broker.
--backoff: measure actual per-order delays, restart persistence and recovery in real time.
Fault facade compilation uses BOOST188_ROOT (default ~/.local/boost-1.88.0).`);
    process.exit(0);
}
const lostReply = process.argv.length === 3 && process.argv[2] === '--lost-reply';
const cancelFill = process.argv.length === 3 && process.argv[2] === '--cancel-fill';
const remaining = process.argv.length === 3 && process.argv[2] === '--remaining';
const snapshotRules = process.argv.length === 3 && process.argv[2] === '--snapshot-rules';
const backoff = process.argv.length === 3 && process.argv[2] === '--backoff';
assert.ok(process.argv.length === 2 || lostReply || cancelFill || remaining || snapshotRules || backoff, 'Unknown argument; use --help');
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'm2-mainline-'));
const suffix = `${Date.now()}${process.pid}`;
const tradeDb = `tradewritefaultm2trade${suffix}`;
const brokerDb = `tradewritefaultm2broker${suffix}`;
const report = {started_at: new Date().toISOString(), directory, databases: [tradeDb, brokerDb],
    scope: 'Two production processes, separate databases, pool size 1 per process; no performance claim',
    deferred: ['broker commits but reply is lost', 'exact cancel/fill and dispatch/cancel races',
        'local settlement COMMIT reply loss', 'stale or invalid broker snapshots', 'late callbacks during in-flight shutdown'],
    cases: [], processes: [], cleanup: []};
if (lostReply) {
    report.scope = 'Production gateway, test-only RPC facade with real broker Repository/COMMIT, then production broker; isolated databases';
    report.deferred = report.deferred.filter(item => item !== 'broker commits but reply is lost');
    report.deferred.push('actual network packet loss with the unmodified broker RPC facade');
}
if (cancelFill) {
    report.scope = 'Production gateway/broker before and after a two-connection broker Repository race; test-only trigger and observed row-lock waits';
    report.deferred = report.deferred.filter(item => item !== 'exact cancel/fill and dispatch/cancel races');
    report.deferred.push('local cancel versus prepare dispatch race', 'RPC transport and worker scheduling races beyond the two tested SQL lock orders');
}
if (remaining) {
    report.scope = 'M2 remaining audit: real local Repository SQL races/COMMIT faults, production shutdown/recovery, intentionally invalid RPC snapshots';
    report.deferred = ['version/content matrix tested separately by --snapshot-rules',
        'retry timing tested separately by --backoff; no throughput or latency capacity claim'];
}
if (backoff) {
    report.scope = 'Production dispatcher, real broker and MySQL retry writes in isolated databases; one dispatcher';
    report.deferred = ['capacity under load', 'multiple dispatcher writers'];
}
if (snapshotRules) {
    report.scope = 'Real local Repository snapshot decisions and transactions in isolated MySQL tables; synthetic complete replies, no RPC transport';
    report.deferred = ['RPC-level snapshot validation', 'concurrent callback ordering', 'per-order retry backoff'];
}
const servers = new Map();
const created = new Set();
const grants = new Set();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let run = 0;
let interrupted;
let cleaningUp = false;
for (const signal of ['SIGINT', 'SIGTERM']) {
    process.once(signal, () => {interrupted = signal;});
}

function checkInterrupted() {
    if (interrupted && !cleaningUp) throw new Error(`Interrupted by ${interrupted}; cleaning owned resources`);
}

function command(file, args, options = {}) {
    checkInterrupted();
    const result = spawnSync(file, args, {cwd: root, encoding: 'utf8', timeout: 120000, ...options});
    assert.equal(result.status, 0, result.error?.message || result.stderr || result.stdout);
    return result.stdout;
}

function sql(statement, database = tradeDb) {
    return command(path.join(build, 'submit_order_acceptance'),
        ['--admin-sql', ...(database ? [database] : [])], {input: statement, timeout: 10000}).trim();
}
const jsonRows = (statement, database = tradeDb) => {
    const output = sql(statement, database);
    return output ? output.split('\n').map(line => JSON.parse(line)) : [];
};

function portOpen(port) {
    return new Promise(resolve => {
        const socket = net.connect({host: '127.0.0.1', port});
        const done = open => {socket.destroy(); resolve(open);};
        socket.once('connect', () => done(true));
        socket.once('error', () => done(false));
        socket.setTimeout(400, () => done(false));
    });
}

async function until(predicate, label, timeout = 20000) {
    const begin = Date.now();
    while (Date.now() - begin < timeout) {
        checkInterrupted();
        for (const [name, handle] of servers) {
            assert.ok(handle.child.exitCode === null && handle.child.signalCode === null,
                `${name} exited unexpectedly; inspect ${handle.log}`);
        }
        if (await predicate()) return Date.now() - begin;
        await sleep(100);
    }
    throw new Error(`Timed out: ${label}`);
}

async function start(name, fault = false) {
    assert.ok(!servers.has(name));
    const port = name === 'trade' ? 13401 : 3666;
    const database = name === 'trade' ? tradeDb : brokerDb;
    assert.equal(await portOpen(port), false, `Port ${port} occupied; no existing process was stopped`);
    const cwd = path.join(directory, `${name}-${++run}`);
    fs.mkdirSync(cwd);
    const log = path.join(cwd, 'process.log');
    const fd = fs.openSync(log, 'w');
    assert.ok(!fault || name === 'broker');
    const binary = fault === 'snapshot' ? 'm2_snapshot_broker' : fault ? 'm2_lost_reply_broker' : name === 'trade' ? 'trade_server' : 'trade_broker';
    const args = fault ? [path.join(directory, fault === 'snapshot' ? 'snapshot-events.jsonl' : 'lost-reply-events.jsonl')] : [];
    const child = spawn(path.join(build, binary), args, {
        cwd, env: {...process.env, TRADE_MYSQL_DATABASE: database}, stdio: ['ignore', fd, fd]
    });
    fs.closeSync(fd);
    const exited = new Promise(resolve => {
        child.once('error', error => resolve({error: error.message}));
        child.once('close', (code, signal) => resolve({code, signal}));
    });
    servers.set(name, {child, exited, log});
    report.processes.push({name, binary, pid: child.pid, database, log});
    await until(() => portOpen(port), `${name} listen`);
    if (fault !== 'snapshot') {
        await until(() => Number(sql(`SELECT COUNT(*) FROM information_schema.PROCESSLIST
            WHERE DB='${database}' AND USER='a'`, '')) > 0, `${name} pool connected`);
    }
}

async function startProxy() {
    for (const port of [13307, 13308]) assert.equal(await portOpen(port), false, `Proxy port ${port} occupied`);
    const log = path.join(directory, 'proxy.log');
    const fd = fs.openSync(log, 'w');
    const child = spawn('python3', [path.join(root, 'test/script/mysql_commit_fault_proxy.py'),
        '--events', path.join(directory, 'commit-events.jsonl')], {cwd: root, stdio: ['ignore', fd, fd]});
    fs.closeSync(fd);
    const exited = new Promise(resolve => {
        child.once('error', error => resolve({error: error.message}));
        child.once('close', (code, signal) => resolve({code, signal}));
    });
    servers.set('proxy', {child, exited, log});
    report.processes.push({name: 'proxy', pid: child.pid, log});
    await until(() => portOpen(13308), 'COMMIT fault proxy');
}

async function startGate() {
    assert.ok(!servers.has('gate'));
    const log = path.join(directory, 'settlement-gate.log');
    const fd = fs.openSync(log, 'w');
    const child = spawn(path.join(build, 'm2_local_repository_probe'), ['gate', '1'], {
        cwd: root, env: {...process.env, TRADE_MYSQL_DATABASE: tradeDb}, stdio: ['ignore', fd, fd]
    });
    fs.closeSync(fd);
    const exited = new Promise(resolve => {
        child.once('error', error => resolve({error: error.message}));
        child.once('close', (code, signal) => resolve({code, signal}));
    });
    servers.set('gate', {child, exited, log});
    report.processes.push({name: 'gate', pid: child.pid, log});
    await until(() => fs.readFileSync(log, 'utf8').includes('gate_ready'), 'settlement gate held');
}

async function stop(name, crash = false) {
    const handle = servers.get(name);
    if (!handle) return null;
    const begin = Date.now();
    const {child, exited} = handle;
    if (child.exitCode === null && child.signalCode === null) child.kill(crash ? 'SIGKILL' : 'SIGTERM');
    let timer;
    let forced = false;
    let result = await Promise.race([exited, new Promise(resolve => {
        timer = setTimeout(() => resolve(null), 8000);
    })]);
    clearTimeout(timer);
    if (result === null) {
        forced = true;
        child.kill('SIGKILL');
        result = await exited;
    }
    servers.delete(name);
    const details = {name, ...result, forced, crash_requested: crash, elapsed_ms: Date.now() - begin};
    report.processes.push(details);
    assert.equal(forced, false, `${name} exceeded shutdown bound`);
    if (crash) assert.equal(result.signal, 'SIGKILL');
    else assert.equal(result.code, 0, JSON.stringify(details));
    return details;
}

function rpc(...args) {
    return new Promise((resolve, reject) => {
        const child = spawn(path.join(build, 'm2_rpc_client'), ['127.0.0.1:13401', ...args.map(String)]);
        let stdout = '', stderr = '';
        const timer = setTimeout(() => child.kill('SIGKILL'), 7000);
        child.stdout.on('data', chunk => {stdout += chunk;});
        child.stderr.on('data', chunk => {stderr += chunk;});
        child.once('error', error => {clearTimeout(timer); reject(error);});
        child.once('close', (code, signal) => {
            clearTimeout(timer);
            try {
                assert.ok(code === 0 || code === 1, `RPC driver failed ${code}/${signal}: ${stderr}`);
                const result = JSON.parse(stdout);
                assert.equal(code, result.grpc_code === 0 ? 0 : 1);
                resolve(result);
            } catch (error) {reject(error);}
        });
    });
}

function account(id) {
    return jsonRows(`SELECT JSON_OBJECT('initial',initial_budget_cent,'available',available_cent,
        'frozen',frozen_cent,'spent',spent_cent) FROM accounts WHERE account_id=${id}`)[0];
}
function fixture(id) {
    sql(`INSERT INTO accounts (account_id,account_name,initial_budget_cent,available_cent)
        VALUES (${id},'m2-fixture',10000,10000)`);
}
function localOrder(accountId, key) {
    assert.match(key, /^[a-z0-9-]+$/);
    return jsonRows(`SELECT JSON_OBJECT('order_id',order_id,'account_id',account_id,'state',state,
        'version',version,'reserved',reserved_cent,'quantity',quantity,'limit',limit_price_cent,
        'broker_order_id',broker_order_id,'broker_version',broker_version,'broker_state',broker_state,
        'filled_quantity',filled_quantity,'fill_price_cent',fill_price_cent,'reject_reason',reject_reason)
        FROM orders WHERE account_id=${accountId} AND client_order_id='${key}'`)[0] || null;
}
function remoteOrder(id) {
    return jsonRows(`SELECT JSON_OBJECT('broker_order_id',broker_order_id,'gateway_order_id',gateway_order_id,
        'account_id',account_id,'symbol',symbol,'state',state,'version',broker_version,
        'filled',filled_quantity,'price',fill_price_cent,'reason',reject_reason)
        FROM broker_orders WHERE gateway_namespace='SolCelestial69' AND gateway_order_id=${id}`, brokerDb)[0] || null;
}
function snapshot(id, key) {
    const order = localOrder(id, key);
    assert.ok(order, 'Local order missing');
    const ledger = jsonRows(`SELECT JSON_OBJECT('type',entry_type,'amount',amount_cent)
        FROM fund_ledger WHERE order_id=${order.order_id} ORDER BY entry_type`);
    const outbox = jsonRows(`SELECT JSON_OBJECT('type',event_type,'version',order_version,
        'payload',payload,'publish_state',publish_state)
        FROM outbox_events WHERE order_id=${order.order_id} ORDER BY order_version`);
    return {account: account(id), order, ledger, outbox, broker: remoteOrder(order.order_id)};
}
async function waitState(id, key, state) {
    await until(() => localOrder(id, key)?.state === state, `account ${id}/${key} state ${state}`);
    const reply = await rpc('get', id, key);
    assert.equal(reply.grpc_code, 0);
    assert.equal(reply.state, state);
    return reply;
}
async function submit(id, key, symbol = 'SIM.FILL', price = 300, quantity = 10) {
    const reply = await rpc('submit', id, key, symbol, price, quantity);
    assert.equal(reply.grpc_code, 0, JSON.stringify(reply));
    assert.ok(reply.order_id > 0);
    return reply;
}
function terminal(id, key, state, spent = 0) {
    const result = snapshot(id, key);
    report.last_terminal_snapshot = {account_id: id, client_order_id: key, ...result};
    assert.equal(result.order.state, state);
    assert.deepEqual(result.account, {initial: 10000, available: 10000 - spent, frozen: 0, spent});
    const ledgerType = {8: 3, 9: 2, 10: 4}[state];
    const ledger = [{type: 1, amount: 3000}, {type: ledgerType, amount: state === 8 ? spent : 3000}];
    if (state === 8 && result.order.reserved > spent) {
        ledger.push({type: 5, amount: result.order.reserved - spent});
    }
    assert.deepEqual(result.ledger, ledger);
    for (const event of result.outbox) {
        assert.ok(event.payload !== null && typeof event.payload === 'object' && !Array.isArray(event.payload),
            `Outbox event type ${event.type} must contain a JSON object, got ${JSON.stringify(event.payload)}`);
        assert.equal(event.payload.account_id, id);
        assert.equal(event.payload.client_order_id, key);
    }
    const started = result.outbox.filter(event => event.type === 6);
    assert.equal(started.length, 1, 'A remotely handled order must have exactly one DISPATCH_STARTED event');
    assert.equal(started[0].version, 2);
    assert.equal(started[0].payload.state, 6);
    assert.equal(result.outbox[0].type, 1);
    assert.equal(result.outbox[0].payload.state, 5);
    assert.equal(result.outbox.at(-1).type, {8: 5, 9: 2, 10: 4}[state]);
    assert.equal(result.outbox.at(-1).version, result.order.version);
    assert.equal(result.outbox.at(-1).payload.state, state);
    assert.ok(result.outbox.every(event => event.publish_state === 0));
    assert.equal(result.broker?.state, {8: 2, 9: 4, 10: 3}[state]);
    assert.equal(result.order.broker_order_id, result.broker.broker_order_id,
        'Local binding must persist the actual broker order ID, including lost-reply recovery');
    for (const [local, remote] of [
        ['broker_version', 'version'], ['broker_state', 'state'], ['filled_quantity', 'filled'],
        ['fill_price_cent', 'price'], ['reject_reason', 'reason']
    ]) {
        assert.equal(result.order[local], result.broker[remote],
            `Persisted broker snapshot field ${local} must match the settled remote order`);
    }
    if (state === 8) {
        assert.equal(result.broker.filled, 10);
        assert.equal(result.broker.price * result.broker.filled, spent);
        const payload = result.outbox.at(-1).payload;
        assert.equal(payload.filled_quantity, result.broker.filled);
        assert.equal(payload.fill_price_cent, result.broker.price);
        assert.equal(payload.spent_cent, spent);
        assert.equal(payload.refund_cent, result.order.reserved - spent);
    } else {
        for (const key of ['filled_quantity', 'fill_price_cent', 'spent_cent', 'refund_cent']) {
            assert.equal(Object.hasOwn(result.outbox.at(-1).payload, key), false,
                'Non-FILLED events should not carry FILLED-only fields');
        }
    }
    return result;
}
async function check(name, fn) {
    try {
        checkInterrupted();
        const details = await fn();
        checkInterrupted();
        report.cases.push({name, passed: true, details});
        console.log('PASS: ' + name);
    } catch (error) {
        report.cases.push({name, passed: false, error: error.message});
        throw error;
    }
}

function faultEvents() {
    const file = path.join(directory, 'lost-reply-events.jsonl');
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, 'utf8').split('\n').slice(0, -1).filter(Boolean).map(line => JSON.parse(line));
}

async function lostReplyChecks() {
    await start('broker', true);
    await start('trade');
    let pending;
    await check('real broker commit precedes withheld reply; deadline and failed confirmation cannot refund', async () => {
        const submitted = await submit(1, 'lost-reply');
        await until(() => faultEvents().some(e => e.event === 'withheld_call_finished'), 'withheld RPC cancelled by deadline');
        await until(() => faultEvents().some(e => e.event === 'confirmation_unavailable'), 'gateway attempts confirmation');
        const events = faultEvents();
        const commits = events.filter(e => e.event === 'committed_reply_withheld');
        assert.equal(commits.length, 1);
        assert.equal(commits[0].gateway_order_id, submitted.order_id);
        assert.equal(commits[0].gateway_namespace, 'SolCelestial69');
        assert.equal(commits[0].state, 1);
        const ended = events.find(e => e.event === 'withheld_call_finished');
        assert.equal(ended.cancelled, true);
        assert.ok(ended.elapsed_ms >= 2000 && ended.elapsed_ms < 10000);
        assert.ok(!events.some(e => e.event === 'fixture_error' || e.event === 'repository_failed'));
        pending = snapshot(1, 'lost-reply');
        assert.equal(pending.order.state, 6);
        assert.equal(pending.order.broker_order_id, null,
            'A withheld Submit reply must leave the local broker ID unbound before recovery');
        assert.equal(pending.broker.state, 1);
        assert.equal(pending.broker.broker_order_id, commits[0].broker_order_id);
        assert.deepEqual(pending.account, {initial: 10000, available: 7000, frozen: 3000, spent: 0});
        assert.deepEqual(pending.ledger, [{type: 1, amount: 3000}]);
        assert.deepEqual(pending.outbox.map(e => e.type), [1, 6]);
        assert.equal(Number(sql('SELECT COUNT(*) FROM broker_orders', brokerDb)), 1);
        return {events, snapshot: pending};
    });
    await check('client retries while execution is unknown return the original order without another freeze', async () => {
        const retry = await submit(1, 'lost-reply');
        assert.equal(retry.order_id, pending.order.order_id);
        assert.equal(retry.state, 6);
        await sleep(650);
        assert.deepEqual(snapshot(1, 'lost-reply'), pending);
        assert.equal(faultEvents().filter(e => e.event === 'committed_reply_withheld').length, 1);
        return retry;
    });
    let settled;
    await check('gateway SIGKILL and restored production broker settle the same persisted broker order', async () => {
        await stop('trade', true);
        await stop('broker');
        report.fault_events = faultEvents();
        await start('broker');
        await start('trade');
        await waitState(1, 'lost-reply', 8);
        settled = terminal(1, 'lost-reply', 8, 2800);
        assert.equal(settled.order.order_id, pending.order.order_id);
        assert.equal(settled.broker.broker_order_id, pending.broker.broker_order_id);
        assert.equal(settled.broker.gateway_order_id, pending.order.order_id);
        assert.equal(Number(sql('SELECT COUNT(*) FROM broker_orders', brokerDb)), 1);
        assert.equal(Number(sql('SELECT COUNT(*) FROM orders')), 1);
        return settled;
    });
    await check('post-recovery retries and repeated scans preserve the one settlement and price refund', async () => {
        const retry = await submit(1, 'lost-reply');
        assert.equal(retry.state, 8);
        assert.equal(retry.order_id, settled.order.order_id);
        await sleep(1000);
        assert.deepEqual(snapshot(1, 'lost-reply'), settled);
        assert.equal(Number(sql('SELECT COUNT(*) FROM accounts WHERE initial_budget_cent <> available_cent + frozen_cent + spent_cent')), 0);
        return {retry, snapshot: settled};
    });
}

async function cancelFillChecks() {
    await start('trade');
    for (const [id, mode, state, spent] of [[1, 'cancel-first', 9, 0], [2, 'fill-first', 8, 2800]]) {
        sql("UPDATE broker_instruments SET simulated_price_cent=1000 WHERE symbol='SIM.HOLD'", brokerDb);
        await start('broker');
        await submit(id, mode, 'SIM.HOLD');
        await waitState(id, mode, 7);
        const accepted = snapshot(id, mode);
        await stop('broker');
        await check(`${mode}: local cancellation remains pending and retains funds before broker confirmation`, async () => {
            const reply = await rpc('cancel', id, mode);
            assert.equal(reply.grpc_code, 0);
            assert.equal(reply.state, 11);
            const pending = snapshot(id, mode);
            assert.equal(pending.order.state, 11);
            assert.deepEqual(pending.account, {initial: 10000, available: 7000, frozen: 3000, spent: 0});
            assert.deepEqual(pending.ledger, [{type: 1, amount: 3000}]);
            return pending;
        });
        await check(`${mode}: two real Repository operations overlap and the loser waits for the winner's row lock`, async () => {
            sql("UPDATE broker_instruments SET simulated_price_cent=280 WHERE symbol='SIM.HOLD'", brokerDb);
            const evidence = JSON.parse(command(path.join(build, 'm2_broker_cancel_fill_race'),
                [mode, String(accepted.order.order_id), String(accepted.broker.broker_order_id)],
                {env: {...process.env, TRADE_MYSQL_DATABASE: brokerDb}, timeout: 12000}));
            assert.equal(evidence.pool_size, 2);
            assert.equal(evidence.lock_wait.lock_type, 'RECORD');
            assert.notEqual(evidence.lock_wait.requesting_thread_id, evidence.lock_wait.blocking_thread_id);
            const current = snapshot(id, mode);
            assert.equal(current.broker.state, state === 9 ? 4 : 2);
            assert.equal(current.broker.version, 2);
            assert.equal(current.broker.filled, state === 9 ? 0 : 10);
            assert.equal(current.broker.price, state === 9 ? 0 : 280);
            assert.equal(current.order.state, 11);
            assert.deepEqual(current.account, accepted.account);
            assert.deepEqual(current.ledger, accepted.ledger);
            assert.equal(Number(sql("SELECT COUNT(*) FROM information_schema.TRIGGERS " +
                `WHERE TRIGGER_SCHEMA='${brokerDb}' AND TRIGGER_NAME='m2_cancel_fill_gate'`, '')), 0);
            return {evidence, snapshot: current};
        });
        let settled;
        await check(`${mode}: production services reconcile CANCEL_PENDING to the one broker terminal state`, async () => {
            await start('broker');
            await waitState(id, mode, state);
            settled = terminal(id, mode, state, spent);
            assert.equal(settled.broker.broker_order_id, accepted.broker.broker_order_id);
            assert.equal(settled.broker.version, 2);
            return settled;
        });
        await check(`${mode}: late cancel, same-key retry and repeated scans cannot alter the settlement`, async () => {
            const cancel = await rpc('cancel', id, mode);
            assert.equal(cancel.grpc_code, state === 9 ? 0 : 9);
            const retry = await submit(id, mode, 'SIM.HOLD');
            assert.equal(retry.state, state);
            assert.equal(retry.order_id, settled.order.order_id);
            await sleep(1000);
            assert.deepEqual(snapshot(id, mode), settled);
            return {cancel, retry};
        });
        await stop('broker');
    }
}

async function main() {
    try {
        assert.equal(await portOpen(13401), false, 'trade port occupied');
        assert.equal(await portOpen(3666), false, 'broker port occupied');
        assert.equal(await portOpen(3306), true, 'MySQL is not running');
        console.log('Building acceptance targets...');
        command('cmake', ['--build', build, '--target', 'trade_server', 'trade_broker', 'submit_order_acceptance', '-j2'], {stdio: 'inherit'});
        const flags = command('pkg-config', ['--cflags', '--libs', 'grpc++', 'protobuf']).trim().split(/\s+/);
        command(process.env.CXX || 'c++', ['-std=c++20', '-g', '-pthread', '-I' + path.join(build, 'generated'),
            path.join(root, 'test/m2_rpc_client.cpp'), path.join(build, 'libtrade_account_proto.a'),
            ...flags, '-o', path.join(build, 'm2_rpc_client')]);
        if (lostReply || cancelFill || remaining || snapshotRules) {
            const boost = process.env.BOOST188_ROOT || path.join(os.homedir(), '.local/boost-1.88.0');
            const logFlags = command('pkg-config', ['--cflags', '--libs', 'spdlog']).trim().split(/\s+/);
            const testName = cancelFill ? 'm2_broker_cancel_fill_race' : 'm2_lost_reply_broker';
            if (!snapshotRules) command(process.env.CXX || 'c++', ['-std=c++20', '-g', '-pthread',
                '-I' + path.join(build, 'generated'), '-I' + path.join(root, 'pub'),
                '-I' + path.join(root, 'broker/repository'), '-I' + path.join(boost, 'include'),
                path.join(root, `test/${testName}.cpp`),
                path.join(root, 'broker/repository/TradeBrokerRepository.cpp'),
                path.join(build, 'libtrade_broker_proto.a'), path.join(boost, 'lib/libboost_charconv.a'),
                ...flags, ...logFlags, '-lssl', '-lcrypto', '-lquadmath',
                '-o', path.join(build, testName)], {stdio: 'inherit', timeout: 240000});
            if (remaining || snapshotRules) {
                command(process.env.CXX || 'c++', ['-std=c++20', '-g', '-pthread',
                    '-I' + path.join(build, 'generated'), '-I' + path.join(root, 'pub'),
                    '-I' + path.join(root, 'server/repository'), '-I' + path.join(boost, 'include'),
                    path.join(root, 'test/m2_local_repository_probe.cpp'),
                    path.join(root, 'server/repository/TradeAccountRepository.cpp'),
                    path.join(build, 'libtrade_account_proto.a'), path.join(build, 'libtrade_broker_proto.a'),
                    path.join(boost, 'lib/libboost_charconv.a'),
                    ...flags, ...logFlags, '-lssl', '-lcrypto', '-lquadmath', '-o',
                    path.join(build, 'm2_local_repository_probe')], {stdio: 'inherit', timeout: 240000});
                if (remaining) command(process.env.CXX || 'c++', ['-std=c++20', '-g', '-pthread',
                    '-I' + path.join(build, 'generated'), path.join(root, 'test/m2_snapshot_broker.cpp'),
                    path.join(build, 'libtrade_broker_proto.a'), ...flags,
                    '-o', path.join(build, 'm2_snapshot_broker')], {stdio: 'inherit', timeout: 240000});
            }
        }
        for (const database of [tradeDb, brokerDb]) {
            sql(`CREATE DATABASE ${database} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin`, '');
            created.add(database);
            sql(`GRANT ALL PRIVILEGES ON ${database}.* TO 'a'@'%'`, '');
            grants.add(database);
        }
        for (const file of ['trade_accounts_schema.sql', 'trade_orders_schema.sql', 'trade_fund_ledger_schema.sql', 'trade_outbox_events_schema.sql']) {
            sql(fs.readFileSync(path.join(root, 'sql', file), 'utf8'));
        }
        for (const file of ['broker_orders.sql', 'broker_instruments.sql']) {
            sql(fs.readFileSync(path.join(root, 'sql', file), 'utf8'), brokerDb);
        }
        sql("INSERT INTO broker_instruments (symbol,display_name,simulated_price_cent,enabled) VALUES " +
            "('SIM.FILL','Fill fixture',280,1),('SIM.HOLD','Hold fixture',1000,1)," +
            "('SIM.RESTART','Restart fixture',1000,1),('SIM.DISABLED','Disabled fixture',280,0)", brokerDb);
        for (let id = 1; id <= 8; ++id) fixture(id);
        if (backoff) {
            await require('./m2_backoff_checks')({sql, jsonRows, start, stop, until, submit,
                waitState, terminal, account, check, brokerDb, report});
            return;
        }
        if (snapshotRules) {
            await require('./m2_snapshot_rules')({directory, build, tradeDb, report,
                command, snapshot, fixture, check});
            return;
        }
        if (remaining) {
            await require('./m2_remaining_checks')({directory, build, tradeDb, brokerDb, report,
                command, sql, jsonRows, start, stop, startProxy, startGate, until, sleep, rpc, account, fixture,
                localOrder, snapshot, waitState, submit, terminal, check, faultEvents});
            return;
        }
        if (lostReply) {
            await lostReplyChecks();
            return;
        }
        if (cancelFill) {
            await cancelFillChecks();
            return;
        }
        await start('broker');
        await start('trade');

        let filled;
        await check('normal RPC submission, automatic fill and four-table settlement with price improvement', async () => {
            const submitted = await submit(1, 'normal');
            const done = await waitState(1, 'normal', 8);
            assert.equal(done.order_id, submitted.order_id);
            filled = terminal(1, 'normal', 8, 2800);
            const balance = await rpc('account', 1);
            assert.equal(balance.grpc_code, 0);
            assert.equal(balance.available_cent, 7200);
            assert.equal(balance.frozen_cent, 0);
            assert.equal(balance.spent_cent, 2800);
            return filled;
        });
        await check('same-key same-parameters retry returns original terminal order without another debit', async () => {
            const repeated = await submit(1, 'normal');
            assert.equal(repeated.order_id, filled.order.order_id);
            assert.equal(repeated.state, 8);
            await sleep(650);
            assert.deepEqual(snapshot(1, 'normal'), filled);
            return repeated;
        });
        await check('same-key different-parameters retry is ALREADY_EXISTS without side effects', async () => {
            const reply = await rpc('submit', 1, 'normal', 'SIM.FILL', 301, 10);
            assert.equal(reply.grpc_code, 6);
            assert.deepEqual(snapshot(1, 'normal'), filled);
            return reply;
        });
        for (const [id, symbol] of [[2, 'SIM.MISSING'], [3, 'SIM.DISABLED']]) {
            await check(`${symbol}: broker rejection returns all frozen funds exactly once`, async () => {
                await submit(id, 'rejected', symbol);
                await waitState(id, 'rejected', 10);
                const before = terminal(id, 'rejected', 10);
                const repeated = await submit(id, 'rejected', symbol);
                assert.equal(repeated.state, 10);
                await sleep(650);
                assert.deepEqual(snapshot(id, 'rejected'), before);
                return before;
            });
        }
        await check('accepted but unfilled order can be cancelled remotely; repeated cancel does not refund twice', async () => {
            await submit(4, 'cancel', 'SIM.HOLD');
            await waitState(4, 'cancel', 7);
            assert.deepEqual(account(4), {initial: 10000, available: 7000, frozen: 3000, spent: 0});
            assert.equal((await rpc('cancel', 4, 'cancel')).grpc_code, 0);
            await waitState(4, 'cancel', 9);
            const before = terminal(4, 'cancel', 9);
            assert.equal((await rpc('cancel', 4, 'cancel')).state, 9);
            await sleep(650);
            assert.deepEqual(snapshot(4, 'cancel'), before);
            return before;
        });
        await check('cancel after fill is FAILED_PRECONDITION and cannot refund spent funds', async () => {
            const reply = await rpc('cancel', 1, 'normal');
            assert.equal(reply.grpc_code, 9);
            assert.deepEqual(snapshot(1, 'normal'), filled);
            return reply;
        });

        await stop('broker');
        await check('broker unavailable leaves DISPATCHING funds frozen rather than rejecting', async () => {
            await submit(5, 'recover');
            await waitState(5, 'recover', 6);
            await sleep(900);
            const current = snapshot(5, 'recover');
            assert.equal(current.order.state, 6);
            assert.equal(current.broker, null);
            assert.deepEqual(current.account, {initial: 10000, available: 7000, frozen: 3000, spent: 0});
            assert.deepEqual(current.ledger, [{type: 1, amount: 3000}]);
            return current;
        });
        await check('cancel while broker is unavailable remains CANCEL_PENDING with no premature refund', async () => {
            await submit(6, 'cancel-recover', 'SIM.HOLD');
            await waitState(6, 'cancel-recover', 6);
            assert.equal((await rpc('cancel', 6, 'cancel-recover')).state, 11);
            await sleep(650);
            const current = snapshot(6, 'cancel-recover');
            assert.equal(current.order.state, 11);
            assert.equal(current.broker, null);
            assert.deepEqual(current.account, {initial: 10000, available: 7000, frozen: 3000, spent: 0});
            assert.deepEqual(current.ledger, [{type: 1, amount: 3000}]);
            return current;
        });
        const beforeRecoveryId = localOrder(5, 'recover').order_id;
        await stop('trade', true);
        await start('broker');
        await start('trade');
        await check('gateway crash and restart recovers DISPATCHING through the same broker business key', async () => {
            await waitState(5, 'recover', 8);
            const recovered = terminal(5, 'recover', 8, 2800);
            assert.equal(recovered.order.order_id, beforeRecoveryId);
            assert.equal(recovered.broker.gateway_order_id, beforeRecoveryId);
            return recovered;
        });
        await check('persisted CANCEL_PENDING with remote NOT_FOUND recovers via same-key submit then cancel', async () => {
            await waitState(6, 'cancel-recover', 9);
            return terminal(6, 'cancel-recover', 9);
        });
        await check('broker crash preserves accepted order; restart and new price automatically finish it', async () => {
            await submit(7, 'broker-restart', 'SIM.RESTART');
            await waitState(7, 'broker-restart', 7);
            const before = snapshot(7, 'broker-restart');
            await stop('broker', true);
            await sleep(650);
            assert.equal(localOrder(7, 'broker-restart').state, 7);
            assert.deepEqual(account(7), before.account);
            sql("UPDATE broker_instruments SET simulated_price_cent=280 WHERE symbol='SIM.RESTART'", brokerDb);
            await start('broker');
            await waitState(7, 'broker-restart', 8);
            const after = terminal(7, 'broker-restart', 8, 2800);
            assert.equal(after.broker.broker_order_id, before.broker.broker_order_id);
            return {before, after};
        });
        await check('8 simultaneous submissions share a 10000-cent account without overspending', async () => {
            const replies = await Promise.all(Array.from({length: 8}, (_, i) =>
                rpc('submit', 8, `budget-${i}`, 'SIM.HOLD', 300, 10)));
            assert.equal(replies.filter(r => r.grpc_code === 0).length, 3);
            assert.equal(replies.filter(r => r.grpc_code === 9).length, 5);
            assert.deepEqual(account(8), {initial: 10000, available: 1000, frozen: 9000, spent: 0});
            assert.equal(Number(sql('SELECT COUNT(*) FROM orders WHERE account_id=8')), 3);
            assert.equal(Number(sql('SELECT COUNT(*) FROM fund_ledger WHERE account_id=8')), 3);
            return {replies, account: account(8)};
        });
        await check('settled orders remain immutable after recovery and repeated background scans', async () => {
            await sleep(650);
            assert.deepEqual(snapshot(1, 'normal'), filled);
            assert.equal(Number(sql('SELECT COUNT(*) FROM accounts WHERE initial_budget_cent <> available_cent + frozen_cent + spent_cent')), 0);
            return {normal: filled, budget_invariant_violations: 0};
        });
        await check('both production processes stop within 8 seconds with waiting orders present', async () => {
            return {trade: await stop('trade'), broker: await stop('broker')};
        });
    } catch (error) {
        report.failure = error.message;
        throw error;
    } finally {
        cleaningUp = true;
        for (const name of [...servers.keys()]) {
            try {if (servers.has(name)) report.cleanup.push(await stop(name));}
            catch (error) {report.cleanup.push({error: error.message}); process.exitCode = 1;}
        }
        for (const database of created) {
            try {
                if (grants.has(database)) sql(`REVOKE ALL PRIVILEGES ON ${database}.* FROM 'a'@'%'`, '');
                sql(`DROP DATABASE ${database}`, '');
                report.cleanup.push({database, removed: true});
            } catch (error) {report.cleanup.push({database, removed: false, error: error.message}); process.exitCode = 1;}
        }
        report.finished_at = new Date().toISOString();
        fs.writeFileSync(path.join(directory, 'report.json'), JSON.stringify(report, null, 2));
        console.log(`RESULT: ${report.cases.filter(c => c.passed).length}/${report.cases.length} passed`);
        console.log('Report: ' + path.join(directory, 'report.json'));
    }
}

main().catch(error => {console.error(error); process.exitCode = 1;});
