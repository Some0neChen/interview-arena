'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');

function control(request) {
    return new Promise((resolve, reject) => {
        const socket = net.connect({host: '127.0.0.1', port: 13308});
        let text = '';
        socket.setTimeout(3000, () => socket.destroy(new Error('Proxy control timeout')));
        socket.on('error', reject);
        socket.on('connect', () => socket.write(JSON.stringify(request) + '\n'));
        socket.on('data', chunk => {
            text += chunk;
            if (!text.includes('\n')) return;
            socket.end();
            try {
                const reply = JSON.parse(text.split('\n')[0]);
                assert.equal(reply.ok, true, text);
                resolve(reply);
            } catch (error) { reject(error); }
        });
    });
}

module.exports = async function remainingChecks(c) {
    const {directory, build, tradeDb, report, command, sql, start, stop, startProxy, startGate,
        until, sleep, rpc, account, fixture, snapshot, waitState, submit, terminal, check, faultEvents} = c;
    const probe = (mode, id, port = 3306) => JSON.parse(command(path.join(build, 'm2_local_repository_probe'),
        [mode, String(id), String(port)], {env: {...process.env, TRADE_MYSQL_DATABASE: tradeDb}, timeout: 15000}));
    const frozen = {initial: 10000, available: 7000, frozen: 3000, spent: 0};
    const settled = {initial: 10000, available: 7200, frozen: 0, spent: 2800};
    for (const [id, mode, expectedState] of [[1, 'cancel-first', 9], [2, 'prepare-first', 11]]) {
        await check(`local ${mode}: competing real SQL transactions cannot both refund and gain dispatch rights`, async () => {
            const evidence = probe(mode, id);
            assert.equal(evidence.prepare_granted, mode === 'prepare-first');
            assert.equal(evidence.cancel_state, expectedState);
            assert.notEqual(evidence.lock_wait.requester, evidence.lock_wait.blocker);
            const data = snapshot(id, `probe-${id}`);
            assert.equal(data.order.state, expectedState);
            assert.deepEqual(data.account, mode === 'cancel-first' ?
                {initial: 10000, available: 10000, frozen: 0, spent: 0} : frozen);
            assert.deepEqual(data.ledger, mode === 'cancel-first' ?
                [{type: 1, amount: 3000}, {type: 2, amount: 3000}] : [{type: 1, amount: 3000}]);
            assert.deepEqual(data.outbox.map(e => e.type), mode === 'cancel-first' ? [1, 2] : [1, 6]);
            return {evidence, snapshot: data};
        });
    }

    await startProxy();
    for (const [id, mode] of [[3, 'before_commit'], [4, 'drop_commit_ack']]) {
        const seeded = probe('seed', id);
        const before = snapshot(id, `probe-${id}`);
        await check(`settlement ${mode}: SQL fault leaves either zero or one complete settlement`, async () => {
            await control({command: 'arm', mode, case: `m2-settlement-${mode}`});
            const result = probe('settle', seeded.order_id, 13307);
            const fault = await control({command: 'status'});
            assert.equal(fault.armed, null, 'Fault did not intercept COMMIT');
            assert.ok(fault.events.some(e => e.event === (mode === 'before_commit' ? 'commit_not_forwarded' : 'commit_ok_dropped')));
            assert.ok(!fault.events.some(e => e.event === 'proxy_error'));
            assert.equal(result.threw, true, 'Lost connection must not look like a confirmed successful call');
            const after = snapshot(id, `probe-${id}`);
            if (mode === 'before_commit') assert.deepEqual(after, before);
            else {
                assert.equal(after.order.state, 8);
                assert.deepEqual(after.account, settled);
                assert.deepEqual(after.ledger, [{type: 1, amount: 3000}, {type: 3, amount: 2800}, {type: 5, amount: 200}]);
                assert.deepEqual(after.outbox.map(e => e.type), [1, 6, 5]);
            }
            return {call: result, proxy: fault.events, before, after};
        });
        await check(`${mode}: retry applies FILLED directly from DISPATCHING or leaves committed result unchanged`, async () => {
            assert.equal(probe('settle', seeded.order_id).threw, false);
            const after = snapshot(id, `probe-${id}`);
            assert.equal(after.order.state, 8);
            assert.deepEqual(after.account, settled);
            assert.deepEqual(after.ledger, [{type: 1, amount: 3000}, {type: 3, amount: 2800}, {type: 5, amount: 200}]);
            assert.deepEqual(after.outbox.map(e => e.type), [1, 6, 5]);
            for (const replay of ['settle', 'accepted', 'cancelled']) {
                assert.equal(probe(replay, seeded.order_id).threw, false);
                assert.deepEqual(snapshot(id, `probe-${id}`), after);
            }
            return {snapshot: after, replayed_targets: ['FILLED', 'BROKER_ACCEPTED', 'CANCELLED'],
                boundary: 'State-based duplicate protection; not broker version/content comparison'};
        });
    }
    await stop('proxy');

    for (const [id, mode, state, ledgerType, eventType] of [[10, 'cancelled', 9, 2, 2], [11, 'rejected', 10, 4, 4]]) {
        fixture(id);
        await check(`${mode}: replaying the same settlement operation releases funds exactly once`, async () => {
            const seeded = probe('seed', id);
            assert.equal(probe(mode, seeded.order_id).threw, false);
            const after = snapshot(id, `probe-${id}`);
            assert.equal(after.order.state, state);
            assert.deepEqual(after.account, {initial: 10000, available: 10000, frozen: 0, spent: 0});
            assert.deepEqual(after.ledger, [{type: 1, amount: 3000}, {type: ledgerType, amount: 3000}]);
            assert.deepEqual(after.outbox.map(e => e.type), [1, 6, eventType]);
            for (const replay of [mode, mode, 'accepted', 'settle']) {
                assert.equal(probe(replay, seeded.order_id).threw, false);
                assert.deepEqual(snapshot(id, `probe-${id}`), after);
            }
            return after;
        });
    }

    const prepared = probe('seed', 5);
    await check('restart after durable DISPATCHING with no RPC ever sent recovers the same business key', async () => {
        assert.equal(snapshot(5, 'probe-5').broker, null);
        await start('broker');
        await start('trade');
        await waitState(5, 'probe-5', 8);
        const data = terminal(5, 'probe-5', 8, 2800);
        assert.equal(data.order.order_id, prepared.order_id);
        assert.equal(data.broker.gateway_order_id, prepared.order_id);
        return {snapshot: data, boundary: 'Real Repository prepares and exits normally before production startup; not instruction-level SIGKILL injection'};
    });
    // The prepare-first race also persisted a cancellation intent without sending an RPC.
    await check('prepare-first race cancellation intent survives restart and settles only the real broker result', async () => {
        await until(() => [8, 9].includes(c.localOrder(2, 'probe-2')?.state), 'pending cancellation recovery');
        const state = c.localOrder(2, 'probe-2').state;
        return terminal(2, 'probe-2', state, state === 8 ? 2800 : 0);
    });
    await stop('trade');
    await stop('broker');

    await check('SIGKILL after real FILLED reply but before local settlement commits preserves all four tables', async () => {
        const gate = `settle_${tradeDb}`;
        await startGate();
        sql(`CREATE TRIGGER m2_settlement_gate BEFORE UPDATE ON orders FOR EACH ROW BEGIN
            DECLARE acquired INT DEFAULT 0;
            IF OLD.account_id=7 AND NEW.state=8 THEN
                SET acquired=GET_LOCK('${gate}',8);
                IF acquired IS NULL OR acquired<>1 THEN SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT='settlement gate timed out'; END IF;
                DO RELEASE_LOCK('${gate}');
            END IF; END`);
        await start('broker');
        await start('trade');
        await submit(7, 'crash-settle');
        await until(() => Number(sql(`SELECT COUNT(*) FROM performance_schema.metadata_locks
            WHERE OBJECT_TYPE='USER LEVEL LOCK' AND OBJECT_NAME='${gate}' AND LOCK_STATUS='PENDING'`, '')) === 1,
            'real dispatcher settlement reached its UPDATE');
        const pending = snapshot(7, 'crash-settle');
        assert.equal(pending.broker.state, 2, 'Broker must have actually filled before local crash');
        assert.ok([6, 7].includes(pending.order.state));
        assert.deepEqual(pending.account, frozen);
        const killed = await stop('trade', true);
        await stop('gate');
        await until(() => Number(sql(`SELECT COUNT(*) FROM information_schema.PROCESSLIST
            WHERE DB='${tradeDb}' AND USER='a'`, '')) === 0, 'dead gateway database sessions released');
        assert.deepEqual(snapshot(7, 'crash-settle'), pending);
        sql('DROP TRIGGER m2_settlement_gate');
        await start('trade');
        await waitState(7, 'crash-settle', 8);
        const recovered = terminal(7, 'crash-settle', 8, 2800);
        assert.equal(recovered.broker.broker_order_id, pending.broker.broker_order_id);
        return {killed, pending, recovered};
    });
    await stop('trade');
    await stop('broker');

    await start('broker', true);
    await start('trade');
    await submit(6, 'inflight-stop');
    await until(() => faultEvents().some(e => e.event === 'committed_reply_withheld'), 'in-flight RPC committed');
    await check('SIGTERM while broker reply is held drains the in-flight callback within the bound', async () => {
        assert.ok(!faultEvents().some(e => e.event === 'withheld_call_finished'), 'RPC must still be pending before stop');
        const stopped = await stop('trade');
        await until(() => faultEvents().some(e => e.event === 'withheld_call_finished'), 'broker observes RPC cancellation/deadline');
        const data = snapshot(6, 'inflight-stop');
        assert.equal(data.order.state, 6);
        assert.equal(data.broker.state, 1);
        assert.deepEqual(data.account, frozen);
        return {stopped, events: faultEvents(), snapshot: data};
    });
    await stop('broker');
    await check('restart after in-flight shutdown confirms and settles the persisted broker order', async () => {
        const before = snapshot(6, 'inflight-stop');
        await start('broker');
        await start('trade');
        await waitState(6, 'inflight-stop', 8);
        const after = terminal(6, 'inflight-stop', 8, 2800);
        assert.equal(after.broker.broker_order_id, before.broker.broker_order_id);
        return after;
    });
    await stop('trade');
    await stop('broker');

    await start('broker', 'snapshot');
    await start('trade');
    const events = () => fs.readFileSync(path.join(directory, 'snapshot-events.jsonl'), 'utf8')
        .split('\n').slice(0, -1).filter(Boolean).map(line => JSON.parse(line));
    const modes = ['GOOD', 'BAD.NAMESPACE', 'BAD.ORDER', 'BAD.ACCOUNT', 'BAD.SYMBOL', 'BAD.QUANTITY',
        'BAD.LIMIT', 'BAD.BROKERID', 'BAD.ZEROID', 'BAD.VERSION', 'BAD.PARTIAL', 'BAD.PRICE',
        'BAD.STATE', 'BAD.ZEROQTY', 'BAD.OVERBUDGET', 'BAD.OVERFLOW'];
    for (let i = 0; i < modes.length; ++i) {
        const mode = modes[i], id = 20 + i, key = `snapshot-${i}`;
        fixture(id);
        await submit(id, key, mode);
        await until(() => events().some(e => e.mode === mode), `${mode} snapshot received`);
        await sleep(800);
        const observed = snapshot(id, key);
        report.snapshot_observations ??= [];
        report.snapshot_observations.push({mode, snapshot: observed, sent: events().find(e => e.mode === mode)});
        try {
            await check(`${mode}: ${mode === 'GOOD' ? 'valid snapshot settles' : 'invalid snapshot cannot settle funds'}`, async () => {
                if (mode === 'GOOD') {
                    assert.equal(observed.order.state, 8);
                    assert.deepEqual(observed.account, settled);
                } else {
                    // Submit only binds the broker ID. No valid full snapshot has arrived.
                    assert.equal(observed.order.state, 6);
                    assert.equal(observed.order.broker_order_id, observed.order.order_id + 10000);
                    assert.equal(observed.order.broker_version, 0);
                    assert.equal(observed.order.broker_state, 0);
                    assert.equal(observed.order.filled_quantity, 0);
                    assert.equal(observed.order.fill_price_cent, 0);
                    assert.equal(observed.order.reject_reason, '');
                    assert.deepEqual(observed.account, frozen);
                    assert.deepEqual(observed.ledger, [{type: 1, amount: 3000}]);
                    assert.deepEqual(observed.outbox.map(e => e.type), [1, 6]);
                }
                return {snapshot: observed};
            });
        } catch (error) {
            // Keep the failing counterexample and continue the remaining independent probes.
            console.error(`FAIL: ${mode}: ${error.message}`);
        }
    }
    const failures = report.cases.filter(item => !item.passed);
    report.audit_cases_completed = true;
    if (failures.length) throw new Error(`${failures.length} M2 acceptance requirements failed; see snapshot_observations`);
};
