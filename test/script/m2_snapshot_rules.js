'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');

module.exports = async function snapshotRules(c) {
    const {build, tradeDb, report, command, snapshot, fixture, check} = c;
    const probe = (mode, id, data = {}) => JSON.parse(command(
        path.join(build, 'm2_local_repository_probe'), [mode, String(id), '3306'],
        {env: {...process.env, TRADE_MYSQL_DATABASE: tradeDb}, input: JSON.stringify(data), timeout: 15000}));
    const run = async (name, test) => {
        try { await check(name, test); }
        catch { process.exitCode = 1; }
    };
    const seed = id => {
        fixture(id);
        return {id, key: `probe-${id}`, order: probe('seed', id).order_id};
    };
    const read = f => snapshot(f.id, f.key);
    const apply = (f, fields) => {
        const result = probe('snapshot', f.order, fields);
        assert.equal(result.threw, false, JSON.stringify(result));
        return result;
    };
    const unchanged = (f, fields, rejected) => {
        const before = read(f);
        const call = apply(f, fields);
        const after = read(f);
        assert.deepEqual(after, before, 'Rejected or repeated snapshots must not change any stored record');
        if (rejected) assert.match(call.logs, /broker/, 'Invalid snapshot must be diagnosed, not silently treated as a replay');
        else assert.equal(call.logs.trim(), '', 'Valid old/repeated snapshots are not protocol errors');
        return {call, before, after};
    };

    const f = seed(20);
    await run('first ACCEPTED snapshot is persisted with funds still frozen', () => {
        const call = apply(f, {broker_state: 1, broker_version: 1});
        const after = read(f);
        assert.equal(after.order.state, 7);
        assert.equal(after.order.broker_order_id, f.order + 10000);
        assert.equal(after.order.broker_version, 1);
        assert.equal(after.order.broker_state, 1);
        assert.equal(after.order.filled_quantity, 0);
        assert.equal(after.order.fill_price_cent, 0);
        assert.deepEqual(after.account, {initial: 10000, available: 7000, frozen: 3000, spent: 0});
        return {call, after};
    });
    await run('same-version ACCEPTED replay is a no-op', () =>
        unchanged(f, {broker_state: 1, broker_version: 1}, false));
    await run('same-version changed reason is diagnosed even when local state is unchanged', () =>
        unchanged(f, {broker_state: 1, broker_version: 1, reject_reason: 'changed content'}, true));
    await run('same-version ACCEPTED-to-FILLED conflict cannot settle', () =>
        unchanged(f, {broker_state: 2, broker_version: 1}, true));
    await run('higher-version ACCEPTED-to-ACCEPTED is rejected by the current broker contract', () =>
        unchanged(f, {broker_state: 1, broker_version: 2}, true));
    await run('newer FILLED snapshot settles once and persists complete execution facts', () => {
        const call = apply(f, {broker_state: 2, broker_version: 2});
        const after = read(f);
        assert.equal(after.order.state, 8);
        assert.equal(after.order.broker_version, 2);
        assert.equal(after.order.broker_state, 2);
        assert.equal(after.order.filled_quantity, 10);
        assert.equal(after.order.fill_price_cent, 280);
        assert.deepEqual(after.account, {initial: 10000, available: 7200, frozen: 0, spent: 2800});
        assert.deepEqual(after.ledger, [{type: 1, amount: 3000}, {type: 3, amount: 2800}, {type: 5, amount: 200}]);
        return {call, after};
    });
    await run('same-version FILLED replay is a no-op', () =>
        unchanged(f, {broker_state: 2, broker_version: 2}, false));
    await run('older ACCEPTED snapshot cannot undo FILLED', () =>
        unchanged(f, {broker_state: 1, broker_version: 1}, false));
    await run('same-version changed fill price is diagnosed after local settlement', () =>
        unchanged(f, {broker_state: 2, broker_version: 2, fill_price_cent: 290}, true));
    await run('newer CANCELLED snapshot cannot undo FILLED', () =>
        unchanged(f, {broker_state: 4, broker_version: 3}, true));
    await run('same-version wrong account is diagnosed before replay classification', () =>
        unchanged(f, {broker_state: 2, broker_version: 2, account_id: 999}, true));

    const direct = seed(21);
    await run('first complete snapshot may be FILLED without consuming ACCEPTED first', () => {
        const call = apply(direct, {broker_state: 2, broker_version: 2});
        const after = read(direct);
        assert.equal(after.order.state, 8);
        assert.equal(after.order.broker_version, 2);
        assert.equal(after.order.broker_order_id, direct.order + 10000);
        assert.deepEqual(after.account, {initial: 10000, available: 7200, frozen: 0, spent: 2800});
        return {call, after};
    });
    for (const [id, state, name] of [[22, 1, 'ACCEPTED'], [23, 4, 'CANCELLED'], [24, 3, 'REJECTED']]) {
        const invalid = seed(id);
        await run(`${name} carrying nonzero fill data is rejected without even persisting a first binding`, () =>
            unchanged(invalid, {broker_state: state, broker_version: 2, filled_quantity: 10, fill_price_cent: 280}, true));
    }
    report.snapshot_rules_boundary = 'Complete known-enum synthetic replies to the production Repository; no live broker, no partial fills, no transport validation claim';
};
