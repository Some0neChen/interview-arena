'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');

module.exports = async function backoffChecks(c) {
    const {sql, jsonRows, start, stop, until, submit, waitState, terminal, account, check, brokerDb, report} = c;
    // The trigger observes production writes using the same MySQL statement clock;
    // client polling latency must not be mistaken for a change in the backoff.
    sql(`CREATE TABLE retry_trace (seq BIGINT PRIMARY KEY AUTO_INCREMENT, oid BIGINT,
        n BIGINT, at_ms DATETIME(3), old_due DATETIME(3), due DATETIME(3))`);
    sql(`CREATE TRIGGER trace_retry AFTER UPDATE ON orders FOR EACH ROW BEGIN
        IF OLD.retry_count <> NEW.retry_count OR NOT (OLD.next_retry_at <=> NEW.next_retry_at) THEN
            INSERT INTO retry_trace (oid,n,at_ms,old_due,due) VALUES
                (NEW.order_id,NEW.retry_count,CURRENT_TIMESTAMP(3),OLD.next_retry_at,NEW.next_retry_at);
        END IF; END`);
    const trace = oid => jsonRows(`SELECT JSON_OBJECT('count',n,
        'delay_ms',TIMESTAMPDIFF(MICROSECOND,at_ms,due) DIV 1000,
        'past_old_due_ms',TIMESTAMPDIFF(MICROSECOND,old_due,at_ms) DIV 1000)
        FROM retry_trace WHERE oid=${oid} ORDER BY seq`);
    const meta = oid => jsonRows(`SELECT JSON_OBJECT('count',retry_count,'due',next_retry_at)
        FROM orders WHERE order_id=${oid}`)[0];

    await start('trade');
    const {order_id: oid} = await submit(1, 'outage');
    await check('broker offline: four consecutive attempts schedule 1/2/4/8 seconds without refund', async () => {
        await until(() => meta(oid).count >= 4, 'four retries');
        const rows = trace(oid);
        assert.deepEqual(rows.map(r => r.count), [1, 2, 3, 4]);
        assert.deepEqual(rows.map(r => r.delay_ms), [1000, 2000, 4000, 8000]);
        assert.deepEqual(account(1), {initial: 10000, available: 7000, frozen: 3000, spent: 0});
        return rows;
    });
    await check('restart preserves due time; outage reaches 16/30/30 seconds without premature retry', async () => {
        const before = meta(oid);
        await stop('trade', true);
        await start('trade');
        assert.deepEqual(meta(oid), before);
        await until(() => meta(oid).count >= 7, 'real-time capped retry', 70000);
        const rows = trace(oid);
        assert.deepEqual(rows.map(r => r.count), [1, 2, 3, 4, 5, 6, 7]);
        assert.deepEqual(rows.map(r => r.delay_ms), [1000, 2000, 4000, 8000, 16000, 30000, 30000]);
        assert.ok(rows.slice(1).every(r => r.past_old_due_ms >= 0), JSON.stringify(rows));
        return {before, rows};
    });
    let other;
    await check('another order gets its own attempt and first retry while the original waits 30s', async () => {
        other = (await submit(2, 'other')).order_id;
        await until(() => meta(other).count >= 1, 'separate order attempted while offline');
        assert.equal(meta(oid).count, 7);
        assert.equal(Number(sql(`SELECT next_retry_at > CURRENT_TIMESTAMP(3) FROM orders WHERE order_id=${oid}`)), 1);
        assert.equal(trace(other)[0].delay_ms, 1000);
        return {other: trace(other), original: meta(oid)};
    });
    await start('broker');
    await check('broker recovery clears retry metadata and settles the original frozen funds once', async () => {
        // A listening port does not imply this gateway's existing gRPC channel is ready.
        await until(() => Number(sql(`SELECT COUNT(*) FROM orders WHERE order_id IN (${oid},${other})
            AND state=8 AND retry_count=0 AND next_retry_at IS NULL`)) === 2, 'both orders recover without restarting trade', 180000);
        const rows = trace(oid);
        assert.ok(rows.some(r => r.count === 0 && r.delay_ms === null));
        return {rows, settlement: terminal(1, 'outage', 8, 2800), other: terminal(2, 'other', 8, 2800)};
    });
    let unchangedFailure;
    await check('unchanged ACCEPTED snapshots also back off; a subsequent fill clears the wait', async () => {
        const {order_id} = await submit(3, 'unchanged', 'SIM.HOLD');
        await waitState(3, 'unchanged', 7);
        try {
            await until(() => meta(order_id).count >= 2, 'two valid unchanged snapshots', 10000);
        } catch (error) {
            report.unchanged_observation = {order_id, ...meta(order_id), trace: trace(order_id),
                local: sql(`SELECT state,broker_version FROM orders WHERE order_id=${order_id}`),
                remote: sql(`SELECT state,broker_version FROM broker_orders WHERE gateway_order_id=${order_id}`, brokerDb)};
            throw error;
        }
        assert.deepEqual(trace(order_id).slice(0, 2).map(r => r.delay_ms), [1000, 2000]);
        sql("UPDATE broker_instruments SET simulated_price_cent=280 WHERE symbol='SIM.HOLD'", brokerDb);
        await waitState(3, 'unchanged', 8);
        await until(() => meta(order_id).count === 0, 'clear after progress');
        assert.equal(meta(order_id).due, null);
        return {rows: trace(order_id), settlement: terminal(3, 'unchanged', 8, 2800)};
    }).catch(error => { unchangedFailure = error; }); // The next case has independent accounts.
    await check('one order failing both settlement and retry writes does not abandon the next order', async () => {
        const first = await submit(4, 'fault', 'SIM.RESTART');
        const second = await submit(5, 'next', 'SIM.RESTART');
        await waitState(4, 'fault', 7);
        await waitState(5, 'next', 7);
        await stop('trade');
        // Make both due in the same initial scan; only the first row rejects writes.
        sql('UPDATE orders SET retry_count=0,next_retry_at=NULL WHERE account_id IN (4,5)');
        sql(`CREATE TRIGGER fail_settle BEFORE UPDATE ON orders FOR EACH ROW BEGIN
            IF OLD.account_id=4 AND NEW.state=8 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='m2-settle-fault'; END IF; END`);
        sql(`CREATE TRIGGER fail_retry BEFORE UPDATE ON orders FOR EACH ROW BEGIN
            IF OLD.account_id=4 AND NEW.retry_count>OLD.retry_count THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='m2-retry-fault'; END IF; END`);
        sql("UPDATE broker_instruments SET simulated_price_cent=280 WHERE symbol='SIM.RESTART'", brokerDb);
        await until(() => Number(sql(`SELECT COUNT(*) FROM broker_orders WHERE gateway_order_id IN
            (${first.order_id},${second.order_id}) AND state=2`, brokerDb)) === 2, 'two filled broker orders');
        await start('trade');
        await waitState(5, 'next', 8);
        terminal(5, 'next', 8, 2800);
        assert.deepEqual(meta(first.order_id), {count: 0, due: null});
        assert.equal(account(4).frozen, 3000);
        const log = report.processes.filter(p => p.name === 'trade' && p.log).at(-1).log;
        const evidence = fs.readFileSync(log, 'utf8');
        // The coroutine surfaces MySQL 1644, not necessarily the SQL SIGNAL text.
        const faultLines = evidence.split('\n').filter(line => line.includes('1644'));
        assert.ok(faultLines.some(line => line.includes(`handle id:${first.order_id},`)), evidence);
        assert.ok(faultLines.some(line => line.includes(`update retry info for id:${first.order_id},`)), evidence);
        sql('DROP TRIGGER fail_retry');
        await until(() => meta(first.order_id).count > 0, 'handler exception persisted as a retry');
        sql('DROP TRIGGER fail_settle');
        await waitState(4, 'fault', 8);
        await until(() => meta(first.order_id).count === 0, 'clear retry after removing fault');
        return {log, faultLines, recovered: terminal(4, 'fault', 8, 2800)};
    });
    if (unchangedFailure) throw unchangedFailure;
};
