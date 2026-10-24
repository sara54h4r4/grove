<?php
declare(strict_types=1);
// Run only inside the disposable fixture, never on a hosting account.
require '/fixture/.grove/Store.php';
$config = require '/fixture/.grove/config.php';
$config['repository_id'] = 'bbbbbbbb-2222-4444-8888-bbbbbbbbbbbb';
$store = new GroveStore($config);
$store->initialize();
function check(bool $ok, string $label): void {
    if (!$ok) throw new RuntimeException('fixture assertion: '.$label);
}
function record(string $table, array $data, int $keys = 1): array {
    return ['table_name'=>$table,'row_key'=>array_slice($data,0,$keys),'data'=>$data];
}
function manifest(array $records): array {
    $tables = array_fill_keys(GroveStore::TABLES,[]);
    foreach ($records as $r) {
        $tables[$r['table_name']][hash('sha256',GroveStore::encode($r['row_key']))] = hash('sha256',GroveStore::encode($r['data']));
    }
    $result = [];
    foreach ($tables as $table=>$rows) {
        ksort($rows);
        $result[$table] = ['rows'=>count($rows),'sha256'=>hash('sha256',implode('',$rows))];
    }
    return $result;
}
$old = (int)(microtime(true)*1000)-9*86400000;
$records = [record('coord_meta',['repository_id',$config['repository_id']]),
    record('coord_events',[42,'message','codex:a','codex:b','["."]','{}',$old]),
    record('coord_messages',['acked','codex:a','codex:b','old-acked','{}',$old,$old,$old]),
    record('coord_messages',['unread','codex:a','codex:b','old-unread','{}',$old,null,null]),
    record('coord_decisions',['policy',1,'["."]','keep policy','operator','codex:a','approved','operator',$old,$old],2),
    record('coord_claims',['expired','codex:a','src/a',1,$old,0])];
$expected = manifest($records);
$store->beginImport(100,42,50);
try { $store->check(0); throw new RuntimeException('fixture: incomplete import was served'); }
catch (GroveStoreError $e) { check(str_contains($e->getMessage(),'paused'),'loading gates readers'); }
$store->importRows($records);
$store->importRows($records); // Idempotent page retry.
$bad = $expected; $bad['coord_messages']['rows']++;
try { $store->finishImport($bad); throw new RuntimeException('fixture: corrupt manifest accepted'); }
catch (GroveStoreError $e) { check(str_contains($e->getMessage(),'verification'),'manifest blocks promotion'); }
$store->finishImport($expected);
check($store->fingerprint()[0][2]===$expected,'all eight imported table hashes');
check($store->check(0)[0][0]===101,'source revision retained');
try { $store->beginImport(100,42,50); throw new RuntimeException('fixture: overwrite accepted'); }
catch (GroveStoreError $e) { check(str_contains($e->getMessage(),'empty'),'nonempty target protected'); }
$before = $store->fingerprint();
check($store->commit(99,42,true,[record('coord_meta',['must-not-exist','x'])])===[],'losing CAS has no response');
check($store->fingerprint()===$before,'losing CAS writes no rows');
try {
    $store->commit(101,42,true,[record('coord_meta',['must-not-exist','x']),
        record('coord_messages',['collision','codex:a','codex:b','old-unread','{}',$old,null,null])]);
    throw new RuntimeException('fixture: conflicting unique key accepted');
} catch (PDOException $e) {}
check($store->fingerprint()===$before,'unique-key failure rolls back the whole patch');
$store->prune(true);
$after = $store->fingerprint();
check($after[0][1]===42,'pruning preserves global event watermark');
check($store->check(0)[0][4]===42,'pruning records history gap');
check($after[0][2]['coord_messages']['rows']===1,'unread old message retained');
check($after[0][2]['coord_decisions']['rows']===1,'approved old policy retained');
check($after[0][2]['coord_events']['rows']===0,'old event removed');
check($after[0][2]['coord_claims']['rows']===0,'expired old lease removed');
echo "BLUEHOST_STORAGE_RECEIPT ".json_encode(['import_hashes'=>8,'loading_gate'=>true,
    'overwrite_refused'=>true,'atomic_rollback'=>true,'prune_keeps_unread_and_authority'=>true,
    'event_watermark_preserved'=>true])."\n";
