<?php
declare(strict_types=1);

final class GroveStoreError extends RuntimeException {}

// Only storage and transaction boundaries live here. Grove's existing Rust
// state machine continues to validate every agent operation and authority.
final class GroveStore
{
    public const TABLES = ['coord_meta','coord_sessions','coord_messages',
        'coord_decisions','coord_decision_acks','coord_claims','coord_transfers','coord_events'];
    private PDO $db;
    private string $repo;
    private int $days;
    private ?int $maintainedAt = null;
    public int $rowsReturned = 0;

    public function __construct(array $config)
    {
        $this->repo = $config['repository_id'];
        $this->days = (int)($config['retention_days'] ?? 7);
        if ($this->days < 1 || $this->days > 90) {
            throw new GroveStoreError('invalid retention configuration');
        }
        $this->db = new PDO($config['dsn'], $config['username'], $config['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_TIMEOUT => 5,
        ]);
        $this->db->exec('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ');
    }

    public static function encode(mixed $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_LINE_TERMINATORS);
    }

    private function query(string $sql, array $params = []): array
    {
        $stmt = $this->db->prepare($sql);
        $stmt->execute($params);
        $rows = $stmt->columnCount() ? $stmt->fetchAll() : [];
        $this->rowsReturned += count($rows);
        return $rows;
    }

    private function transaction(callable $work): mixed
    {
        $this->db->beginTransaction();
        try {
            $result = $work();
            $this->db->commit();
            return $result;
        } catch (Throwable $error) {
            if ($this->db->inTransaction()) $this->db->rollBack();
            throw $error;
        }
    }

    private function state(bool $lock = false, int $version = 2): array
    {
        $rows = $this->query('SELECT * FROM grove_store_state WHERE repository_id=?'
            . ($lock ? ' FOR UPDATE' : ''), [$this->repo]);
        if (!$rows || (int)$rows[0]['storage_version'] !== $version) {
            throw new GroveStoreError('store is not initialized or is paused for migration');
        }
        $this->maintainedAt = (int)$rows[0]['maintained_at'];
        return $rows[0];
    }

    public function initialize(): array
    {
        // DDL has implicit commit semantics in MySQL; do it before any data tx.
        foreach (explode(';', file_get_contents(__DIR__.'/schema.sql')) as $sql) {
            if (trim($sql) !== '') $this->db->exec($sql);
        }
        return $this->transaction(function () {
            $this->query('INSERT IGNORE INTO grove_store_state(repository_id,maintained_at) VALUES(?,?)',
                [$this->repo, (int)(microtime(true)*1000)]);
            $state = $this->state(true);
            $key = ['repository_id'];
            if (!$this->keyRows('coord_meta', $key)) {
                $this->put('coord_meta', $key, ['repository_id',$this->repo], (int)$state['revision']);
            }
            return [];
        });
    }

    private function keyRows(string $table, array $key): array
    {
        return $this->query('SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name=? AND row_hash=?',
            [$this->repo,$table,hash('sha256',self::encode($key),true)]);
    }

    private function records(array $rows): array
    {
        return array_map(fn($r) => json_decode($r['data_json'], true, 512, JSON_THROW_ON_ERROR), $rows);
    }

    private function tableRows(string $table): array
    {
        return $this->query('SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name=?',
            [$this->repo,$table]);
    }

    public function load(array $command): array
    {
        return $this->transaction(function () use ($command) {
            $g = $this->state();
            $op = $command['op'] ?? '';
            $id = $command['session_id'] ?? '';
            $tables = array_fill_keys(self::TABLES, []);
            $tables['coord_meta'] = [['repository_id',$this->repo]];
            $viewer = $this->records($this->keyRows('coord_sessions',[$id]));
            $allPeers = in_array($op,['register','sync','claim','capture_transfer',
                'get_transfer','check_transfer','review_transfer'],true);
            $tables['coord_sessions'] = $allPeers ? $this->records($this->tableRows('coord_sessions')) : $viewer;
            if ($op === 'send' && ($command['to'] ?? '') !== $id) {
                $tables['coord_sessions'] = array_merge($tables['coord_sessions'],
                    $this->records($this->keyRows('coord_sessions',[$command['to'] ?? ''])));
            }
            if ($op === 'sync') {
                $tables['coord_messages'] = $this->records($this->query(
                    "SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name='coord_messages' AND recipient=? AND acked_at IS NULL ORDER BY created_at,CAST(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$[0]')) AS BINARY) LIMIT 100",[$this->repo,$id]));
            } elseif ($op === 'send') {
                $tables['coord_messages'] = $this->records($this->query(
                    "SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name='coord_messages' AND sender=? AND request_key=?",[$this->repo,$id,$command['key'] ?? '']));
            } elseif ($op === 'ack_message') {
                $tables['coord_messages'] = $this->records($this->keyRows('coord_messages',[$command['message_id'] ?? '']));
            }
            if ($op === 'sync') {
                $tables['coord_decisions'] = $this->records($this->tableRows('coord_decisions'));
            } elseif (in_array($op,['propose','approve','ack_decision'],true)) {
                $tables['coord_decisions'] = $this->records($this->query(
                    "SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name='coord_decisions' AND decision_id=?",[$this->repo,$command['decision_id'] ?? '']));
            }
            if (in_array($op,['sync','ack_decision'],true)) {
                // Small exact per-session ACK set; scope/revision authority is
                // checked by the Rust engine using the selected decisions.
                $tables['coord_decision_acks'] = array_values(array_filter(
                    $this->records($this->tableRows('coord_decision_acks')),fn($r) => $r[0] === $id));
            }
            if (in_array($op,['sync','claim'],true)) {
                $tables['coord_claims'] = $this->records($this->tableRows('coord_claims'));
            } elseif (in_array($op,['renew_claim','release_claim'],true)) {
                $tables['coord_claims'] = $this->records($this->keyRows('coord_claims',[$command['claim_id'] ?? '']));
            }
            if ($op === 'sync') {
                $tables['coord_transfers'] = $this->records($this->query(
                    "SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name='coord_transfers' ORDER BY CAST(JSON_UNQUOTE(JSON_EXTRACT(data_json,'$[0]')) AS BINARY) LIMIT 100",[$this->repo]));
            } elseif (in_array($op,['capture_transfer','get_transfer','check_transfer','review_transfer'],true)) {
                $tables['coord_transfers'] = $this->records($this->keyRows('coord_transfers',[$command['transfer_id'] ?? '']));
            }
            if ($op === 'sync' && $viewer) {
                $scope = json_decode($viewer[0][1],true,512,JSON_THROW_ON_ERROR)['scope'];
                $headers = $this->query("SELECT row_hash,event_seq,event_kind,actor,recipient,event_scope,created_at FROM grove_store_rows WHERE repository_id=? AND table_name='coord_events' AND event_seq>? AND (recipient IS NULL OR recipient=? OR actor=?) ORDER BY event_seq LIMIT 1001",
                    [$this->repo,$command['after'] ?? $viewer[0][2],$id,$id]);
                $rank = 0;
                foreach ($headers as $h) {
                    $relevant = self::overlaps(json_decode($h['event_scope'],true,512,JSON_THROW_ON_ERROR),$scope);
                    if ($relevant && ++$rank <= ($command['limit'] ?? 50)) {
                        $row = $this->query("SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name='coord_events' AND row_hash=?",[$this->repo,$h['row_hash']]);
                        $tables['coord_events'][] = $this->records($row)[0];
                    } else {
                        $tables['coord_events'][] = [(int)$h['event_seq'],$h['event_kind'],$h['actor'],$h['recipient'],$h['event_scope'],'null',(int)$h['created_at']];
                    }
                }
            }
            return [[(int)$g['revision'],(int)$g['event_seq'],(int)$g['context_revision'],
                ['version'=>1,'tables'=>$tables],(int)$g['history_floor']]];
        });
    }

    public static function overlaps(array $a, array $b): bool
    {
        foreach ($a as $x) foreach ($b as $y) {
            if ($x === '.' || $y === '.' || $x === $y || str_starts_with($x,$y.'/') || str_starts_with($y,$x.'/')) return true;
        }
        return false;
    }

    public function check(int $after): array
    {
        return $this->transaction(function () use ($after) {
            $g = $this->state();
            $rows = $this->records($this->query("SELECT data_json FROM grove_store_rows WHERE repository_id=? AND table_name='coord_sessions' AND updated_revision>?",[$this->repo,$after]));
            $clocks = array_map(fn($r) => [$r[0],json_decode($r[1],true,512,JSON_THROW_ON_ERROR)['heartbeat_at'],$r[2],$r[3]],$rows);
            return [[(int)$g['revision'],(int)$g['event_seq'],(int)$g['context_revision'],$clocks,(int)$g['history_floor']]];
        });
    }

    // Private operator backup / migration verification. Bounded pages; each
    // page carries a revision so a backup can reject a concurrent state change.
    public function exportPage(string $table, string $hash, ?int $expected): array
    {
        if (!preg_match('/^[a-fA-F0-9]*$/D',$hash) || ($hash !== '' && strlen($hash)!==64)) {
            throw new GroveStoreError('invalid backup cursor');
        }
        return $this->transaction(function () use ($table,$hash,$expected) {
            $g = $this->state();
            if ($expected !== null && $expected !== (int)$g['revision']) throw new GroveStoreError('backup revision changed; restart backup');
            $rows = $this->query('SELECT table_name,HEX(row_hash) AS digest,row_key,data_json FROM grove_store_rows WHERE repository_id=? AND (table_name>? OR (table_name=? AND row_hash>UNHEX(?))) ORDER BY table_name,row_hash LIMIT 16',
                [$this->repo,$table,$table,$hash]);
            $records = [];
            $size = 0;
            foreach ($rows as $r) {
                $record = ['table_name'=>$r['table_name'],
                    'row_key'=>json_decode($r['row_key'],true,512,JSON_THROW_ON_ERROR),
                    'data'=>json_decode($r['data_json'],true,512,JSON_THROW_ON_ERROR)];
                $size += strlen(self::encode($record));
                if ($records && $size>1048576) break;
                $records[] = $record;
                $table = $r['table_name'];
                $hash = $r['digest'];
            }
            return [[(int)$g['revision'],(int)$g['event_seq'],(int)$g['context_revision'],
                (int)$g['history_floor'],$records,$table,$hash]];
        });
    }

    private function fingerprints(): array
    {
        $result = [];
        foreach (self::TABLES as $table) {
            $rows = $this->query('SELECT SHA2(data_json,256) AS digest FROM grove_store_rows WHERE repository_id=? AND table_name=? ORDER BY row_hash',[$this->repo,$table]);
            $digest = hash_init('sha256');
            foreach ($rows as $row) hash_update($digest,$row['digest']);
            $result[$table] = ['rows'=>count($rows),'sha256'=>hash_final($digest)];
        }
        return $result;
    }

    public function fingerprint(): array
    {
        return $this->transaction(function () {
            $g = $this->state();
            return [[(int)$g['revision'],(int)$g['event_seq'],$this->fingerprints()]];
        });
    }

    public function beginImport(int $revision, int $high, int $context): array
    {
        if ($revision<0 || $high<0 || $context<0) throw new GroveStoreError('invalid import watermark');
        return $this->transaction(function () use ($revision,$high,$context) {
            $rows = $this->query('SELECT * FROM grove_store_state WHERE repository_id=? FOR UPDATE',[$this->repo]);
            if (!$rows) throw new GroveStoreError('initialize the empty destination before import');
            $g = $rows[0];
            if ((int)$g['storage_version']===0 && (int)$g['revision']===$revision+1 && (int)$g['event_seq']===$high && (int)$g['context_revision']===$context+1) return [];
            $count = $this->query("SELECT COUNT(*) AS n FROM grove_store_rows WHERE repository_id=? AND (table_name<>'coord_meta' OR row_hash<>?)",[$this->repo,hash('sha256',self::encode(['repository_id']),true)])[0]['n'];
            if ((int)$g['revision']!==0 || (int)$g['event_seq']!==0 || (int)$count!==0) throw new GroveStoreError('import requires an empty destination');
            $this->query('UPDATE grove_store_state SET storage_version=0,revision=?,event_seq=?,context_revision=? WHERE repository_id=?',[$revision+1,$high,$context+1,$this->repo]);
            return [];
        });
    }

    public function importRows(array $patch): array
    {
        if (count($patch)>256) throw new GroveStoreError('import page exceeds row bound');
        return $this->transaction(function () use ($patch) {
            $g = $this->state(true,0);
            foreach ($patch as $row) {
                if ($row['table_name']==='coord_events' && $row['data'][0]>(int)$g['event_seq']) throw new GroveStoreError('import event exceeds source watermark');
                if ($row['table_name']==='coord_meta' && $row['row_key']===['repository_id'] && $row['data']!==['repository_id',$this->repo]) throw new GroveStoreError('import repository mismatch');
                $this->put($row['table_name'],$row['row_key'],$row['data'],(int)$g['revision']);
            }
            return [];
        });
    }

    public function finishImport(array $expected): array
    {
        return $this->transaction(function () use ($expected) {
            $this->state(true,0);
            $actual = $this->fingerprints();
            foreach (self::TABLES as $table) {
                if (($expected[$table]['rows'] ?? -1)!==$actual[$table]['rows'] ||
                    !hash_equals($expected[$table]['sha256'] ?? '',$actual[$table]['sha256'])) {
                    throw new GroveStoreError('import content verification failed; destination stays paused');
                }
            }
            $this->query('UPDATE grove_store_state SET storage_version=2,maintained_at=? WHERE repository_id=?',[(int)(microtime(true)*1000),$this->repo]);
            return [[$actual]];
        });
    }

    private function put(string $table, array $key, array $data, int $revision): void
    {
        if (!in_array($table,self::TABLES,true) || !array_is_list($data) || !array_is_list($key)) {
            throw new GroveStoreError('invalid row patch');
        }
        $columns = $table === 'coord_decisions' ? 2 : ($table === 'coord_decision_acks' ? 3 : 1);
        if ($key !== array_slice($data,0,$columns)) throw new GroveStoreError('row key mismatch');
        $event = $table === 'coord_events';
        $message = $table === 'coord_messages';
        $encodedKey = self::encode($key);
        $hash = hash('sha256',$encodedKey,true);
        $old = $this->query('SELECT row_key FROM grove_store_rows WHERE repository_id=? AND table_name=? AND row_hash=?',[$this->repo,$table,$hash]);
        if ($old) {
            if ($old[0]['row_key'] !== $encodedKey) throw new GroveStoreError('row key digest mismatch');
            $this->query('UPDATE grove_store_rows SET data_json=?,updated_revision=?,acked_at=?,claim_expires_at=? WHERE repository_id=? AND table_name=? AND row_hash=?',
                [self::encode($data),$revision,$message ? $data[7] : null,$table === 'coord_claims' ? $data[4] : null,$this->repo,$table,$hash]);
            return;
        }
        $this->query('INSERT INTO grove_store_rows(repository_id,table_name,row_hash,row_key,data_json,updated_revision,event_seq,actor,recipient,sender,request_key,acked_at,created_at,event_kind,event_scope,decision_id,claim_expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            [$this->repo,$table,$hash,$encodedKey,self::encode($data),$revision,
             $event ? $data[0] : null,$event ? $data[2] : null,
             $event ? $data[3] : ($message ? $data[2] : null),$message ? $data[1] : null,
             $message ? $data[3] : null,$message ? $data[7] : null,
             $event ? $data[6] : ($message ? $data[5] : null),$event ? $data[1] : null,
             $event ? $data[4] : null,$table === 'coord_decisions' ? $data[0] : null,
             $table === 'coord_claims' ? $data[4] : null]);
    }

    public function commit(int $expected, int $high, bool $context, array $patch): array
    {
        return $this->transaction(function () use ($expected,$high,$context,$patch) {
            $g = $this->state(true);
            if ((int)$g['revision'] !== $expected) return [];
            if ($high < (int)$g['event_seq']) throw new GroveStoreError('event watermark cannot regress');
            $revision = $expected+1;
            foreach ($patch as $row) $this->put($row['table_name'],$row['row_key'],$row['data'],$revision);
            $nextContext = (int)$g['context_revision'] + ($context ? 1 : 0);
            $this->query('UPDATE grove_store_state SET revision=?,event_seq=?,context_revision=? WHERE repository_id=?',
                [$revision,$high,$nextContext,$this->repo]);
            return [[$revision,$nextContext]];
        });
    }

    public function prune(bool $force = false): array
    {
        if (!$force && $this->maintainedAt !== null && (int)(microtime(true)*1000)-$this->maintainedAt < 3600000) return [];
        return $this->transaction(function () use ($force) {
            $g = $this->state(true);
            $now = (int)(microtime(true)*1000);
            if (!$force && $now-(int)$g['maintained_at'] < 3600000) return [];
            $cutoff = $now-$this->days*86400000;
            $removed = 0;
            $floor = (int)$g['history_floor'];
            foreach (['coord_events','coord_messages','coord_claims'] as $table) {
                $condition = $table === 'coord_messages' ? 'created_at<? AND acked_at<?' : ($table === 'coord_events' ? 'created_at<?' : 'claim_expires_at<?');
                $params = [$this->repo,$table,$cutoff];
                if ($table === 'coord_messages') $params[] = $cutoff;
                $rows = $this->query('SELECT row_hash,event_seq FROM grove_store_rows WHERE repository_id=? AND table_name=? AND '.$condition.' LIMIT 256',$params);
                foreach ($rows as $row) {
                    $this->query('DELETE FROM grove_store_rows WHERE repository_id=? AND table_name=? AND row_hash=?',[$this->repo,$table,$row['row_hash']]);
                    $floor = max($floor,(int)($row['event_seq'] ?? 0));
                    $removed++;
                }
            }
            $this->query('UPDATE grove_store_state SET revision=revision+1,context_revision=context_revision+?,history_floor=?,maintained_at=? WHERE repository_id=?',
                [$removed ? 1 : 0,$floor,$now,$this->repo]);
            return [[$removed,$floor]];
        });
    }
}
