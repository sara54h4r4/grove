<?php
declare(strict_types=1);
ini_set('display_errors','0');
header('Content-Type: application/json');
header('Cache-Control: no-store');

function reply(int $status, array $value): never {
    http_response_code($status);
    $raw = json_encode($value,JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    if ($raw === false || strlen($raw) > 16777216) {
        http_response_code(503);
        echo '{"ok":false,"error":"store response exceeds size bound"}';
    } else echo $raw;
    exit;
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') reply(405,['ok'=>false,'error'=>'POST required']);
try {
    // For a custom addon-domain document root, edit ONLY this private path.
    $private = dirname(__DIR__,2).'/.grove';
    $root = realpath($_SERVER['DOCUMENT_ROOT'] ?? '');
    $resolved = realpath($private);
    if ($root === false || $resolved === false || $resolved === $root || str_starts_with($resolved,$root.DIRECTORY_SEPARATOR)) {
        reply(503,['ok'=>false,'error'=>'store private directory must be outside the website root']);
    }
    $config = require $private.'/config.php';
    $token = $config['store_token'] ?? '';
    if (!is_string($token) || strlen($token)<48 || str_contains($token,'SET_')) {
        reply(503,['ok'=>false,'error'=>'store configuration unavailable']);
    }
    if (!hash_equals($token,$_SERVER['HTTP_X_GROVE_STORE_TOKEN'] ?? '')) {
        reply(401,['ok'=>false,'error'=>'authentication required']);
    }
    $raw = file_get_contents('php://input',false,null,0,4194305);
    if ($raw === false || strlen($raw)>4194304) reply(413,['ok'=>false,'error'=>'request exceeds size bound']);
    try { $input = json_decode($raw,true,512,JSON_THROW_ON_ERROR); }
    catch (JsonException $e) { reply(400,['ok'=>false,'error'=>'invalid store request']); }
    if (!is_array($input) || ($input['repository_id'] ?? '') !== $config['repository_id']) {
        reply(403,['ok'=>false,'error'=>'repository identity mismatch']);
    }
    require $private.'/Store.php';
    $store = new GroveStore($config);
    $action = $input['action'] ?? '';
    $rows = match ($action) {
        'initialize' => $store->initialize(),
        'load' => $store->load($input['command']),
        'check' => $store->check($input['revision']),
        'commit' => $store->commit($input['revision'],$input['high'],$input['context'],$input['patch']),
        'prune' => $store->prune(true),
        'export' => $store->exportPage($input['table'] ?? '',$input['hash'] ?? '',$input['revision'] ?? null),
        'fingerprint' => $store->fingerprint(),
        'import_begin' => $store->beginImport($input['revision'],$input['high'],$input['context']),
        'import_rows' => $store->importRows($input['patch']),
        'import_finish' => $store->finishImport($input['fingerprints']),
        default => throw new GroveStoreError('unknown store action'),
    };
    // Once an hour, after returning a consistent operation result. A concurrent
    // caller's revision fence detects any pruning before its commit.
    if (in_array($action,['load','check','commit'],true)) {
        try { $store->prune(); }
        catch (Throwable $ignored) { error_log('Grove maintenance deferred'); }
    }
    reply(200,['ok'=>true,'rows'=>$rows,'rows_returned'=>$store->rowsReturned]);
} catch (GroveStoreError $error) {
    reply(503,['ok'=>false,'error'=>$error->getMessage()]);
} catch (Throwable $error) {
    // No SQL, DSNs, IDs, request/response bodies, tokens, or raw exception text.
    error_log('Grove store unavailable ('.get_class($error).')');
    reply(503,['ok'=>false,'error'=>'store unavailable; check private configuration and database privileges']);
}
