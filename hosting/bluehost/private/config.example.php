<?php
// Put the completed config in ~/.grove/config.php, OUTSIDE public_html.
// Use a NEW database and database user, separate from WordPress.
return [
    'dsn' => 'mysql:host=localhost;dbname=CPANELUSER_grove;charset=utf8mb4',
    'username' => 'CPANELUSER_grove',
    'password' => 'SET_DATABASE_PASSWORD_HERE',
    'repository_id' => 'SET_EXISTING_GROVE_REPOSITORY_UUID_HERE',
    // Generate 32 random bytes (64 hex characters). This is a NEW backend-only
    // credential; keep the agents' existing GROVE_COORD_TOKEN unchanged.
    'store_token' => 'SET_RANDOM_BACKEND_TOKEN_HERE',
    'retention_days' => 7,
];
