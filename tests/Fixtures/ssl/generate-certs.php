<?php

declare(strict_types=1);

$dir = __DIR__;
echo "Generating SSL Certificates for Testing in $dir...\n\n";

// ── CA config ─────────────────────────────────────────────────────────────────
// basicConstraints=CA:TRUE is mandatory for verify-ca / verify-full.
// Without it, modern OpenSSL and libpq reject the cert as a non-CA issuer.
$caConfFile = "$dir/temp_ca.cnf";
file_put_contents($caConfFile, <<<CONF
[req]
distinguished_name = req_distinguished_name
x509_extensions    = v3_ca

[req_distinguished_name]

[v3_ca]
basicConstraints = critical, CA:TRUE
keyUsage         = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
CONF);

$caOptions = [
    'private_key_bits' => 2048,
    'private_key_type' => OPENSSL_KEYTYPE_RSA,
    'digest_alg' => 'sha256',
    'config' => $caConfFile,
    'x509_extensions' => 'v3_ca',
];

echo "Generating CA...\n";
$caKey = openssl_pkey_new($caOptions);
$caCsr = openssl_csr_new(['commonName' => 'Test CA'], $caKey, $caOptions);
if ($caCsr === false) {
    die('Error generating CA CSR: ' . openssl_error_string() . "\n");
}
$caCert = openssl_csr_sign($caCsr, null, $caKey, 3650, $caOptions);
openssl_x509_export_to_file($caCert, "$dir/ca.pem");
openssl_pkey_export_to_file($caKey, "$dir/ca-key.pem", null, $caOptions);

// ── Server cert config (SAN required for verify-full hostname matching) ────────
$serverConfFile = "$dir/temp_server.cnf";
file_put_contents($serverConfFile, <<<CONF
[req]
distinguished_name = req_distinguished_name
req_extensions     = v3_server

[req_distinguished_name]

[v3_server]
basicConstraints     = CA:FALSE
subjectAltName       = IP:127.0.0.1
keyUsage             = critical, digitalSignature, keyEncipherment
extendedKeyUsage     = serverAuth
CONF);

$serverOptions = [
    'private_key_bits' => 2048,
    'private_key_type' => OPENSSL_KEYTYPE_RSA,
    'digest_alg' => 'sha256',
    'config' => $serverConfFile,
    'x509_extensions' => 'v3_server',
];

echo "Generating Server Certificate (SAN: IP:127.0.0.1)...\n";
$serverKey = openssl_pkey_new($serverOptions);
$serverCsr = openssl_csr_new(['commonName' => '127.0.0.1'], $serverKey, $serverOptions);
if ($serverCsr === false) {
    die('Error generating Server CSR: ' . openssl_error_string() . "\n");
}
$serverCert = openssl_csr_sign($serverCsr, $caCert, $caKey, 3650, $serverOptions);
openssl_x509_export_to_file($serverCert, "$dir/server-cert.pem");
openssl_pkey_export_to_file($serverKey, "$dir/server-key.pem", null, $serverOptions);

// ── Client cert config (mTLS) ─────────────────────────────────────────────────
$clientConfFile = "$dir/temp_client.cnf";
file_put_contents($clientConfFile, <<<CONF
[req]
distinguished_name = req_distinguished_name
req_extensions     = v3_client

[req_distinguished_name]

[v3_client]
basicConstraints = CA:FALSE
keyUsage         = critical, digitalSignature
extendedKeyUsage = clientAuth
CONF);

$clientOptions = [
    'private_key_bits' => 2048,
    'private_key_type' => OPENSSL_KEYTYPE_RSA,
    'digest_alg' => 'sha256',
    'config' => $clientConfFile,
    'x509_extensions' => 'v3_client',
];

echo "Generating Client Certificate (CN=test_user)...\n";
$clientKey = openssl_pkey_new($clientOptions);
$clientCsr = openssl_csr_new(['commonName' => 'test_user'], $clientKey, $clientOptions);
if ($clientCsr === false) {
    die('Error generating Client CSR: ' . openssl_error_string() . "\n");
}
$clientCert = openssl_csr_sign($clientCsr, $caCert, $caKey, 3650, $clientOptions);
openssl_x509_export_to_file($clientCert, "$dir/client-cert.pem");
openssl_pkey_export_to_file($clientKey, "$dir/client-key.pem", null, $clientOptions);

foreach ([$caConfFile, $serverConfFile, $clientConfFile] as $f) {
    if (file_exists($f)) {
        unlink($f);
    }
}

@chmod("$dir/server-key.pem", 0600);
@chmod("$dir/client-key.pem", 0600);

echo "\nDone! Certificates generated successfully.\n";
echo "Note: If running tests from WSL on /mnt/c/, the mTLS test copies the\n";
echo "client key to /tmp automatically to satisfy libpq's 0600 requirement.\n";
