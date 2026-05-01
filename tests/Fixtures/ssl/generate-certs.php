<?php

declare(strict_types=1);

$dir = __DIR__;
echo "Generating SSL Certificates for Testing in $dir...\n\n";

// Create a base OpenSSL config to fix Windows missing openssl.cnf issues
$baseConfFile = "$dir/temp_base.cnf";
file_put_contents($baseConfFile, "[req]\ndistinguished_name = req_distinguished_name\n[req_distinguished_name]\n");

$options = [
    'private_key_bits' => 2048,
    'private_key_type' => OPENSSL_KEYTYPE_RSA,
    'digest_alg' => 'sha256',
    'config' => $baseConfFile,
];

// Generate CA (Certificate Authority)
echo "Generating CA...\n";
$caKey = openssl_pkey_new($options);
$caCsr = openssl_csr_new(['commonName' => 'Test CA'], $caKey, $options);
if ($caCsr === false) {
    die('Error generating CA CSR: ' . openssl_error_string() . "\n");
}
$caCert = openssl_csr_sign($caCsr, null, $caKey, 3650, $options);

openssl_x509_export_to_file($caCert, "$dir/ca.pem");
openssl_pkey_export_to_file($caKey, "$dir/ca-key.pem", null, $options);

// Temporary OpenSSL Config for Server SAN (Subject Alternative Name)
$sanConfFile = "$dir/temp_san.cnf";
$sanConf = <<<CONF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
basicConstraints = CA:FALSE
subjectAltName = IP:127.0.0.1
CONF;
file_put_contents($sanConfFile, $sanConf);

$serverOptions = $options;
$serverOptions['config'] = $sanConfFile; // Use SAN config for server

// Generate Server Certificate
echo "Generating Server Certificate (with SAN for 127.0.0.1)...\n";
$serverKey = openssl_pkey_new($serverOptions);
$serverCsr = openssl_csr_new(['commonName' => '127.0.0.1'], $serverKey, $serverOptions);
if ($serverCsr === false) {
    die('Error generating Server CSR: ' . openssl_error_string() . "\n");
}

$serverSignOptions = $serverOptions;
$serverSignOptions['x509_extensions'] = 'v3_req';

$serverCert = openssl_csr_sign($serverCsr, $caCert, $caKey, 3650, $serverSignOptions);
openssl_x509_export_to_file($serverCert, "$dir/server-cert.pem");
openssl_pkey_export_to_file($serverKey, "$dir/server-key.pem", null, $serverOptions);

// Generate Client Certificate (for mTLS)
echo "Generating Client Certificate (CN=test_user)...\n";
$clientKey = openssl_pkey_new($options);
$clientCsr = openssl_csr_new(['commonName' => 'test_user'], $clientKey, $options);
if ($clientCsr === false) {
    die('Error generating Client CSR: ' . openssl_error_string() . "\n");
}
$clientCert = openssl_csr_sign($clientCsr, $caCert, $caKey, 3650, $options);

openssl_x509_export_to_file($clientCert, "$dir/client-cert.pem");
openssl_pkey_export_to_file($clientKey, "$dir/client-key.pem", null, $options);

if (file_exists($baseConfFile)) {
    unlink($baseConfFile);
}
if (file_exists($sanConfFile)) {
    unlink($sanConfFile);
}
@chmod("$dir/server-key.pem", 0600);

echo "\nDone! Certificates generated successfully.\n";
