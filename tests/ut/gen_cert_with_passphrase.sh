#!/bin/bash
mkdir -p certs
cat > certs/etcd_ssl.conf << EOF
[req]
default_bits   = 4096
req_extensions = v3_req
distinguished_name = req_distinguished_name
subjectAltName = @alt_names

[req_distinguished_name]

[ v3_ca ]
basicConstraints       = CA:true
keyUsage               = keyCertSign,cRLSign
subjectKeyIdentifier   = hash

[ v3_req ]
basicConstraints       = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = etcd
IP.1 = 127.0.0.1
EOF

unset LD_LIBRARY_PATH
# Create the CA Certificate and Key
openssl req -keyout certs/ca.key -out certs/ca.crt -passin pass:huawei -passout pass:huawei \
  -subj "/C=CN/ST=GuangDong/L=ShenZhen/O=huawei/OU=Test/CN=etcd CA" -config certs/etcd_ssl.conf -new -x509 -extensions v3_ca

# Generate valid Server Key/Cert
openssl genrsa -passout pass:huawei -out certs/etcd-server.key 4096
openssl req -passin pass:huawei -new -key certs/etcd-server.key -out server.csr -subj  "/C=CN/ST=GuangDong/L=ShenZhen/O=huawei/OU=Server/CN=etcd"  -config certs/etcd_ssl.conf
openssl x509 -req -passin pass:huawei -days 365 -in server.csr -CA certs/ca.crt -CAkey certs/ca.key -set_serial 01 -out certs/etcd-server.crt  -extensions v3_req  -extfile certs/etcd_ssl.conf

# Remove passphrase from the Server Key
openssl rsa -passin pass:huawei -in certs/etcd-server.key -out certs/etcd-server.key

# Generate valid Client Key/Cert
openssl genrsa -aes128 -passout pass:huawei -out certs/etcd-client.key 4096
openssl req -passin pass:huawei -new -key certs/etcd-client.key -out client.csr -subj  "/C=CN/ST=GuangDong/L=ShenZhen/O=huawei/OU=Client/CN=etcd"
openssl x509 -passin pass:huawei -req -days 365 -in client.csr -CA certs/ca.crt -CAkey certs/ca.key -set_serial 01 -out certs/etcd-client.crt  -extensions v3_req  -extfile certs/etcd_ssl.conf

cat > certs/passphrase << EOF
huawei
EOF

rm -f ./server.csr
rm -f ./client.csr
