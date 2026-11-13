#!/bin/sh
# Regenerates the loopback TLS material used by the QUIC sync tests.
#
# SwiftNIO QUIC signs with P-256 keys and reads PEM files, so this makes:
#   loopback-root.pem   a P-256 certificate authority the link trusts
#   loopback-chain.pem  the host's leaf certificate followed by the root
#   loopback-key.pem    the leaf's private key (PKCS#8)
# The leaf names localhost, 127.0.0.1 and ::1 and lasts 397 days, under the
# 398-day ceiling clients apply to server certificates; renew it before then.
set -e
cd "$(dirname "$0")"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$work/root-key.pem" -out loopback-root.pem -days 3650 \
  -subj "/CN=KwiftDB Loopback Root" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$work/leaf-key.pem" -out "$work/leaf.csr" -subj "/CN=localhost"
cat > "$work/leaf.ext" <<EXT
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1
EXT
openssl x509 -req -in "$work/leaf.csr" -CA loopback-root.pem -CAkey "$work/root-key.pem" \
  -set_serial "0x$(openssl rand -hex 8)" -out "$work/leaf.pem" -days 397 -extfile "$work/leaf.ext"

cat "$work/leaf.pem" loopback-root.pem > loopback-chain.pem
openssl pkcs8 -topk8 -nocrypt -in "$work/leaf-key.pem" -out loopback-key.pem
