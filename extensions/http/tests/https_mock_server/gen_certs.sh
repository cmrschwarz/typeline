#!/bin/sh
set -xe

# adapted from by https://github.com/rustls/hyper-rustls/blob/main/examples/refresh-certificates.sh
# openssl.cnf also from the same source

# We choose to use the hyper-tls source under their MIT License:

# Copyright (c) 2016 Joseph Birr-Pixton <jpixton@gmail.com>
#
# Permission is hereby granted, free of charge, to any
# person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the
# Software without restriction, including without
# limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software
# is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice
# shall be included in all copies or substantial portions
# of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
# ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
# TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
# IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

cert_validity_days=9000
ca_name="WeSignThingsLTD"
hostname=localhost
rsa_bits=2048
key_name="example"

openssl req -nodes \
          -x509 \
          -days "$cert_validity_days" \
          -newkey "rsa:$rsa_bits" \
          -keyout ca.key \
          -out ca.cert \
          -sha256 \
          -batch \
          -subj "/CN=$ca_name RSA CA"

openssl req -nodes \
          -newkey "rsa:$rsa_bits" \
          -keyout inter.key \
          -out inter.req \
          -sha256 \
          -batch \
          -subj "/CN=$ca_name RSA level 2 intermediate"

openssl req -nodes \
          -newkey "rsa:$rsa_bits" \
          -keyout end.key \
          -out end.req \
          -sha256 \
          -batch \
          -subj "/CN=$hostname"

openssl rsa \
          -in end.key \
          -out "$key_name.rsa"

openssl x509 -req \
            -in inter.req \
            -out inter.cert \
            -CA ca.cert \
            -CAkey ca.key \
            -sha256 \
            -days "$cert_validity_days" \
            -set_serial 1 \
            -extensions v3_inter -extfile openssl.cnf

openssl x509 -req \
            -in end.req \
            -out end.cert \
            -CA inter.cert \
            -CAkey inter.key \
            -sha256 \
            -days "$cert_validity_days" \
            -set_serial 2 \
            -extensions v3_end -extfile openssl.cnf

cat end.cert inter.cert ca.cert > "$key_name.pem"
rm *.key *.cert *.req
