# nlogn

flask
pyopenssl
schedule

the relay needs a ssl/tls key + cert
openssl req -x509 -nodes -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes