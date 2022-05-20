## Install mockserver CA certificate   

An example bash script:
https://raw.githubusercontent.com/mock-server/mockserver/master/scripts/install_ca_certificate.sh

````
wget https://raw.githubusercontent.com/mock-server/mockserver/master/mockserver-core/src/main/resources/org/mockserver/socket/CertificateAuthorityCertificate.pem
keytool -import -v -keystore cacerts -alias mockserver-ca -file CertificateAuthorityCertificate.pem -storepass changeit -trustcacerts -noprompt
