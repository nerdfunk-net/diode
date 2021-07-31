#!/bin/bash

# install the homebrew version of openssl!!!

helpFunction()
{
   echo ""
   echo "Usage: $0 -c clientname -i ipaddress -d domain"
   echo -e "\t-c Clientname"
   echo -e "\t-i IP Adress of the Client"
   echo -e "\t-d Domain name"
   exit 1 # Exit script after printing help
}

while getopts "c:i:d:" opt
do
   case "$opt" in
      c ) CLIENTNAME="$OPTARG" ;;
      i ) IPADDRESS="$OPTARG" ;;
      d ) DOMAIN="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$CLIENTNAME" ] || [ -z "$IPADDRESS" ] || [ -z "$DOMAIN" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi

# Begin script in case all parameters are correct

/usr/local/opt/openssl/bin/openssl req -new \
    -config etc/server.conf \
    -out certs/client.csr \
    -keyout certs/client.key \
    -addext "subjectAltName=IP:${IPADDRESS},DNS:${CLIENTNAME},DNS:${DOMAIN}"

# checking csr
/usr/local/opt/openssl/bin/openssl req -text -noout -verify -in certs/client.csr

/usr/local/opt/openssl/bin/openssl ca \
    -config etc/signing-ca.conf \
    -in certs/client.csr \
    -out certs/client.crt \
    -extensions server_ext
    
openssl pkcs12 -export \
    -name ${CLIENTNAME} \
    -inkey certs/client.key \
    -in certs/client.crt \
    -out certs/client.p12
        
echo ""
echo "checking the certificate"
echo ""
/usr/local/opt/openssl/bin/openssl x509 -in certs/client.crt -text -noout
