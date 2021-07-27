#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -h hostname -i ipaddress -d domain"
   echo -e "\t-h Hostname of the server"
   echo -e "\t-i IP Adress of the server"
   echo -e "\t-d Domain name"
   exit 1 # Exit script after printing help
}

while getopts "h:i:d:" opt
do
   case "$opt" in
      h ) SERVERNAME="$OPTARG" ;;
      i ) IPADDRESS="$OPTARG" ;;
	  d ) DOMAIN="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$SERVERNAME" ] || [ -z "$IPADDRESS" ] || [ -z "$DOMAIN" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi

# Begin script in case all parameters are correct

echo ""
echo create Truststore
keytool -import -keystore truststore.jks -trustcacerts -file ./ca/root-ca/01.pem -alias root-ca
keytool -import -keystore truststore.jks -trustcacerts -file ./ca/signing-ca-chain.pem -alias signing-ca

echo ""

echo Generating key
keytool -genkey -dname "CN=${SERVERNAME}.${DOMAIN},OU=NIFI" -alias ${SERVERNAME} -keyalg RSA -keystore keystore.jks -keysize 2048

echo ""
echo Creating signing request
echo ""
keytool -certreq -alias ${SERVERNAME} -ext SAN=dns:${SERVERNAME}.${DOMAIN},dns:${SERVERNAME},ip:${IPADDRESS} -keystore keystore.jks -file ./certs/${SERVERNAME}.csr

echo ""
echo "signing request"
echo ""
openssl ca -config ./etc/signing-ca.conf -in ./certs/${SERVERNAME}.csr -out ./certs/${SERVERNAME}.crt -extensions server_ext

echo ""
echo "import certs in keystore.jks"

keytool -import -keystore keystore.jks -file ca/root-ca/01.pem 
keytool -import -trustcacerts -alias ${DOMAIN} -file certs/${SERVERNAME}.crt -keystore keystore.jks

