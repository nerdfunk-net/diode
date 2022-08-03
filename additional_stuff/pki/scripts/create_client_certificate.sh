#!/bin/bash

# If you are using a MAC:
# install the homebrew version of openssl!!!
# the homebrew version supports addext to add a subjectAltName

#OPENSSL=/usr/local/opt/openssl/bin/openssl
OPENSSL=/usr/bin/openssl

# read subject from etc/subj
source etc/subj

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

# get DC Part
IFS="."
read -ra dc <<< "$DOMAIN"
NNDC=${#dc[@]}

case ${NNDC} in
   3) DC="DC=${dc[0]}/DC=${dc[1]}/DC=${dc[2]}"
   ;;
   2) DC="DC=${dc[0]}/DC=${dc[1]}"
   ;;
esac

if [ "${DC}" = "" ]
then
  echo "please specify domain as fqdn"
  helpFunction
fi

echo ""
echo "Creating certificate for ${CLIENTNAME}.${DOMAIN}"
echo ""
echo "Create key and signing request"
echo ""

mkdir certs/${CLIENTNAME}

echo ""
echo "Creating signing request (CSR)"
echo ""
${OPENSSL} req -new \
    -config etc/server.conf \
    -out certs/${CLIENTNAME}/${CLIENTNAME}.csr \
    -keyout certs/${CLIENTNAME}/${CLIENTNAME}.key \
    -addext "subjectAltName=IP:${IPADDRESS},DNS:${CLIENTNAME},DNS:${DOMAIN}" \
	-subj "/${DC}/O=${O}/OU=${OU}/CN=${CLIENTNAME}.${DOMAIN}" \
    -passout file:etc/password \

# checking csr
echo ""
echo "Checking signing request (CSR)"
echo ""
${OPENSSL} req -text \
    -noout \
    -verify \
    -in certs/${CLIENTNAME}/${CLIENTNAME}.csr \
    -passout file:etc/password

echo ""
echo "Signing signing request (CSR)"
echo ""
${OPENSSL} ca \
    -config etc/signing-ca.conf \
    -in certs/${CLIENTNAME}/${CLIENTNAME}.csr \
    -out certs/${CLIENTNAME}/${CLIENTNAME}.crt \
    -extensions server_ext \
    -passin file:etc/password \
    -batch

echo ""
echo "Exporting to PKCS12"
echo ""
${OPENSSL} pkcs12 -export \
    -name ${CLIENTNAME} \
    -inkey certs/${CLIENTNAME}/${CLIENTNAME}.key \
    -in certs/${CLIENTNAME}/${CLIENTNAME}.crt \
    -out certs/${CLIENTNAME}/${CLIENTNAME}.p12 \
    -passout file:etc/password \
        
echo ""
echo "checking the certificate"
echo ""
${OPENSSL} x509 -in certs/${CLIENTNAME}/${CLIENTNAME}.crt -text -noout
