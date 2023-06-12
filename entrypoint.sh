#!/usr/bin/env bash
DEBUG=${DEBUG-false}

if ${DEBUG}; then
  set -ex
fi

CERTIFICATE_FILE_STR=${CERTIFICATE_FILE_STR-''}
CERTIFICATE_PATH=${CERTIFICATE_PATH-/var/secrets/certs/certificate.crt}
CERTIFICATE_PATH_DIR=$(dirname "${CERTIFICATE_PATH}")
mkdir -p "${CERTIFICATE_PATH_DIR}"


echo "Exporting certificate file ${CERTIFICATE_PATH}"
echo "${CERTIFICATE_FILE_STR}" > ${CERTIFICATE_PATH}

if ${DEBUG}; then
  cat ${CERTIFICATE_PATH}
fi

/opt/bitnami/go/src/go-tpcc/go-tpcc "$@"
