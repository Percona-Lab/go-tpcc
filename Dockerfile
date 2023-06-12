FROM bitnami/golang:1.15

WORKDIR /opt/bitnami/go/src/go-tpcc
COPY src .
RUN go build .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh \
    && mkdir -p /var/secrets/certs

ENV CERTIFICATE_FILE_STR ''
ENV DEBUG false
ENTRYPOINT ["/opt/bitnami/go/src/go-tpcc/entrypoint.sh"]
