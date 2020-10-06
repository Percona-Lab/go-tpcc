FROM ubuntu:18.04
COPY go-tpcc /go-tpcc
RUN chmod +rx /go-tpcc
ENTRYPOINT ["/go-tpcc"]