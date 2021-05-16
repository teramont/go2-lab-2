FROM golang:1.16 as build

RUN apt-get update && apt-get install -y ninja-build

WORKDIR /go/src
RUN git clone https://github.com/teramont/go2-lab-1
WORKDIR /go/src/go2-lab-1
RUN go get -u ./build/cmd/bood

WORKDIR /go/src/go2-lab-2
COPY . .

RUN CGO_ENABLED=0 bood

# ==== Final image ====
FROM alpine:3.11
WORKDIR /opt/go2-lab-2
COPY entry.sh ./
COPY --from=build /go/src/go2-lab-2/out/bin/* ./
ENTRYPOINT ["/opt/go2-lab-2/entry.sh"]
CMD ["server"]