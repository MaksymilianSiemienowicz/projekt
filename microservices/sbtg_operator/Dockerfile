FROM golang:1.24 AS builder

WORKDIR /workspace
RUN go mod init sbtg-operator
RUN go get k8s.io/client-go \
    k8s.io/api \
    k8s.io/apimachinery \
    sigs.k8s.io/controller-runtime \
    github.com/google/gnostic \
    github.com/google/gnostic-models
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o manager main.go
FROM gcr.io/distroless/base-debian11
WORKDIR /
COPY --from=builder /workspace/manager .
ENTRYPOINT ["/manager"]

