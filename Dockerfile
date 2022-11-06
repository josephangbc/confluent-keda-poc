# FROM golang:1.19.2-alpine3.16
FROM golang:1.19.2
RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN go mod download
RUN go build -o main .
CMD ["/app/main"]