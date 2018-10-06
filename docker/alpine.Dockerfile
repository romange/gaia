FROM  frolvlad/alpine-gxx

RUN apk update && \
    apk upgrade && \
    apk add git autoconf automake libtool make linux-headers

