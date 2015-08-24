# Pull base image.
FROM google/golang

# Install HG for go get
#RUN apt-get update && \
#    apt-get install -y mercurial curl git

ADD . /gopath/src/github.com/wayt/scheduler
WORKDIR /gopath/src/github.com/wayt/scheduler

RUN go get
RUN go install

CMD ["/gopath/bin/scheduler"]
