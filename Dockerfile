FROM golang:1.26
LABEL maintainer="Allen Institute for Immunology"
WORKDIR /usr/bin
COPY ide-workflow-management .
RUN mkdir ./flows
COPY flows/* flows/
CMD ["/usr/bin/ide-workflow-management"]