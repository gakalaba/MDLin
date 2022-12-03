CURR_DIR = $(shell pwd)
BIN_DIR = bin
GO_BUILD = GO111MODULE=off GOPATH=$(CURR_DIR) GOBIN=$(CURR_DIR)/$(BIN_DIR) go install $@

all: server master clientnew coordinator # lintest seqtest

server:
	$(GO_BUILD)

client:
	$(GO_BUILD)

master:
	$(GO_BUILD)

clientnew:
	$(GO_BUILD)

coordinator:
	$(GO_BUILD)

lintest:
	$(GO_BUILD)

seqtest:
	$(GO_BUILD)

.PHONY: clean

clean:
	rm -rf bin pkg
