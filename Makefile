CURR_DIR = $(shell pwd)
BIN_DIR = bin
GO_BUILD = GO111MODULE=off GOPATH=$(CURR_DIR) GOBIN=$(CURR_DIR)/$(BIN_DIR) go install $@

all: server master coordinator clientnew python_wrapper monitoring_app_client # retwisclient lintest seqtest monitoring_app_client 

server:
	$(GO_BUILD)

client:
	$(GO_BUILD)

master:
	$(GO_BUILD)

# retwisclient:
# 	$(GO_BUILD)

monitoring_app_client:
	$(GO_BUILD)

clientnew:
	$(GO_BUILD)

coordinator:
	$(GO_BUILD)

# lintest:
# 	$(GO_BUILD)

# seqtest:
# 	$(GO_BUILD)

python_wrapper:
	GO111MODULE=off GOPATH=$(CURR_DIR) go build -buildmode=c-shared -o src/pythonwrapper/libmdlclient.so src/pythonwrapper/python_mdl_wrapper.go

.PHONY: clean python_wrapper

clean:
	rm -rf bin pkg
	rm -f src/pythonwrapper/libmdlclient.so*
