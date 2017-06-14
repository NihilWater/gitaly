PREFIX=/usr/local
PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=$(shell pwd)
BIN_BUILD_DIR=${BUILD_DIR}/_build/bin
PKG_BUILD_DIR:=${BUILD_DIR}/_build/src/${PKG}
CMDS:=$(shell cd cmd && ls)
TEST_REPO=internal/testhelper/testdata/data/gitlab-test.git
VERSION=$(shell git describe)-$(shell date -u +%Y%m%d.%H%M%S)

export GOPATH=${BUILD_DIR}/_build
export GO15VENDOREXPERIMENT=1
export PATH:=${GOPATH}/bin:$(PATH)

.PHONY: all
all: build

build:	$(shell find . -name '*.go' -not -path './vendor/*' -not -path './_build/*')
	./run prepare-build
	rm -f -- "${BIN_BUILD_DIR}/*"
	go install -ldflags "-X main.version=${VERSION}" ${PKG}/cmd/...
	cp ${BIN_BUILD_DIR}/* ${BUILD_DIR}/

install: build
	mkdir -p $(DESTDIR)${PREFIX}/bin/
	cd ${BIN_BUILD_DIR} && install ${CMDS} ${DESTDIR}${PREFIX}/bin/

verify: govendor-status
	./run lint
	./run check-formatting

govendor-status:
	./run prepare-build
	./run install-developer-tools
	cd ${PKG_BUILD_DIR} && govendor status

${TEST_REPO}:
	git clone --bare https://gitlab.com/gitlab-org/gitlab-test.git $@

test: ${TEST_REPO}
	./run prepare-build
	go test ${PKG}/...

package: build
	./_support/package/package ${CMDS}

notice:
	./run prepare-build
	./run install-developer-tools
	rm -f ${PKG_BUILD_DIR}/NOTICE # Avoid NOTICE-in-NOTICE
	cd ${PKG_BUILD_DIR} && govendor license -template _support/notice.template -o ${BUILD_DIR}/NOTICE

clean:	clean-build
	rm -rf internal/testhelper/testdata
	rm -f $(foreach cmd,${CMDS},./${cmd})

clean-build:
	rm -rf ${BUILD_DIR}/_build
