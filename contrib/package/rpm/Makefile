
GIT_COMMIT ?= HEAD
GIT_VERSION := $(shell git describe $(GIT_COMMIT) 2>/dev/null)
GIT_VERSION := $(subst -, ,$(GIT_VERSION))

VERSION ?= $(word 1,$(GIT_VERSION))
ifeq (,$(VERSION))
VERSION := 1.0.2
endif
RELEASE ?= $(word 2,$(GIT_VERSION)).$(word 3,$(GIT_VERSION))
ifeq (.,$(RELEASE))
RELEASE := 0
endif

SOURCE_TARGET := rpmbuild/SOURCES/qfs-$(VERSION).tar.gz
SOURCE ?= $(SOURCE_TARGET)

.PHONY: all version clean mkdir prepare_source srpm rpm

all: rpm

version:
	@if [ "$(origin VERSION)" = "file" -a -z "$(GIT_VERSION)" ] ; then \
		echo >&2 WARNING: Unable to determine qfs version via git describe; \
		sleep 1; \
	fi
	@echo QFS Version $(VERSION)-$(RELEASE); \

clean:
	rm -rf rpmbuild

mkdir: clean
	mkdir -p rpmbuild
	mkdir -p rpmbuild/BUILD
	mkdir -p rpmbuild/BUILDROOT
	mkdir -p rpmbuild/RPMS
	mkdir -p rpmbuild/SOURCES
	mkdir -p rpmbuild/SRPMS

$(SOURCE_TARGET): mkdir
	if [ -f "$(SOURCE)" ]; then \
		cp $(SOURCE) $(SOURCE_TARGET); \
	else \
		(cd `git rev-parse --show-toplevel` && git archive --format=tar \
			--prefix=qfs-$(VERSION)/ $(GIT_COMMIT)) | gzip > $@; \
	fi

prepare_source: $(SOURCE_TARGET)
	cp *.patch rpmbuild/SOURCES

srpm: version prepare_source
	rpmbuild $(RPM_OPTS) \
	  --define "_topdir %(pwd)" \
	  --define "_builddir %{_topdir}/rpmbuild/BUILD" \
	  --define "_buildrootdir %{_topdir}/rpmbuild/BUILDROOT" \
	  --define "_rpmdir %{_topdir}/rpmbuild/RPMS" \
	  --define "_srcrpmdir %{_topdir}/rpmbuild/SRPMS" \
	  --define "_specdir %{_topdir}" \
	  --define "_sourcedir  %{_topdir}/rpmbuild/SOURCES" \
	  --define "QFS_VERSION $(VERSION)" \
	  --define "QFS_RELEASE $(RELEASE)" \
	  --nodeps -bs qfs.spec

rpm: version prepare_source
	rpmbuild $(RPM_OPTS) \
	  --define "_topdir %(pwd)" \
	  --define "_builddir %{_topdir}/rpmbuild/BUILD" \
          --define "_buildrootdir %{_topdir}/rpmbuild/BUILDROOT" \
	  --define "_rpmdir %{_topdir}/rpmbuild/RPMS" \
	  --define "_srcrpmdir %{_topdir}/rpmbuild/SRPMS" \
	  --define "_specdir %{_topdir}" \
	  --define "_sourcedir  %{_topdir}/rpmbuild/SOURCES" \
	  --define "QFS_VERSION $(VERSION)" \
	  --define "QFS_RELEASE $(RELEASE)" \
	  -ba qfs.spec

