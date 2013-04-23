
GIT_COMMIT ?= HEAD
GIT_VERSION := $(shell git describe $(GIT_COMMIT))
GIT_VERSION := $(subst -, ,$(GIT_VERSION))

VERSION ?= $(word 1,$(GIT_VERSION))
RELEASE ?= $(word 2,$(GIT_VERSION)).$(word 3,$(GIT_VERSION))
ifeq (.,$(RELEASE))
RELEASE := 0
endif

.PHONY: all version clean mkdir prepare_source srpm rpm

all: rpm

version:
	@if [ -n "$(VERSION)" ] ; then \
		echo QFS Version $(VERSION)-$(RELEASE); \
	else \
		echo >&2 Unable to determine qfs version; \
		exit 1; \
	fi

clean:
	rm -rf rpmbuild

mkdir: clean
	mkdir -p rpmbuild
	mkdir -p rpmbuild/BUILD
	mkdir -p rpmbuild/BUILDROOT
	mkdir -p rpmbuild/RPMS
	mkdir -p rpmbuild/SOURCES
	mkdir -p rpmbuild/SRPMS

prepare_source: mkdir
	cp *.patch rpmbuild/SOURCES
	(cd `git rev-parse --show-toplevel` && git archive --format=tar --prefix=qfs-$(VERSION)/ $(GIT_COMMIT)) | \
	   gzip > rpmbuild/SOURCES/qfs-$(VERSION).tar.gz

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

