GIT_COMMIT ?= HEAD
GIT_VERSION = $(subst -, ,$(shell git describe $(GIT_COMMIT)))
VERSION ?= $(word 1,$(GIT_VERSION))
RELEASE ?= $(word 2,$(GIT_VERSION))
ifeq ($(VERSION),)
$(error Unable to determine git version)
endif
$(info QFS Version $(VERSION)-$(RELEASE))

all: rpm

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

srpm: prepare_source
	rpmbuild --define "_topdir %(pwd)" \
	  --define "_builddir %{_topdir}/rpmbuild/BUILD" \
	  --define "_buildrootdir %{_topdir}/rpmbuild/BUILDROOT" \
	  --define "_rpmdir %{_topdir}/rpmbuild/RPMS" \
	  --define "_srcrpmdir %{_topdir}/rpmbuild/SRPMS" \
	  --define "_specdir %{_topdir}" \
	  --define "_sourcedir  %{_topdir}/rpmbuild/SOURCES" \
	  --define "QFS_VERSION $(VERSION)" \
	  --define "QFS_RELEASE $(RELEASE)" \
	  --nodeps -bs qfs.spec

rpm: prepare_source
	rpmbuild --define "_topdir %(pwd)" \
	  --define "_builddir %{_topdir}/rpmbuild/BUILD" \
          --define "_buildrootdir %{_topdir}/rpmbuild/BUILDROOT" \
	  --define "_rpmdir %{_topdir}/rpmbuild/RPMS" \
	  --define "_srcrpmdir %{_topdir}/rpmbuild/SRPMS" \
	  --define "_specdir %{_topdir}" \
	  --define "_sourcedir  %{_topdir}/rpmbuild/SOURCES" \
	  --define "QFS_VERSION $(VERSION)" \
	  --define "QFS_RELEASE $(RELEASE)" \
	  -ba qfs.spec

