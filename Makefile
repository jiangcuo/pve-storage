include /usr/share/dpkg/pkg-info.mk

PACKAGE=libpve-storage-perl
BUILDDIR ?= $(PACKAGE)-$(DEB_VERSION)
DSC=$(PACKAGE)_$(DEB_VERSION).dsc

GITVERSION:=$(shell git rev-parse HEAD)

DEB=$(PACKAGE)_$(DEB_VERSION_UPSTREAM_REVISION)_all.deb

all:

.PHONY: dinstall
dinstall: deb
	dpkg -i $(DEB)

$(BUILDDIR):
	rm -rf $@ $@.tmp
	cp -a src $@.tmp
	cp -a debian $@.tmp/
	echo "git clone https://github.com/jiangcuo/pve-storage.git\\ngit checkout $(GITVERSION)" >$@.tmp/debian/SOURCE
	mv $@.tmp $@

.PHONY: deb
deb: $(DEB)
$(DEB): $(BUILDDIR)
	cd $(BUILDDIR); dpkg-buildpackage -b -us -uc
	lintian $(DEB)

.PHONY: clean distclean
distclean: clean
clean:
	rm -rf $(PACKAGE)-[0-9]*/ *.deb *.dsc *.build *.buildinfo *.changes $(PACKAGE)*.tar.*

.PHONY: upload
upload: UPLOAD_DIST ?= $(DEB_DISTRIBUTION)
upload: $(DEB)
	tar cf - $(DEB) | ssh -X repoman@repo.proxmox.com -- upload --product pve --dist $(UPLOAD_DIST)

dsc: $(DSC)
	$(MAKE) clean
	$(MAKE) $(DSC)
	lintian $(DSC)

$(DSC): $(BUILDDIR)
	cd $(BUILDDIR); dpkg-buildpackage -S -us -uc -d

sbuild: $(DSC)
	sbuild $(DSC)
