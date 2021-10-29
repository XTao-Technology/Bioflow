%global xstone_version %{xstone_package_version}


Summary: XTao xstone
Name: xstone
Version: %{xstone_version} 
Release: 0
License: GPL
Buildroot: %{_tmppath}/%{name}-buildroot
Group: Applications/File
%define tarball %{name}-%{version}.tar.gz
Source0: %{tarball}


%description
This is distributed auto mount created by XTao to do xstone.

%package -n %{name}-server
Summary: xstone server
Group: System Environment/Kernel
Provides: %{name}-server = %{version}-%{release}

%description -n %{name}-server
This is XTao mount manager.

%prep
%setup -q


%build
%configure
make

%postun
%define __debug_install_post   \
         %{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} "%{_builddir}/%{?buildsubdir}"\
         %{nil}

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/xstone

make DESTDIR=%{buildroot} MANDIR=%{_mandir} BINDIR=%{_sbindir} SYSTEMD_DIR=%{_unitdir} install
mkdir -p %{buildroot}/var/run/mnt/xtao
cd ../../../..
mkdir -p %{buildroot}/usr/lib/systemd/system/
install -p src/automount-manager/init/automount-manager-plugin.service %{buildroot}/usr/lib/systemd/system/automount-manager-plugin.service
install -p src/automount-manager/init/volumeConfig.json %{buildroot}/opt/xstone/volumeConfig.json
install -p src/automount-manager/init/automount.sh %{buildroot}/opt/xstone/automount.sh
cd -

%clean
rm -rf %{buildroot}


%files -n %{name}-server
/opt/xstone/automount-manager-plugin
/usr/lib/systemd/system/automount-manager-plugin.service
/opt/xstone/volumeConfig.json
/opt/xstone/automount.sh

%changelog
* Mon Jul 10 2017 Yawei Liu <yawei.liu@xtaotech.com> - add auto mount manager
- add the xstone manager

* Thu Feb 16 2017 Jason Zhang <jason.zhang@xtaotech.com> - initial version
- create the initial version
