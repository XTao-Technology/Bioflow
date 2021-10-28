%global bioflow_version %{bioflow_package_version}


Summary: XTao bioflow job scheduler
Name: bioflow
Version: %{bioflow_version} 
Release: 0
License: GPL
Buildroot: %{_tmppath}/%{name}-buildroot
Group: Applications/File
%define tarball %{name}-%{version}.tar.gz
Source0: %{tarball}


%description
This is distributed scheduler created by XTao to do large-scale
bioinformatics analysis


%package -n %{name}-server
Summary: bioflow scheduler server
Group: System Environment/Kernel
Provides: %{name}-server = %{version}-%{release}

%package -n %{name}-cli
Summary: bioflow cli utils
Group: System Environment/Kernel
Provides: %{name}-cli = %{version}-%{release}

%package -n %{name}-adm
Summary: bioflow administration tool
Group: System Environment/Kernel
Provides: %{name}-adm = %{version}-%{release}

%package -n %{name}-xtfscli
Summary: xtao file system tool
Group: System Environment/Kernel
Provides: %{name}-xtfscli = %{version}-%{release}

%description -n %{name}-server
This is XTao bioinformatic analysis job scheduler

%description -n %{name}-cli
This is XTao bioinformatic analysis CLI utils

%description -n %{name}-adm
This is XTao bioinformatic analysis Administration tool

%description -n %{name}-xtfscli
This is XTao file system CLI utils

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
mkdir -p %{buildroot}/opt/bioflow
make DESTDIR=%{buildroot} MANDIR=%{_mandir} BINDIR=%{_sbindir} SYSTEMD_DIR=%{_unitdir} install


%clean
rm -rf %{buildroot}


%files -n %{name}-server
/opt/bioflow/bioflow

%files -n %{name}-cli
%{_bindir}/*
/opt/bioflow/doc/*

%files -n %{name}-adm
%{_sbindir}/bioadm

%files -n %{name}-xtfscli
%{_bindir}/*

%changelog
* Wed Apr 26 2017 Javen Wu <javen.wu@xtaotech.com> 
- split bioadm from biocli
- move biocli to /usr/bin

* Thu Feb 16 2017 Jason Zhang <jason.zhang@xtaotech.com> - initial version
- create the initial version

