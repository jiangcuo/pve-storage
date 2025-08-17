package PVE::API2::Disks::Directory;

use strict;
use warnings;

use POSIX;

use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::RPCEnvironment;
use PVE::Systemd;
use PVE::Tools qw(run_command trim file_set_contents file_get_contents dir_glob_foreach lock_file);

use PVE::API2::Storage::Config;

use base qw(PVE::RESTHandler);

my $SGDISK = '/sbin/sgdisk';
my $MKFS = '/sbin/mkfs';
my $BLKID = '/sbin/blkid';

my $read_ini = sub {
    my ($filename) = @_;

    my $content = file_get_contents($filename);
    my @lines = split /\n/, $content;

    my $result = {};
    my $section;

    foreach my $line (@lines) {
        $line = trim($line);
        if ($line =~ m/^\[([^\]]+)\]/) {
            $section = $1;
            if (!defined($result->{$section})) {
                $result->{$section} = {};
            }
        } elsif ($line =~ m/^(.*?)=(.*)$/) {
            my ($key, $val) = ($1, $2);
            if (!$section) {
                warn "key value pair found without section, skipping\n";
                next;
            }

            if ($result->{$section}->{$key}) {
                # make duplicate properties to arrays to keep the order
                my $prop = $result->{$section}->{$key};
                if (ref($prop) eq 'ARRAY') {
                    push @$prop, $val;
                } else {
                    $result->{$section}->{$key} = [$prop, $val];
                }
            } else {
                $result->{$section}->{$key} = $val;
            }
        }
        # ignore everything else
    }

    return $result;
};

my $write_ini = sub {
    my ($ini, $filename) = @_;

    my $content = "";

    foreach my $sname (sort keys %$ini) {
        my $section = $ini->{$sname};

        $content .= "[$sname]\n";

        foreach my $pname (sort keys %$section) {
            my $prop = $section->{$pname};

            if (!ref($prop)) {
                $content .= "$pname=$prop\n";
            } elsif (ref($prop) eq 'ARRAY') {
                foreach my $val (@$prop) {
                    $content .= "$pname=$val\n";
                }
            } else {
                die "invalid property '$pname'\n";
            }
        }
        $content .= "\n";
    }

    file_set_contents($filename, $content);
};

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    protected => 1,
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
    },
    description => "PVE Managed Directory storages.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
        },
    },
    returns => {
        type => 'array',
        items => {
            type => 'object',
            properties => {
                unitfile => {
                    type => 'string',
                    description => 'The path of the mount unit.',
                },
                path => {
                    type => 'string',
                    description => 'The mount path.',
                },
                device => {
                    type => 'string',
                    description => 'The mounted device.',
                },
                type => {
                    type => 'string',
                    description => 'The filesystem type.',
                },
                options => {
                    type => 'string',
                    description => 'The mount options.',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $result = [];

        dir_glob_foreach(
            '/etc/systemd/system',
            '^mnt-pve-(.+)\.mount$',
            sub {
                my ($filename, $storid) = @_;
                $storid = PVE::Systemd::unescape_unit($storid);

                my $unitfile = "/etc/systemd/system/$filename";
                my $unit = $read_ini->($unitfile);

                push @$result,
                    {
                        unitfile => $unitfile,
                        path => "/mnt/pve/$storid",
                        device => $unit->{'Mount'}->{'What'},
                        type => $unit->{'Mount'}->{'Type'},
                        options => $unit->{'Mount'}->{'Options'},
                    };
            },
        );

        return $result;
    },
});

__PACKAGE__->register_method({
    name => 'create',
    path => '',
    method => 'POST',
    proxyto => 'node',
    protected => 1,
    permissions => {
        description =>
            "Requires additionally 'Datastore.Allocate' on /storage when setting 'add_storage'",
        check => ['perm', '/', ['Sys.Modify']],
    },
    description =>
        "Create a Filesystem on an unused disk. Will be mounted under '/mnt/pve/NAME'.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            name => get_standard_option('pve-storage-id'),
            device => {
                type => 'string',
                description => 'The block device you want to create the filesystem on.',
            },
            add_storage => {
                description => "Configure storage using the directory.",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            filesystem => {
                description => "The desired filesystem.",
                type => 'string',
                enum => ['ext4', 'xfs'],
                optional => 1,
                default => 'ext4',
            },
        },
    },
    returns => { type => 'string' },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $user = $rpcenv->get_user();

        my $name = $param->{name};
        my $dev = $param->{device};
        my $node = $param->{node};
        my $type = $param->{filesystem} // 'ext4';
        my $path = "/mnt/pve/$name";
        my $mountunitname = PVE::Systemd::escape_unit($path, 1) . ".mount";
        my $mountunitpath = "/etc/systemd/system/$mountunitname";

        $dev = PVE::Diskmanage::verify_blockdev_path($dev);
        PVE::Diskmanage::assert_disk_unused($dev);

        my $storage_params = {
            type => 'dir',
            storage => $name,
            content => 'rootdir,images,iso,backup,vztmpl,snippets',
            is_mountpoint => 1,
            path => $path,
            nodes => $node,
        };
        my $verify_params = [qw(path)];

        if ($param->{add_storage}) {
            $rpcenv->check($user, "/storage", ['Datastore.Allocate']);

            # reserve the name and add as disabled, will be enabled below if creation works out
            PVE::API2::Storage::Config->create_or_update(
                $name, $node, $storage_params, $verify_params, 1,
            );
        }

        my $mounted = PVE::Diskmanage::mounted_paths();
        die "the path for '${name}' is already mounted: ${path} ($mounted->{$path})\n"
            if $mounted->{$path};
        die "a systemd mount unit already exists: ${mountunitpath}\n" if -e $mountunitpath;

        my $worker = sub {
            PVE::Diskmanage::locked_disk_action(sub {
                PVE::Diskmanage::assert_disk_unused($dev);

                my $part = $dev;

                if (PVE::Diskmanage::is_partition($dev)) {
                    eval { PVE::Diskmanage::change_parttype($dev, '8300'); };
                    warn $@ if $@;
                } else {
                    # create partition
                    my $cmd = [$SGDISK, '-n1', '-t1:8300', $dev];
                    print "# ", join(' ', @$cmd), "\n";
                    run_command($cmd);

                    my ($devname) = $dev =~ m|^/dev/(.*)$|;
                    $part = "/dev/";
                    dir_glob_foreach(
                        "/sys/block/$devname",
                        qr/\Q$devname\E.+/,
                        sub {
                            my ($partition) = @_;
                            $part .= $partition;
                        },
                    );
                }

                # create filesystem
                my $cmd = [$MKFS, '-t', $type, $part];
                print "# ", join(' ', @$cmd), "\n";
                run_command($cmd);

                # create systemd mount unit and enable & start it
                my $ini = {
                    'Unit' => {
                        'Description' => "Mount storage '$name' under /mnt/pve",
                    },
                    'Install' => {
                        'WantedBy' => 'multi-user.target',
                    },
                };

                my $uuid_path;
                my $uuid;

                $cmd = [$BLKID, $part, '-o', 'export'];
                print "# ", join(' ', @$cmd), "\n";
                run_command(
                    $cmd,
                    outfunc => sub {
                        my ($line) = @_;

                        if ($line =~ m/^UUID=(.*)$/) {
                            $uuid = $1;
                            $uuid_path = "/dev/disk/by-uuid/$uuid";
                        }
                    },
                );

                die "could not get UUID of device '$part'\n" if !$uuid;

                $ini->{'Mount'} = {
                    'What' => $uuid_path,
                    'Where' => $path,
                    'Type' => $type,
                    'Options' => 'defaults',
                };

                $write_ini->($ini, $mountunitpath);

                PVE::Diskmanage::udevadm_trigger($part);

                run_command(['systemctl', 'daemon-reload']);
                run_command(['systemctl', 'enable', $mountunitname]);
                run_command(['systemctl', 'start', $mountunitname]);

                if ($param->{add_storage}) {
                    PVE::API2::Storage::Config->create_or_update(
                        $name, $node, $storage_params, $verify_params,
                    );
                }
            });
        };

        return $rpcenv->fork_worker('dircreate', $name, $user, $worker);
    },
});

__PACKAGE__->register_method({
    name => 'delete',
    path => '{name}',
    method => 'DELETE',
    proxyto => 'node',
    protected => 1,
    permissions => {
        description =>
            "Requires additionally 'Datastore.Allocate' on /storage when setting 'cleanup-config'",
        check => ['perm', '/', ['Sys.Modify']],
    },
    description => "Unmounts the storage and removes the mount unit.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            name => get_standard_option('pve-storage-id'),
            'cleanup-config' => {
                description =>
                    "Marks associated storage(s) as not available on this node anymore "
                    . "or removes them from the configuration (if configured for this node only).",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            'cleanup-disks' => {
                description => "Also wipe disk so it can be repurposed afterwards.",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
        },
    },
    returns => { type => 'string' },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $user = $rpcenv->get_user();

        $rpcenv->check($user, "/storage", ['Datastore.Allocate']) if $param->{'cleanup-config'};

        my $name = $param->{name};
        my $node = $param->{node};

        my $worker = sub {
            my $path = "/mnt/pve/$name";
            my $mountunitname = PVE::Systemd::escape_unit($path, 1) . ".mount";
            my $mountunitpath = "/etc/systemd/system/$mountunitname";

            PVE::Diskmanage::locked_disk_action(sub {
                my $to_wipe;
                if ($param->{'cleanup-disks'}) {
                    my $unit = $read_ini->($mountunitpath);

                    my $dev = PVE::Diskmanage::verify_blockdev_path($unit->{'Mount'}->{'What'});
                    $to_wipe = $dev;

                    # clean up whole device if this is the only partition
                    $dev =~ s|^/dev/||;
                    my $info = PVE::Diskmanage::get_disks($dev, 1, 1);
                    die "unable to obtain information for disk '$dev'\n" if !$info->{$dev};
                    $to_wipe = $info->{$dev}->{parent}
                        if $info->{$dev}->{parent} && scalar(keys $info->%*) == 2;
                }

                run_command(['systemctl', 'stop', $mountunitname]);
                run_command(['systemctl', 'disable', $mountunitname]);

                unlink $mountunitpath
                    or $! == ENOENT
                    or die "cannot remove $mountunitpath - $!\n";

                my $config_err;
                if ($param->{'cleanup-config'}) {
                    my $match = sub {
                        my ($scfg) = @_;
                        return $scfg->{type} eq 'dir' && $scfg->{path} eq $path;
                    };
                    eval {
                        PVE::API2::Storage::Config->cleanup_storages_for_node($match, $node);
                    };
                    warn $config_err = $@ if $@;
                }

                if ($to_wipe) {
                    PVE::Diskmanage::wipe_blockdev($to_wipe);
                    PVE::Diskmanage::udevadm_trigger($to_wipe);
                }

                die "config cleanup failed - $config_err" if $config_err;
            });
        };

        return $rpcenv->fork_worker('dirremove', $name, $user, $worker);
    },
});

1;
