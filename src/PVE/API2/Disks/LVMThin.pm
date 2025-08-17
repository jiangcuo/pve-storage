package PVE::API2::Disks::LVMThin;

use strict;
use warnings;

use PVE::Storage::LvmThinPlugin;
use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::API2::Storage::Config;
use PVE::Storage;
use PVE::Tools qw(run_command lock_file);

use PVE::RPCEnvironment;
use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    protected => 1,
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
    },
    description => "List LVM thinpools",
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
                lv => {
                    type => 'string',
                    description => 'The name of the thinpool.',
                },
                vg => {
                    type => 'string',
                    description => 'The associated volume group.',
                },
                lv_size => {
                    type => 'integer',
                    description => 'The size of the thinpool in bytes.',
                },
                used => {
                    type => 'integer',
                    description => 'The used bytes of the thinpool.',
                },
                metadata_size => {
                    type => 'integer',
                    description => 'The size of the metadata lv in bytes.',
                },
                metadata_used => {
                    type => 'integer',
                    description => 'The used bytes of the metadata lv.',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;
        return PVE::Storage::LvmThinPlugin::list_thinpools(undef);
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
    description => "Create an LVM thinpool",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            name => get_standard_option('pve-storage-id'),
            device => {
                type => 'string',
                description => 'The block device you want to create the thinpool on.',
            },
            add_storage => {
                description => "Configure storage using the thinpool.",
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

        my $name = $param->{name};
        my $dev = $param->{device};
        my $node = $param->{node};

        $dev = PVE::Diskmanage::verify_blockdev_path($dev);
        PVE::Diskmanage::assert_disk_unused($dev);

        my $storage_params = {
            type => 'lvmthin',
            vgname => $name,
            thinpool => $name,
            storage => $name,
            content => 'rootdir,images',
            nodes => $node,
        };
        my $verify_params = [qw(vgname thinpool)];

        if ($param->{add_storage}) {
            $rpcenv->check($user, "/storage", ['Datastore.Allocate']);

            # reserve the name and add as disabled, will be enabled below if creation works out
            PVE::API2::Storage::Config->create_or_update(
                $name, $node, $storage_params, $verify_params, 1,
            );
        }

        my $worker = sub {
            PVE::Diskmanage::locked_disk_action(sub {
                PVE::Diskmanage::assert_disk_unused($dev);

                die "volume group with name '${name}' already exists on node '${node}'\n"
                    if PVE::Storage::LVMPlugin::lvm_vgs()->{$name};

                if (PVE::Diskmanage::is_partition($dev)) {
                    eval { PVE::Diskmanage::change_parttype($dev, '8E00'); };
                    warn $@ if $@;
                }

                PVE::Storage::LVMPlugin::lvm_create_volume_group($dev, $name);
                my $pv = PVE::Storage::LVMPlugin::lvm_pv_info($dev);
                # keep some free space just in case
                my $datasize = $pv->{size} - 128 * 1024;
                # default to 1% for metadata
                my $metadatasize = $datasize / 100;
                # but at least 1G, as recommended in lvmthin man
                $metadatasize = 1024 * 1024 if $metadatasize < 1024 * 1024;
                # but at most 16G, which is the current lvm max
                $metadatasize = 16 * 1024 * 1024 if $metadatasize > 16 * 1024 * 1024;
                # shrink data by needed amount for metadata
                $datasize -= 2 * $metadatasize;

                run_command([
                    '/sbin/lvcreate',
                    '--type',
                    'thin-pool',
                    "-L${datasize}K",
                    '--poolmetadatasize',
                    "${metadatasize}K",
                    '-n',
                    $name,
                    $name,
                ]);

                PVE::Diskmanage::udevadm_trigger($dev);

                if ($param->{add_storage}) {
                    PVE::API2::Storage::Config->create_or_update(
                        $name, $node, $storage_params, $verify_params,
                    );
                }
            });
        };

        return $rpcenv->fork_worker('lvmthincreate', $name, $user, $worker);
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
    description => "Remove an LVM thin pool.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            name => get_standard_option('pve-storage-id'),
            'volume-group' => get_standard_option('pve-storage-id'),
            'cleanup-config' => {
                description =>
                    "Marks associated storage(s) as not available on this node anymore "
                    . "or removes them from the configuration (if configured for this node only).",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            'cleanup-disks' => {
                description => "Also wipe disks so they can be repurposed afterwards.",
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

        my $vg = $param->{'volume-group'};
        my $lv = $param->{name};
        my $node = $param->{node};

        my $worker = sub {
            PVE::Diskmanage::locked_disk_action(sub {
                my $thinpools = PVE::Storage::LvmThinPlugin::list_thinpools();

                die "no such thin pool ${vg}/${lv}\n"
                    if !grep { $_->{lv} eq $lv && $_->{vg} eq $vg } $thinpools->@*;

                run_command(['lvremove', '-y', "${vg}/${lv}"]);

                my $config_err;
                if ($param->{'cleanup-config'}) {
                    my $match = sub {
                        my ($scfg) = @_;
                        return
                            $scfg->{type} eq 'lvmthin'
                            && $scfg->{vgname} eq $vg
                            && $scfg->{thinpool} eq $lv;
                    };
                    eval {
                        PVE::API2::Storage::Config->cleanup_storages_for_node($match, $node);
                    };
                    warn $config_err = $@ if $@;
                }

                if ($param->{'cleanup-disks'}) {
                    my $vgs = PVE::Storage::LVMPlugin::lvm_vgs(1);

                    die "no such volume group '$vg'\n" if !$vgs->{$vg};
                    die "volume group '$vg' still in use\n" if $vgs->{$vg}->{lvcount} > 0;

                    my $wiped = [];
                    eval {
                        for my $pv ($vgs->{$vg}->{pvs}->@*) {
                            my $dev = PVE::Diskmanage::verify_blockdev_path($pv->{name});
                            PVE::Diskmanage::wipe_blockdev($dev);
                            push $wiped->@*, $dev;
                        }
                    };
                    my $err = $@;
                    PVE::Diskmanage::udevadm_trigger($wiped->@*);
                    die "cleanup failed - $err" if $err;
                }

                die "config cleanup failed - $config_err" if $config_err;
            });
        };

        return $rpcenv->fork_worker('lvmthinremove', "${vg}-${lv}", $user, $worker);
    },
});

1;
