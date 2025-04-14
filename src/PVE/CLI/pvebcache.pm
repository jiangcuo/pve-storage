package PVE::CLI::pvebcache;

use strict;
use warnings;

use PVE::Cluster;
use PVE::APLInfo;
use PVE::SafeSyslog;
use PVE::Tools qw(extract_param file_read_firstline run_command) ;
use PVE::JSONSchema qw(get_standard_option);
use PVE::CLIHandler;
use PVE::API2::Nodes;
use PVE::Storage;
use File::Basename;
use Cwd 'realpath';
use JSON;


use base qw(PVE::CLIHandler);

my $nodename = PVE::INotify::nodename();
my $showbcache = "/usr/sbin/bcache-super-show";
my $makebacahe = "/usr/sbin/make-bcache";
my $LSBLK = "/bin/lsblk";


sub setup_environment {
    PVE::RPCEnvironment->setup_default_cli_env();
}

__PACKAGE__->register_method ({
    name => 'index',
    path => 'index',
    method => 'GET',
    description => "Get list of all templates on storage",
    permissions => {
	description => "Show all users the template which have permission on that storage."
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		'type' => {
			optional => 1,
			type => 'string',
			description => "Show bcache type",
			enum => [qw(all cache backend)],
			default => 'all',
    	},
	},
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();
	my $type = $param->{type} // 'all';
	#print Dumper($res);

    my $devlist = PVE::Diskmanage::scan_bcache_device($type);

	#print Dumper($devlist);
    # foreach my $device (</sys/block/bcache*>) {
	# 	#my $disk = basename($device);
	# 	scan_bcache_device($devlist, $device, 0, 0);
    # }

	printf "%-10s %-10s %-20s %-20s  %-15s %-15s %-15s\n",
	qw(name type backend-dev  cache-dev state size cachemode);
	foreach my $rec ( @$devlist) {
	    printf "%-10s %-10s %-20s %-20s %-15s %-15s %-15s \n", 
			$rec->{name},
			$rec->{type}, 
			$rec->{'backend-dev'}, 
			$rec->{'cache-dev'}, 
	        $rec->{state},
	        $rec->{size},
			$rec->{cachemode} // 0
		;
	};

    }});

__PACKAGE__->register_method ({
    name => 'stop',
    path => 'stop',
    method => 'Post',
    description => "Stop bcache",
    permissions => {
	description => "Stop bcache"
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		dev => {
            type => 'string',
            title => 'bcache name'
        }
	},
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $dev = $param->{dev};
	my $sysdir = "/sys/block";

	die "$dev is not a bcache dev format! \n" if $dev !~ m{bcache\d+$} ;

	if ($dev =~ m{^/dev/bcache\d+$}) {
		$dev = basename($dev);
	}

	die "Stop dev $dev failed!\n" if !PVE::SysFSTools::file_write("$sysdir/$dev/bcache/stop","1");

    }});
__PACKAGE__->register_method ({
    name => 'register',
    path => 'register',
    method => 'Post',
    description => "register a bcache",
    permissions => {
	description => "register a  bcache"
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
        dev => {
            type => 'string',
            title => 'dev name'
        }
	},
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $dev =  PVE::Diskmanage::get_disk_name($param->{dev});
	die "$dev has been a bcache dev!\n" if ( -d "/sys/block/$dev/bcache/");
	return PVE::SysFSTools::file_write("/sys/fs/bcache/register","/dev/$dev");
	
    }});
__PACKAGE__->register_method ({
    name => 'create',
    path => 'create',
    method => 'Post',
    description => "register a bcache",
    permissions => {
	description => "register a  bcache"
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
        backend => {
            type => 'string',
            title => 'backend dev name'
        },
		cache => {
            type => 'string',
            title => 'Cache dev name',
			optional => 1,
        },
		blocksize => {
            type => 'integer',
            title => 'blocksize',
			optional => 1,
        },
		writeback => {
            type => 'boolean',
            title => 'enable writeback',
			default => 0,
			optional => 1,
        },
		discard => {
            type => 'boolean',
            title => 'enable discard',
			default => 1,
			optional => 1,
        },
	},
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $dev =  PVE::Diskmanage::get_disk_name($param->{backend});
	my $cache = $param->{cache};
	my $blocksize = $param->{blocksize};
	my $writeback = $param->{writeback} // 1;
	my $discard = $param->{discard} // 1;

 	die "backend $dev dev is not block device!" if !PVE::Diskmanage::verify_blockdev_path("/dev/$dev");
	die "backend $dev dev has been a bcache device!\n"  if -d "/sys/block/$dev/bcache/"; 

	my $cmd = ["$makebacahe","-B","/dev/$dev"];

	if (defined($cache)){
		die "$cache has been a cache dev,please create without cache and attach cache!\n" if check_bcache_cache_dev($cache);
		$cache =  PVE::Diskmanage::get_disk_name($cache);
		push @$cmd,"-C","/dev/$cache";
	}

	if (defined($blocksize)){
		push @$cmd,"-w",$blocksize;
	}

	if (defined($writeback)){
		push @$cmd,"--writeback";
	}
	if (defined($discard)){
		push @$cmd,"--discard";
	}

	return	run_command($cmd , outfunc => sub {}, errfunc => sub {});

    }});
__PACKAGE__->register_method ({
    name => 'detach',
    path => 'detach',
    method => 'POST',
    description => "detach a cache dev",
    permissions => {
	description => "Show all users which have permission on that host."
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		backend => {
            type => 'string',
			description => "backend dev",
        },
	}
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $backenddev =  PVE::Diskmanage::get_bcache_backend_dev($param->{backend});
	return PVE::SysFSTools::file_write("/sys/block/$backenddev/bcache/detach", "1");

    }
	});

__PACKAGE__->register_method ({
    name => 'attach',
    path => 'attach',
    method => 'POST',
    description => "attach cache dev to backend dev",
    permissions => {
	description => "Show all users which have permission on that host."
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		backend => {
            type => 'string',
			description => "backend dev",
        },
		cache => {
            type => 'string',
			description => "bcache dev",
        },
	}
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $backenddev =  PVE::Diskmanage::get_bcache_backend_dev($param->{backend});
	my $cachedev =  PVE::Diskmanage::get_bcache_cache_dev($param->{cache});

	return PVE::SysFSTools::file_write("/sys/block/$backenddev/bcache/attach", $cachedev);

    }});

__PACKAGE__->register_method ({
    name => 'create_cache',
    path => 'create_cache',
    method => 'POST',
    description => "ceate cache device",
    permissions => {
	description => "Show all users which have permission on that host."
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		cache => {
            type => 'string',
			description => "cache dev",
        },
	}
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();
	my $cachedev =  PVE::Diskmanage::get_disk_name($param->{cache});
	my $cmd =[$makebacahe , "-C","/dev/$cachedev"];
	return	run_command($cmd , outfunc => sub {}, errfunc => sub {});
	

    }});

__PACKAGE__->register_method ({
    name => 'stop_cache',
    path => 'stop_cache',
    method => 'POST',
    description => "stop cache device",
    permissions => {
	description => "Show all users which have permission on that host."
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		cache => {
            type => 'string',
			description => "cache dev",
        },
	}
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $cachedev = PVE::Diskmanage::get_bcache_cache_dev($param->{cache});
	PVE::Diskmanage::check_bcache_cache_is_inuse($cachedev);
	$cachedev =~ /^([a-zA-Z0-9_\-\.]+)$/ || die "Invalid cachedev format: $cachedev";
	my $uuid = $1;
	return PVE::SysFSTools::file_write("/sys/fs/bcache/$uuid/stop","1");
    }});




__PACKAGE__->register_method ({
    name => 'set',
    path => 'set',
    method => 'POST',
    description => "set backend device cache plicy",
    permissions => {
	description => "Show all users which have permission on that host."
	},
    proxyto => 'node',
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
		backend => {
            type => 'string',
			description => "backend dev",
        },
		cachemode => {
            type => 'string',
			description => "cache mode dev",
			enum => [qw(writethrough writeback writearound none)],
			optional => 1,
        },
		sequential => {
            type => 'integer',
			minimum => 0,
			description => "Unit is in kb",
			optional => 1,
        },
		'wb-percent' => {
            type => 'integer',
			minimum => 0,
			maximum => 80,
			description => "writeback_percent",
			optional => 1,
        },
		'clear-stats' => {
            type => 'boolean',
			optional => 1,
			default => 0,
        },
	}
    },
    returns => {
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $backenddev =  PVE::Diskmanage::get_bcache_backend_dev($param->{backend});
	my $cachemode = $param->{cachemode};
	my $sequential = $param->{sequential};
	my $wb_percent = $param->{'wb-percent'};
	my $clear = $param->{'clear-stats'};

	if (!$clear && !$wb_percent && !$sequential && !$cachemode){
		die "Need a param eg. --clear-stats 1 --wb-percent 20 --sequential 8192 --cachemode writeback\n";
	}
	my $path = "/sys/block/$backenddev/bcache";
	sub write_to_file {
		my ($file, $value) = @_;
		eval {
			if ($value){
			my $old = file_read_firstline($file);
			PVE::SysFSTools::file_write($file, $value) ;
			my $new = file_read_firstline("$file");
			my $name = basename($file);
			print "$name: $old => $new \n";
			}
		};
		warn $@ if $@;
	}
	write_to_file("$path/cache_mode", $cachemode);

	write_to_file("$path/writeback_percent", $wb_percent);
	if ($sequential){
		$sequential = PVE::Tools::convert_size($sequential, 'kb' => 'b'); 
		write_to_file("$path/sequential_cutoff", $sequential);
	}

	PVE::SysFSTools::file_write("$path/clear_stats", "1") if $clear;
	return "ok\n";
    }});


our $cmddef = {
	create => [  __PACKAGE__, 'create', [ 'backend' ] ,{ node => $nodename }],
    stop => [  __PACKAGE__, 'stop', [ 'dev' ] ,{ node => $nodename }],
	register => [  __PACKAGE__, 'register',[ 'dev' ] ,{ node => $nodename } ],
    list => [  __PACKAGE__, 'index' , [],{ node => $nodename }],
	start => { alias => 'register' },
	cache => {
		detach => [  __PACKAGE__, 'detach' , ['backend'], { node => $nodename }],
		attach => [  __PACKAGE__, 'attach' , ['backend'], { node => $nodename }],
		create => [  __PACKAGE__, 'create_cache' , ['cache'], { node => $nodename }],
		stop => [  __PACKAGE__, 'stop_cache' , ['cache'], { node => $nodename }],
		set => [  __PACKAGE__, 'set' , ['backend'], { node => $nodename }]
	}

};


1;
