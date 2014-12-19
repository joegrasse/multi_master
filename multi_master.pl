#!/usr/bin/perl

# ==============================================================================
# This script is designed to be used on mulit-master mysql setups. It provides 
# the ability to failover between two mysql servers, and check the current 
# status of several mysql servers.
#
# Permissions needed
# GRANT RELOAD, PROCESS, SUPER, EVENT ON *.* TO <user> 
# GRANT SELECT ON `mysql`.`event` TO <user>
#
# ============================================================================

use strict;
use warnings;
use List::Util qw(max);
use Getopt::Long;
use File::Temp;
use DBI;

my $script_path = $0;
my $script;
my $version = "1.1";

my $debug;
my $quiet;
my $force;
my $close_connections;
my $skip_lvs;
my $ask_password;
my $prompt_once;
my $user;
my $password;
my @host;
my $port;
my $socket;
my $new_primary;
my $mode;
my $lvs_write_group;
my $lvs_read_group;

my $LOG_DEBUG = 1;
my $LOG_INFO = 2;
my $LOG_WARNING = 3;
my $LOG_ERR = 4;

my $dsn = "DBI:mysql:database=";
my %dbh;
my $current_primary;
my @cleanup;
my @errors;
my $in_cleanup = 0;
my $ipvsadm = "/sbin/ipvsadm";

sub _print_msg{
  my $log_level = pop;
  my @messages = @_;
  my $fmt = "%-17s %8s %s\n";
  my $fmt2 = "%26s %s\n";    
  
  my $msg_txt = timestamp();
  my $msg_type;

  if($log_level eq $LOG_DEBUG){
    $msg_type = "Debug:";
  }
  elsif ($log_level eq $LOG_INFO){
    $msg_type = "Info:";
  }
  elsif ($log_level eq $LOG_WARNING){
    $msg_type = "Warning:";
  } 
  elsif ($log_level eq $LOG_ERR){
    $msg_type = "Error:";
  }

  foreach my $msg (@messages){
    my @msg_lines = split(/\n/,$msg);
    my $first_line = 1;
    foreach my $msg_line (@msg_lines){
      if($first_line){
        printf($fmt, $msg_txt, $msg_type, $msg_line);
      }
      else{
        printf($fmt2, '', $msg_line);
      }
      $first_line = 0;
    }      
    
  }
}

sub script_name{
  my $length = length($script_path);
  my $position = rindex($script_path, '/') + 1;
  $script = substr($script_path, $position, $length);
}

sub print_cleanup{
  my $msg = "Did not run the following cleanup commands\n";

  while(my $op = pop(@cleanup)){
    if($op->{'type'} eq 'lvs'){
      $msg .= "Type: $op->{'type'} Host: $op->{'host'} Action: $op->{'action'} Group: $op->{'group'}\n";
    }
    elsif($op->{'type'} eq 'read_only'){
      $msg .= "Type: $op->{'type'} Host: $op->{'host'} Action: $op->{'action'}\n";
    }
    elsif($op->{'type'} eq 'unlock'){
      $msg .= "Type: $op->{'type'} Host: $op->{'host'}\n";
    }
    elsif($op->{'type'} eq 'flush'){
      $msg .= "Type: $op->{'type'} Host: $op->{'host'}\n";
    }
    elsif($op->{'type'} eq 'slave_wait'){
      $msg .= "Type: $op->{'type'} Slave: $op->{'slave'} Host: $op->{'master'}\n";
    }
    elsif($op->{'type'} eq 'event'){
      $msg .= "Type: $op->{'type'} Host: $op->{'host'} Action: $op->{'action'}\n";
    }
    else{
      $msg .= "Unknown cleanup type $op->{'type'}\n";
    }
  }
  
  log_msg($msg, $LOG_WARNING);
}

sub log_msg{
  my $log_level = pop;
  my @msg = @_;
  
  # Print debug messages only if in debug mode
  return if (! $debug) and ($log_level eq $LOG_DEBUG);
  
  # Only log informational messages if quiet option is not set
  return if ($quiet) and ($log_level eq $LOG_INFO || $log_level eq $LOG_DEBUG);
  
  _print_msg(@msg, $log_level);
  
  # Abort if log level is error
  if($log_level eq $LOG_ERR && ! $in_cleanup){
    cleanup();
    exit(1);
  }
  # Got another error while cleaning up
  elsif($log_level eq $LOG_ERR && $in_cleanup){
    print_cleanup();
    exit(1);
  }
}

sub version{
  script_name();
  print $script ." $version\n";
  exit 0;
}

sub cleanup{
  $in_cleanup = 1;
  log_msg("Cleanup", $LOG_INFO);

  while(my $op = pop(@cleanup)){
    if($op->{'type'} eq 'lvs'){
      if($op->{'action'} eq 'add'){
        update_server_in_group($op->{'host'},$op->{'group'},1000,$LOG_ERR);
      }
      elsif($op->{'action'} eq 'remove'){
        remove_server_from_group($op->{'host'}, $op->{'group'}, $LOG_ERR);
      }
      else{
        log_msg("Unknown lvs cleanup action ".$op->{'action'}, $LOG_ERR);
      }      
    }
    elsif($op->{'type'} eq 'read_only'){
      set_read_only($op->{'host'},$op->{'action'});
    }
    elsif($op->{'type'} eq 'unlock'){
      unlock_tables($op->{'host'});
    }
    elsif($op->{'type'} eq 'flush'){
      flush_tables($op->{'host'});
    }
    elsif($op->{'type'} eq 'slave_wait'){
      slave_wait($op->{'slave'},$op->{'master'});
    }
    elsif($op->{'type'} eq 'event'){
      set_event_scheduler($op->{'host'},$op->{'action'});
    }
    else{
      log_msg("Unknown cleanup type ".$op->{'type'}, $LOG_ERR);
    }
  }
  
  db_disconnect();
  
  $in_cleanup = 0;
}
  
sub hms{
  my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
  return sprintf("%02d:%02d:%02d", $hour, $min, $sec);
}

sub ymd
{
  my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
  return sprintf("%04d%02d%02d", $year+1900, $mon+1, $mday);
}

sub timestamp { return ymd() . " " . hms(); }

sub run_command{
  my $stderr;
  my $stdout;
  my @return = (1,'','');
  
  my $fh_stdout = File::Temp->new();
  my $tmp_stdout = $fh_stdout->filename;

  my $fh_stderr = File::Temp->new();
  my $tmp_stderr = $fh_stderr->filename;
    
  log_msg("Running: " . join(" ", @_), $LOG_DEBUG);
  
  my $return_code = system("@_ 1>$tmp_stdout 2>$tmp_stderr");
  
  my $program_status = $?;
  my $program_error = $!;
  
  if(-e $tmp_stderr){
    $stderr = `cat $tmp_stderr`;
    chomp($stderr);
    unlink $tmp_stderr;
  }
  
  if(-e $tmp_stdout){
    $stdout = `cat $tmp_stdout`;
    chomp($stdout);
    unlink $tmp_stdout;
  }
  
  if($return_code == 0 && $program_status == 0){
    $return[0] = 1;
  }
  else{
    if(! $stderr){
      if($program_status & 0xff){
        $stderr = "received signal " . ($program_status & 0xff);
      } 
      elsif($program_status >> 8){
       $stderr = "exit status " . ($program_status >> 8);
      } 
      else{
        $stderr = $program_error;
      }
    }
    
    chomp($stderr);
    
    $return[0] = 0;
  }
  
  $return[1] = $stdout;
  $return[2] = $stderr;
  
  if(wantarray){
    return @return;
  }

  return $return[0];
}

sub help{
  script_name();

print <<EOF;

$script Version $version

This script is designed to be used on mulit-master mysql setups. It can get the 
status of the read_only and event scheduler variables as well as the slave
status of any hosts passed to it. It can also perform a fail over between two
host. It changes the read_only variable to ON for the current primary host and
disables the events in the event scheduler by setting them to
SLAVESIDE_DISABLED. On the new primary host, it sets the read_only variable to
OFF and enables the events that were enable on the current primary host.

Usage: $script --mode=[mode] --host=[host]

Options:
  --mode              Can be one of "status" or "set_primary". Mode of status 
                      reports back host status, and set_primary sets a host to 
                      be primary.
  --host              The hosts to interact with. Can be pass multiple times for 
                      more than one host, or can be a comma separated list of 
                      hosts. Example: host or host:port
  --primary           Sets the host that will become primary. Must be provided
                      in host list.
  --force             Used with mode "set_primary" if there is only one host 
                      provided.
  --close-connections Kills off any none system user connections (e.g. 
                      replication, event scheduler users) on the current primary 
                      server.
  --user              MySQL username. (Current: $user)
  --password          MySQL password
  --port              MySQL port
  --socket            MySQL socket
  --lvs-write-group   LVS write group IP address and port. Required when not 
                      skipping lvs. Example: host:port
  --lvs-read-group    LVS read group IP address and port. Example: host:port
  --skip-lvs          Disable LVS changes
  --ask-password      Prompt for mysql password before connecting
  --prompt-once       Only prompt for password once and use it for all connections
  --debug             Turns on debugging output.
  --quiet             Suppress diagnostic output, prints warnings and errors 
                      only.
  --version           Prints version information.
  --help              Prints this help.

EOF
 exit 1;
}

sub load_defaults{
  $user = $ENV{'USER'};
  $debug = 0;
  $quiet = 0;
  $force = 0;
  $close_connections = 0;
  $skip_lvs = 0;
  $ask_password = 0;
  $prompt_once = 0;
}

sub load_args{
  my $ret = GetOptions(
    "help" => \&help,
    "version" => \&version,
    "user=s" => \$user,
    "password=s" => \$password,
    "host=s" => \@host,
    "port=i" => \$port,
    "socket=s" => \$socket,
    "primary=s" => \$new_primary,
    "force" => \$force,
    "mode=s" => \$mode,
    "close-connections" => \$close_connections,
    "lvs-write-group=s" => \$lvs_write_group,
    "lvs-read-group=s" => \$lvs_read_group,
    "skip-lvs" => \$skip_lvs,
    "ask-password" => \$ask_password,
    "prompt-once" => \$prompt_once,
    "debug" => \$debug,
    "quiet" => \$quiet
  ) or help();
  
  @host = split(/,/,join(',',@host));
}

sub validate_args{
  if(! defined $mode){
    print "Option mode is required\n";
    help();
  }
  
  if(! @host){
    print "Option host is required\n";
    help();
  }
  
  if($lvs_write_group && $lvs_write_group !~ /^[^:]+:.+$/){
    print "Option lvs-write-group requires ip and port\n";
    help();
  }
  
  if($lvs_read_group && $lvs_read_group !~ /^[^:]+:.+$/){
    print "Option lvs-read-group requires ip and port\n";
    help();
  }

  if($mode eq "set_primary"){
    if(! defined $new_primary){
      print 'Option primary requires an argument for mode "set_primary"'."\n";
      help();
    }
    elsif(@host == 1 && ! $force){
      print 'Option host only has one host, must use "force" to continue for mode "set_primary"'."\n";
      help();
    }
    elsif(@host == 1 && $force){
    }
    elsif(@host != 2){
      print 'Invalid number of hosts for mode "set_primary"'."\n";
      help();
    }
    
    if(! $skip_lvs && ! defined $lvs_write_group){
      print "Option lvs-write-group is required. Use skip-lvs to continue or specify the lvs write group\n";
      help();
    }
    
    # Set new_primary to proper format
    my ($host, $set_port, $used_port, $used_socket) = get_host_options($new_primary);
    $new_primary = $host.(($set_port) ? ":$set_port" : '');
  }
  elsif($mode eq "status"){
  }
  else{
    print "Invalid mode\n";
    help();
  }
}

sub prompt_password{
  my $prompt = shift;
  my $password;
  
  print $prompt;
  system("stty -echo");
  $password = <STDIN>;
  system("stty echo");
  print "\n";
  chomp($password);
  
  return $password;
}

sub get_host_options{
  my $db_host = shift;
  
  my $server;
  my $set_port;
  my $used_port;
  my $used_socket;    
  
  # Check for host
  if(length($db_host) > 0){
    # Check if setting port with host
    if($db_host =~ /:/){
      ($server,$set_port) = split(/:/,$db_host);
      
      if(!$server){
        log_msg("Host is undefined for $db_host",$LOG_ERR);
      }
      if(!$set_port){
        log_msg("Port is undefined for $db_host",$LOG_ERR);
      }
    }
    else{
      $server = $db_host;
    }            
    
    # Check if localhost
    if($server =~ /^localhost$/){
      if($socket){            
        $used_socket = $socket;
      }
    }
    # Port set with host
    elsif($set_port){
      $used_port = $set_port;
    }
    # Port set with option
    elsif($port){            
      $used_port = $port;   
      $set_port = $port;
    }
    else{
      $used_port = '3306';
    }
  }
  # localhost and socket set
  elsif($socket){      
    $server = 'localhost';
    $used_socket = $socket;
  }
  else{
    $server = 'localhost';
  }
  
  return ($server, $set_port, $used_port, $used_socket);
}

sub db_connect{
  my $already_prompted = 0;
  my %attr = (
		PrintError => 0,
		RaiseError => 0
	);

  foreach my $db_host (@host){
    my $tmp_dsn = $dsn;
    
    my ($server, $set_port, $used_port, $used_socket) = get_host_options($db_host);
    
    $tmp_dsn .= ";host=".$server;
    $tmp_dsn .= ";port=".$set_port if $set_port;
    $tmp_dsn .= ";mysql_socket=".$used_socket if $used_socket;            
    
    my $host_name = $server.(($set_port) ? ":$set_port" : '');
    
    log_msg("Connecting to $host_name with DSN string $tmp_dsn", $LOG_DEBUG);
    
    if($ask_password && !$already_prompted){
      if($prompt_once){
        $password = prompt_password("Enter password: ");
        $already_prompted = 1;
      }
      else{
        $password = prompt_password("Enter password for $host_name: ");
      }
    }    

    $dbh{$host_name} = {'dbh' => DBI->connect($tmp_dsn,$user,$password, \%attr), 'host' => $server.(($used_port) ? ':'.$used_port : ''), 'server' => $server};
    $dbh{$host_name}{'port'} = $used_port if $used_port;
    $dbh{$host_name}{'socket'} = $used_socket if $used_socket;
    
    if (!$dbh{$host_name}{'dbh'}){
      log_msg ($DBI::errstr, $LOG_ERR);
    }
  }
}

sub db_disconnect{
  foreach my $host_name (keys %dbh){
    if($dbh{$host_name}{'dbh'}){
      log_msg("Disconnecting from $host_name", $LOG_DEBUG);
      $dbh{$host_name}{'dbh'}->disconnect();
    }
  }
}

sub lvs_status{
  my $virtual_service = shift;
  my %lvs_status;
  
  my @program = (
    $ipvsadm,
    '-ln'
  );
  
  if(defined($virtual_service)){
    push(@program, "-t $virtual_service");
  }
  
  log_msg("Getting lvs status".($virtual_service ? " for virtual service $virtual_service." : "."), $LOG_DEBUG);
  my ($success, $stdout, $stderr) = run_command(@program);
  
  if(! $success){
    log_msg($stderr, $LOG_ERR);
  }
  
  if($stdout){
    log_msg($stdout,$LOG_DEBUG);
    
    my @a_stdout = split(/\n/, $stdout);
    
    # Check for version line
    if($a_stdout[0] =~ /^IP Virtual Server version/){
      #throw away
      shift(@a_stdout);
    }
    
    # Check format of service header
    log_msg("Check format of service header", $LOG_DEBUG);
    my $service_header = shift(@a_stdout);
    log_msg($service_header,$LOG_DEBUG);
    if(!$service_header || !($service_header =~ /^Prot LocalAddress:Port Scheduler Flags$/)){
      log_msg("Unrecognized service header", $LOG_ERR);
    }
    
    # Check format of server header
    log_msg("Check format of server header", $LOG_DEBUG);
    my $server_header = shift(@a_stdout);
    log_msg($server_header,$LOG_DEBUG);
    if(!$server_header || !($server_header =~ /^  -> RemoteAddress:Port           Forward Weight ActiveConn InActConn$/)){
      log_msg("Unrecognized server header", $LOG_ERR);
    }
    
    my $last_service;
    foreach my $line (@a_stdout){
      log_msg($line,$LOG_DEBUG);
      # Look for server line
      # example:   -> 10.22.179.76:3306            Masq    1      63         15
      if($line =~ /^\s{2}->\s([^:]+):(\d+)\s+([^\s]+)\s+(\d+)\s+(\d+)\s+(\d+)/){
        log_msg("Found server line",$LOG_DEBUG);
        
        if(!$last_service){
          log_msg("Found server line before service",$LOG_ERR);
        }
        
        my $server = $1.":".$2;
        
        $lvs_status{$last_service}{'servers'}{$server} = {
          'ip' => $1,
          'port' => $2,
          'forward' => $3,
          'weight' => $4,
          'active_connections' => $5,
          'inactive_connections' => $6
        };
        
        log_msg("Server: $lvs_status{$last_service}{'servers'}{$server}{'ip'} Port: $lvs_status{$last_service}{'servers'}{$server}{'port'} Forward: $lvs_status{$last_service}{'servers'}{$server}{'forward'} Weight: $lvs_status{$last_service}{'servers'}{$server}{'weight'} Active Connections: $lvs_status{$last_service}{'servers'}{$server}{'active_connections'} Inactive Connections: $lvs_status{$last_service}{'servers'}{$server}{'inactive_connections'}", $LOG_DEBUG);
        
      }
      # service line
      # example: TCP  10.22.180.171:3306 wlc persistent 120
      elsif($line =~ /^(.{4})\s([^\s]+)\s([^\s]+)\s?(.*)/){
        log_msg("Found service line",$LOG_DEBUG);
        my $service = $2;
        $lvs_status{$service} = {
          'protocol' => $1,
          'service' => $2,
          'scheduler' => $3
        };
        
        # Strip spaces off of protocol
        $lvs_status{$service}{'protocol'} =~ s/\s+$//;
        
        log_msg("Protocol: $lvs_status{$service}{'protocol'} Service: $lvs_status{$service}{'service'} Scheduler: $lvs_status{$service}{'scheduler'}",$LOG_DEBUG);
        
        $last_service = $service;
      }
      else{
        log_msg("Found unknown format",$LOG_ERR);
      }
    }
    
    return %lvs_status;
  }
}

sub lvs_add{
  my $server = shift;
  my $service = shift;
  my $weight = 1000;
  
  $weight = shift if @_;
  
  my @program = (
    $ipvsadm,
    '-a',
    '-t '.$service,
    '-r '.$server,
    '-m',
    '-w '.$weight
  );
  
  log_msg("Adding real server $server to lvs service $service with weight $weight", $LOG_DEBUG);
  my ($success, $stdout, $stderr) = run_command(@program);
  
  if(!$success){
    log_msg("Error: $stderr", $LOG_DEBUG);
  }

  return ($success, $stdout, $stderr);
}

sub lvs_edit{
  my $server = shift;
  my $service = shift;
  my $weight = shift;
  
  my @program = (
    $ipvsadm,
    '-e',
    '-t '.$service,
    '-r '.$server,
    '-m',
    '-w '.$weight
  );
  
  log_msg("Changing weight of real server $server in lvs service $service to $weight", $LOG_DEBUG);
  my ($success, $stdout, $stderr) = run_command(@program);
  
  if(!$success){
    log_msg("Error: $stderr", $LOG_DEBUG);
  }

  return ($success, $stdout, $stderr);
}

sub lvs_delete{
  my $server = shift;
  my $service = shift;
  
  my @program = (
    $ipvsadm,
    '-d',
    '-t '.$service,
    '-r '.$server
  );
  
  log_msg("Removing real server $server from lvs service $service", $LOG_DEBUG);
  my ($success, $stdout, $stderr) = run_command(@program);
  
  if(!$success){
    log_msg("Error: $stderr", $LOG_DEBUG);
  }

  return ($success, $stdout, $stderr);
}

sub remove_server_from_group{
  my $host_name = shift;
  my $group = shift;
  my $error_level = shift;
  
  # Attempt removal from group
  log_msg("Removing $host_name from group $group",$LOG_INFO);
  my ($success, $stdout, $stderr) = lvs_delete($dbh{$host_name}{'host'}, $group);
  
  if(!$success){
    # Check that server wasn't already removed
    if($stderr =~ /No such destination/i){
      # Count as success
    }
    # Check for invalid service
    elsif($stderr =~ /Service not defined/i){
      log_msg("Group not defined",$error_level);
    }
    else{
      log_msg("$stderr",$error_level);
    }
  }
}

sub update_server_in_group{
  my $host_name = shift;
  my $group = shift;
  my $weight = shift;
  my $error_level = shift;  
  
  # Attempt update of server
  log_msg("Updating $host_name in group $group to weight $weight",$LOG_INFO);
  my ($success, $stdout, $stderr) = lvs_edit($dbh{$host_name}{'host'}, $group, $weight);
  
  if(!$success){
    # Check that server doesn't exist
    if($stderr =~ /No such destination/i){
      # Attempt to add server to group
      ($success, $stdout, $stderr) = lvs_add($dbh{$host_name}{'host'}, $group, $weight);
      
      if(!$success){
        # Check that server was already added
        if($stderr =~ /Destination already exists/i){
          log_msg("Server was updated outside this process",$error_level);
        }
        # Check for invalid service
        elsif($stderr =~ /Service not defined/i){
          log_msg("Group not defined",$error_level);
        }
        else{
          log_msg("$stderr",$error_level);
        }
      }
    }
    # Check for invalid service
    elsif($stderr =~ /Service not defined/i){
      log_msg("Group not defined",$error_level);
    }
    else{
      log_msg("$stderr",$error_level);
    }
  }
}

sub get_status{
  log_msg("Getting hosts status", $LOG_DEBUG);
  foreach my $host_name (keys %dbh){    
    get_read_event_status($host_name);

    get_slave_status($host_name);
  }
  
  get_lvs_status();
  get_current_primary();
}

sub get_read_event_status{
  my $host = shift;

  log_msg("Getting read/event status for host $host.", $LOG_DEBUG);  
  my $sth = $dbh{$host}{'dbh'}->prepare("SHOW VARIABLES WHERE Variable_name = 'read_only' OR Variable_name = 'event_scheduler'");
  
  if(!$sth){
    log_msg("Failed prepare of SHOW VARIABLES LIKE 'read_only' " . (($dbh{$host}{'dbh'}->errstr) ? "with: ".$dbh{$host}{'dbh'}->errstr." " : ''). "on $host", $LOG_ERR);
  }
  if(!$sth->execute){
    log_msg("Failed SHOW VARIABLES LIKE 'read_only' with ".$sth->errstr." on $host", $LOG_ERR)
  }
  
  while(my $ref = $sth->fetchrow_hashref()){
    if($ref->{'Variable_name'} eq 'read_only'){
      $dbh{$host}{'read_only'} = $ref->{'Value'};
    }
    if($ref->{'Variable_name'} eq 'event_scheduler'){
      $dbh{$host}{'event_scheduler'} = $ref->{'Value'};
    }
  }
  
  if(!$dbh{$host}{'event_scheduler'}){
    $dbh{$host}{'event_scheduler'} = "NOT AVAILABLE";
  }
}

sub get_slave_status{
  my $host = shift;

  log_msg("Getting slave status for host $host.", $LOG_DEBUG);
  my $hash_ref = $dbh{$host}{'dbh'}->selectrow_hashref("SHOW SLAVE STATUS");
  if(!$hash_ref){
    if($dbh{$host}{'dbh'}->errstr){
      log_msg("Failed SHOW SLAVE STATUS with ". $dbh{$host}{'dbh'}->errstr." on $host", $LOG_ERR);
    }
    else{
      $dbh{$host}{'slave_running'} = "NOT AVAILABLE";
    }
  }
  else{
    if($hash_ref->{'Slave_IO_Running'} eq 'Yes' && $hash_ref->{'Slave_SQL_Running'} eq 'Yes'){
      $dbh{$host}{'slave_running'} = "ON";
    }
    else{
      $dbh{$host}{'slave_running'} = "OFF";
    }
  }
}

sub get_lvs_status{
  my @lvs_groups = ();
  
  # Check that we are using an lvs
  if($lvs_write_group){
    push(@lvs_groups,$lvs_write_group);
    push(@lvs_groups,$lvs_read_group) if $lvs_read_group;
  
    # Get the current lvs status
    my %lvs_status = lvs_status();
    
    # Look for each server in the current lvs status
    foreach my $host_name (keys %dbh){
      foreach my $lvs_group (@lvs_groups){
        my $host = $dbh{$host_name}{'host'};
        
        if($lvs_status{$lvs_group}{'servers'}{$host}{'ip'}){
          log_msg("Found server ".$lvs_status{$lvs_group}{'servers'}{$host}{'ip'}." in lvs group ".$lvs_group,$LOG_DEBUG);
          $dbh{$host_name}{'lvs'}{$lvs_group} = $lvs_status{$lvs_group}{'servers'}{$host};
        }
      }
    }
  }
}

sub get_current_primary{
  my $current_primary_candidate;
  my $error = 0;
  
  log_msg("Finding current primary server.", $LOG_DEBUG);
  
  # Loop through the db servers to locate the current primary
  foreach my $host_name (keys %dbh){
    # Check that a primary isn't already found, that host is not set to read 
    # only and that the lvs write group, and points to it if applicable
    if(!$current_primary_candidate && $dbh{$host_name}{'read_only'} eq 'OFF' && (!$lvs_write_group || $dbh{$host_name}{'lvs'}{$lvs_write_group}{'weight'})){
      $current_primary_candidate = $host_name;
    }
    # read only turned off but lvs write group doesn't point to it
    elsif($dbh{$host_name}{'read_only'} eq 'OFF'){
      $error = 1;
    }
    # read only on but lvs write group points to it
    elsif($dbh{$host_name}{'read_only'} eq 'ON' && $lvs_write_group && $dbh{$host_name}{'lvs'}{$lvs_write_group}{'weight'}){
      $error = 1;
    }
  }
  
  if($error || !$current_primary_candidate){
    log_msg("Could not determine current primary server", $LOG_WARNING);
  }
  else{
    log_msg("Current primary server is ".$current_primary_candidate, $LOG_DEBUG);
    $current_primary = $current_primary_candidate;
    $dbh{$current_primary}{'primary'} = 1;
  }
}

sub slave_running{
  my $host = shift;
  
  if($dbh{$host}{'slave_running'} ne 'ON'){
    return 0;
  }
  
  return 1;
}

sub print_status{ 
  # Find out the max length of the host names
  my $max_host = max(5, map { length($_ = ($_.($dbh{$_}{'primary'} ? '*':''))) } keys %dbh);
  my $fmt = "| %-${max_host}s | %-9s | %-15s | %-13s |";
  my $bar = "+-".("-" x $max_host)."-+-".("-" x 9)."-+-".("-" x 15)."-+-".("-" x 13)."-+";
  my @header = ("Host", "Read Only", "Event Scheduler", "Slave");
  
  # Check if we need to add lvs status
  if($lvs_write_group){
    $fmt .= " %22s |";
    $bar .= "-".("-" x 22)."-+";
    push(@header, 'LVS Write Group Weight');
  }
  
  if($lvs_read_group){
    $fmt .= " %21s |";
    $bar .= "-".("-" x 21)."-+";
    push(@header, 'LVS Read Group Weight');
  }
  
  $fmt .= "\n";
  
  # Print Header
  print $bar."\n";
  printf($fmt, @header);
  print $bar."\n";

  # Print db servers
  foreach my $host_name (keys %dbh){
    my @options = (($dbh{$host_name}{'primary'} ? '*':'').$host_name, $dbh{$host_name}{'read_only'}, $dbh{$host_name}{'event_scheduler'}, $dbh{$host_name}{'slave_running'});
    
    # Check if we need to add lvs status
    if($lvs_write_group && $dbh{$host_name}{'lvs'}{$lvs_write_group}{'weight'}){
      push(@options, $dbh{$host_name}{'lvs'}{$lvs_write_group}{'weight'});
    }
    elsif($lvs_write_group){
      push(@options, 'NOT AVAILABLE');
    }
    
    if($lvs_read_group && $dbh{$host_name}{'lvs'}{$lvs_read_group}{'weight'}){
      push(@options, $dbh{$host_name}{'lvs'}{$lvs_read_group}{'weight'});
    }
    elsif($lvs_read_group){
      push(@options, 'NOT AVAILABLE');
    }
    
    printf($fmt, @options);
  }
  
  print $bar."\n";
  print "* - Denotes current primary\n";
}

sub kill_connections{
  my $host = shift;
  
  log_msg("Killing off open non-system connections on $host", $LOG_INFO);
  log_msg("Getting list of open connections", $LOG_DEBUG);
  
  my $sth = $dbh{$host}{'dbh'}->prepare("SHOW PROCESSLIST");
  if(!$sth){
    log_msg("Failed prepare of SHOW PROCESSLIST " . (($dbh{$host}{'dbh'}->errstr) ? "with: ".$dbh{$host}{'dbh'}->errstr." " : ''). "on $host", $LOG_ERR);
  }
  if(!$sth->execute){
    log_msg("Failed executing SHOW PROCESSLIST with ".$sth->errstr." on $host", $LOG_ERR)
  }
  
  log_msg("Current connection id on $host is ".$dbh{$host}{'dbh'}->{'mysql_thread_id'}, $LOG_DEBUG);
  while(my $p_ref = $sth->fetchrow_arrayref()){
    # Skip connection if it is my current connection
    # Skip connection if user = "system user" OR "event_scheduler"
    # Skip connection if command = "Daemon" OR "Binlog Dump" OR "Binlog Dump GTID"
    log_msg("Checking connection User: ".$p_ref->[1].(($p_ref->[2]) ? "@".$p_ref->[2] : '')." Id: ".$p_ref->[0]." Command: ".$p_ref->[4].(($p_ref->[6]) ? " State: ".$p_ref->[6] : '').(($p_ref->[7]) ? " Info: ".$p_ref->[7] : ''),$LOG_DEBUG);
    if($p_ref->[0] != $dbh{$host}{'dbh'}->{'mysql_thread_id'} && 
      $p_ref->[1] ne "system user" && $p_ref->[1] ne "event_scheduler" && 
      $p_ref->[4] ne "Daemon" && $p_ref->[4] ne "Binlog Dump" && $p_ref->[4] ne "Binlog Dump GTID")
    {
      # Kill off user connection
      log_msg("Killing connection User: ".$p_ref->[1]."@".$p_ref->[2]." Id: ".$p_ref->[0]." Command: ".$p_ref->[4].(($p_ref->[6]) ? " State: ".$p_ref->[6] : ''),$LOG_INFO);
      my $sth2 = $dbh{$host}{'dbh'}->do("KILL ".$p_ref->[0]);
      if(!$sth2){
        # Check if error is because connection is no longer available
        if($dbh{$host}{'dbh'}->err == 1094){
          my $error = $dbh{$host}{'dbh'}->errstr;
          $sth->finish;
          log_msg("Failed killing connection with: ".$error." on $host", $LOG_WARNING);
        }
        else{
          my $error = $dbh{$host}{'dbh'}->errstr;
          $sth->finish;
          log_msg("Failed killing connection with: ".$error." on $host", $LOG_ERR);
        }
      }
    }
  }
}

sub get_master_status{
  my $host = shift;
  my %status;
  
  log_msg("Getting current Master Status on $host", $LOG_INFO);
  my $hash_ref = $dbh{$host}{'dbh'}->selectrow_hashref("SHOW MASTER STATUS");

  if(!$hash_ref){
    log_msg("Failed SHOW MASTER STATUS " . ((defined($dbh{$host}{'dbh'}->errstr) && $dbh{$host}{'dbh'}->errstr ne "") ? "with: ".$dbh{$host}{'dbh'}->errstr." " : ''). "on $host", $LOG_ERR);
  }
  
  log_msg("Master file: ". $hash_ref->{'File'}. " Master Position: ". $hash_ref->{'Position'}, $LOG_DEBUG);
  return %status = ('file' => $hash_ref->{'File'}, 'position' => $hash_ref->{'Position'});
}

sub slave_wait{
  my $slave = shift;
  my $master = shift;

  my %status = get_master_status($master);
  
  # Wait on slave until caught up to master
  log_msg("Waiting on $slave until caught up to $master", $LOG_INFO);
  my $array_ref = $dbh{$slave}{'dbh'}->selectrow_arrayref("SELECT MASTER_POS_WAIT('".$status{'file'}."', ".$status{'position'}.")");
  if(!$array_ref){        
    log_msg("Failed SELECT MASTER_POS_WAIT " . ((defined($dbh{$slave}{'dbh'}->errstr) && $dbh{$slave}{'dbh'}->errstr ne "") ? "with: ".$dbh{$slave}{'dbh'}->errstr." " : ''). "on $slave", $LOG_ERR);
  }
  else{
    # Check that MASTER_POS_WAIT command was successful
    if(! defined($array_ref->[0])){
      log_msg("Failed to wait on $slave until caught up to $master because of problem with slave thread on $slave",$LOG_ERR);
    }
    else{
      log_msg("$slave caught up to $master with MASTER_POS_WAIT return value of ".$array_ref->[0], $LOG_DEBUG);
    }
  }
}

sub enable_events{
  my $one_host = shift;
  my $host;
  
  log_msg("Enabling events on $new_primary", $LOG_INFO);

  # if there is only one host we need to get events off of the new primary server
  if($one_host){
    $host = $new_primary;
  }
  else{
    $host = $current_primary
  }
  
  # Get list of events to enable
  log_msg("Getting list of events from $host", $LOG_DEBUG);
  # if there is only one host then we will only get events that have been disabled on the slave side
  # else we will get all events but slave side desabled
  my $sth = $dbh{$host}{'dbh'}->prepare("SELECT db,name,status,definer from mysql.event WHERE status ".(($one_host) ? "" : "!")."= 'SLAVESIDE_DISABLED' order by db;");
  if(!$sth){
    log_msg("Failed prepare of selecting event list " . (($dbh{$host}{'dbh'}->errstr) ? "with: ".$dbh{$host}{'dbh'}->errstr." " : ''). "on $host", $LOG_ERR);
  }
  if(!$sth->execute){
    log_msg("Failed selecting event list with ".$sth->errstr." on $host", $LOG_ERR)
  }
  
  # Update the events on the new primary server
  while(my $array_ref = $sth->fetchrow_arrayref()){
    # Break the definer up into the user and user host
    my ($user, $user_host) = split(/@([^@]*)$/,$array_ref->[3]);
    my $sql = "ALTER DEFINER = `".$user."`@`".$user_host."` EVENT `".$array_ref->[0]."`.`".$array_ref->[1]."` ";
    
    # Check if event was disabled
    if($array_ref->[2] eq 'DISABLED'){
      # Disable event on new primary server 
      log_msg("Setting ".$array_ref->[0].".".$array_ref->[1]." to DISABLED on $new_primary",$LOG_DEBUG);
      my $np_sth = $dbh{$new_primary}{'dbh'}->do($sql."DISABLE");
      if(!$np_sth){
        my $error = $dbh{$new_primary}{'dbh'}->errstr;
        $sth->finish;
        log_msg("Failed disabling event ".$array_ref->[0].".".$array_ref->[1].(($error) ? " with: ".$error : ''). " on $new_primary", $LOG_ERR);
      }
    }
    # Check if event was enabled
    elsif($array_ref->[2] eq 'ENABLED' || ($one_host && $array_ref->[2] eq 'SLAVESIDE_DISABLED')){
      # Enable event on new primary server
      log_msg("Setting ".$array_ref->[0].".".$array_ref->[1]." to ENABLED on $new_primary",$LOG_DEBUG);
      my $np_sth = $dbh{$new_primary}{'dbh'}->do($sql."ENABLE");
      if(!$np_sth){
        my $error = $dbh{$new_primary}{'dbh'}->errstr;
        $sth->finish;
        log_msg("Failed enabling event ".$array_ref->[0].".".$array_ref->[1].(($error) ? " with: ".$error : ''). " on $new_primary", $LOG_ERR);
      }
    }
    # Check if slaveside disabled with more than one host
    elsif($array_ref->[2] eq 'SLAVESIDE_DISABLED'){
      log_msg("Doing nothing with event ".$array_ref->[0].".".$array_ref->[1]." that is SLAVESIDE_DISABLED",$LOG_DEBUG);
    }
    # Event status is unknown
    else{
      log_msg("Event ".$array_ref->[0].".".$array_ref->[1]." has unknown status of ".$array_ref->[2],$LOG_WARNING);
    }
  }
}

sub set_read_only{
  my $host = shift;
  my $set_to = shift;
  
  log_msg("Setting $host to read_only=$set_to", $LOG_INFO);
  my $sth = $dbh{$host}{'dbh'}->do("SET GLOBAL read_only = '".$set_to."'");
  if(!$sth){
    log_msg("Failed setting read_only='".$set_to."' " . ((defined($dbh{$host}{'dbh'}->errstr) && $dbh{$host}{'dbh'}->errstr ne "") ? "with: ".$dbh{$host}{'dbh'}->errstr." " : ''). "on $host", $LOG_ERR)
  }
}

sub set_event_scheduler{
  my $host = shift;
  my $set_to = shift;
  
  log_msg("Turning event scheduler on $host $set_to", $LOG_INFO);
  my $sth = $dbh{$host}{'dbh'}->do("SET GLOBAL event_scheduler = '".$set_to."'");
  if(!$sth){
    log_msg("Failed setting event_scheduler='".$set_to."' " . ((defined($dbh{$host}{'dbh'}->errstr) && $dbh{$host}{'dbh'}->errstr ne "") ? "with: ".$dbh{$host}{'dbh'}->errstr." " : ''). "on $host", $LOG_ERR);
  }
}

sub get_event_scheduler{
  my $host = shift;
  
  return $dbh{$host}{'event_scheduler'};
}

sub flush_tables{
  my $host = shift;
  
  log_msg("Flushing tables on $host with read lock", $LOG_INFO);
  my $sth = $dbh{$host}{'dbh'}->do("FLUSH TABLES WITH READ LOCK");
  if(!$sth){      
    log_msg("Failed flushing tables with: ".$dbh{$host}{'dbh'}->errstr." on $host", $LOG_ERR);
  }  
}

sub unlock_tables{
  my $host = shift;
  
  log_msg("Unlocking tables on $host", $LOG_INFO);
  my $sth = $dbh{$host}{'dbh'}->do("UNLOCK TABLES");
  if(!$sth){
    log_msg("Failed UNLOCK TABLES with: ".$dbh{$host}{'dbh'}->errstr." on $host", $LOG_WARNING);
  }
}

sub set_primary{
  my $p_sth;
  my $np_sth;
  my $one_host = 0;
  my $error_str;
  
  if(@host == 1){
    $one_host = 1;
  }
  
  # Check that new primary is in host list
  if(! defined $dbh{$new_primary}){
    log_msg("Can not set $new_primary to primary, not provided in hosts", $LOG_ERR);
  }

  # Check that new primary is not already primary
  if($dbh{$new_primary}{'primary'}){
    log_msg("Host $new_primary is already set to primary", $LOG_ERR);
  }

  if(! $one_host){
    # Multiple host, check that we know who is currently primary
    if(! defined $current_primary){
      log_msg("Could not determine current primary server", $LOG_ERR);
    }
    
    if(!$skip_lvs){
      # Remove current primary from write group
      remove_server_from_group($current_primary, $lvs_write_group, $LOG_ERR);
      push(@cleanup, {'type' => 'lvs','host' => $current_primary,'action' => 'add','group' => $lvs_write_group});
    }

    set_read_only($current_primary,'ON');
    push(@cleanup, {'type' => 'read_only','host' => $current_primary,'action' => 'OFF'});

    flush_tables($current_primary);
    push(@cleanup, {'type' => 'unlock','host' => $current_primary});
    
    # Check that the slave is running on the new primary
    if(!slave_running($new_primary)){
      log_msg("Slave is not running on $new_primary", $LOG_ERR);
    }

    # Wait on new primary until caught up with current primary
    slave_wait($new_primary, $current_primary);
  }

  # Turn read_only off on new master
  set_read_only($new_primary,'OFF');
  push(@cleanup,
    {'type' => 'slave_wait','slave' => $current_primary,'master' => $new_primary},
    {'type' => 'flush','host' => $new_primary}
  ) unless $one_host;
  push(@cleanup,{'type' => 'read_only','host' => $new_primary,'action' => 'ON'});

  if(!$skip_lvs){
    # Enable new primary in write group
    update_server_in_group($new_primary,$lvs_write_group,1000,$LOG_ERR);
    push(@cleanup, {'type' => 'lvs','host' => $new_primary,'action' => 'remove','group' => $lvs_write_group});
  }

  if(! $one_host){
    # Kill open connections
    kill_connections($current_primary) if $close_connections;

    # unlock tables on current master
    unlock_tables($current_primary);
  }
  
  # Past the point were we want to cleanup previous tasks if we fail.
  # Clear cleanup tasks
  @cleanup = ();
  
  if(!$skip_lvs && $lvs_read_group && ! $one_host){
    # Enable current primary in read group
    update_server_in_group($current_primary,$lvs_read_group,1000,$LOG_WARNING);
    
    # Remove new primary from read group
    remove_server_from_group($new_primary, $lvs_read_group,$LOG_WARNING);
  }

  # Check for event scheduler
  log_msg("Checking event schedulers", $LOG_INFO);
  my $turn_es_on = 0;
  
  if(! $one_host){
    log_msg("Event scheduler ".get_event_scheduler($current_primary)." on $current_primary", $LOG_DEBUG);
  }
  log_msg("Event scheduler ".get_event_scheduler($new_primary)." on $new_primary", $LOG_DEBUG);
  
  if(($one_host || get_event_scheduler($current_primary) ne "NOT AVAILABLE") && get_event_scheduler($new_primary) ne "NOT AVAILABLE"){
    # Check if event scheduler is disabled on one of the servers
    if((! $one_host && get_event_scheduler($current_primary) eq 'DISABLED') || get_event_scheduler($new_primary) eq 'DISABLED'){
      if($one_host){
        log_msg("The event scheduler is disable", $LOG_INFO);
      }
      else{
        if(get_event_scheduler($current_primary) eq 'ON' && get_event_scheduler($new_primary) eq 'DISABLED'){
          log_msg("The event scheduler is DISABLED on $new_primary can not switch over events", $LOG_ERR);
        }
      }
    }
    # check if we need to turn on the event scheduler
    elsif($one_host || get_event_scheduler($current_primary) eq 'ON'){
      if(! $one_host){
        # event scheduler is on, turn it off on the current primary server
        set_event_scheduler($current_primary,'OFF');
        push(@cleanup, 
          {'type' => 'event','host' => $current_primary,'action' => 'ON'},
          {'type' => 'slave_wait','slave' => $current_primary,'master' => $new_primary},
        );
        $turn_es_on = 1;
      }

      # enable events on new primary server
      enable_events($one_host);

      # Turn event scheduler ON on new primary
      if(get_event_scheduler($new_primary) eq 'OFF'){
        set_event_scheduler($new_primary,'ON');
      }

      # Past the point were we want to cleanup previous tasks if we fail.
      # Clear cleanup tasks
      @cleanup = ();

      # Turn event scheduler back ON if needed on the current primary
      if($turn_es_on){
        $turn_es_on = 0;
        # check that the slave is running on the current primary (now slave)
        if(!slave_running($current_primary)){
          my %status = get_master_status($new_primary);
          log_msg("Not turning event scheduler back on on $current_primary because slave is not running.", $LOG_WARNING);
          log_msg("Enable slave, run SELECT MASTER_POS_WAIT('".$status{'file'}."', ".$status{'position'}.")", $LOG_WARNING);
          log_msg("and then turn on event scheduler.", $LOG_WARNING);
        }
        else{
          # Wait on current primary (now slave) until caught up with new primary (now master)
          slave_wait($current_primary, $new_primary);

          set_event_scheduler($current_primary,'ON');
        }
      }
    }
    else{
      log_msg("Event scheduler is not enabled on $current_primary not changing $new_primary", $LOG_INFO);
    }
  }
  else{
    log_msg("Event schedulers not found", $LOG_INFO);
  }
}

sub main{
  load_defaults();
  load_args();
  validate_args();
  
  db_connect();
  get_status();

  if($mode eq "status"){
    print_status();
  }
  elsif($mode eq "set_primary"){
    set_primary();
  }
  
  db_disconnect();
}

main();
exit(0);