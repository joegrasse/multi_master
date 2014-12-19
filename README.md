multi_master
===================

This script is designed to be used on mulit-master mysql setups. It can get the 
status of the read_only and event scheduler variables as well as the slave
status of any hosts passed to it. It can also perform a fail over between two
host. It changes the read_only variable to ON for the current primary host and
disables the events in the event scheduler by setting them to
SLAVESIDE_DISABLED. On the new primary host, it sets the read_only variable to
OFF and enables the events that were enable on the current primary host.
