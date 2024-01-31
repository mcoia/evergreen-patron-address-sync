# evergreen-patron-address-sync

This software facilitates the export and import of patron addresses. The export file
is generated and saved to your specified local <archive> folder. And attempted SFTP to a remote
server. The remote vendor will make corrections and send the file back. This software expects
the original columns to remain untouched, with the changes appended to the output file. This makes it
so that we can verify that the production database addresses haven't changed in the meantime.

It will send an email to a set of email addresses upon completion. If there is an error, it will
send an email to a different set of email addresses.

## Steps:

        git clone https://github.com/mcoia/evergreen-patron-address-sync.git
        cd evergreen-patron-address-sync
        cp conf.example library_config.conf
        vi library_config.conf
        # make tweaks for the database schema.table, log file, archive folder, temp folder, remote SFTP credentials, etc.
	# it's a good idea to use a different database table for each library configuration file
	# Export the file
        ./address_sync.pl --config library_config.conf
	# import
	./address_sync.pl --config library_config.conf --import

## Command line options

### Alternate path to the Evergreen config file (opensrf.xml)

        --xmlconfig /path/to/xml

Defaults to: /openils/conf/opensrf.xml

### debug

        --debug

## Tab delimited column layout:

	The first set of columns come from Evergreen:
	'address_id', 'address_type', 'street1', 'street2', 'city', 'county', 'state', 'post_code','valid'
	The second set comes from the correction vendor
	'marStreet1', 'marStreet2', 'marCity', 'marPost_Code', 'matching_score', 'distance'


