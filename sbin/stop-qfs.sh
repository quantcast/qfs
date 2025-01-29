#!/usr/bin/env bash

#
# stop-qfs.sh
#
#	Written by Michael Kamprath <michael@kamprath.net>
#
# 	Stops QFS by terminating the Meta Server on the node this script is run on and
#   terminating a Chunk Server on each node the in ${QFS_CONF_DIR}/chunk_servers.
#	Will load ${QFS_CONF_DIR}/qfs-env.sh if it exists to set environment variables.
#
#	Environment Variables
#
#		QFS_HOME
#			The file path to the QFS installation. Not settable in qfs-env.sh.
#			Defaults to "/usr/local/qfs"
#
#		QFS_CONF_DIR
#			The directory path of QFS configuration on all nodes. Not settable in qfs-env.sh
#			Defaults to "${QFS_HOME}/conf"
#
#		QFS_USER
#			The user that the QFS processes will be launched under.
#			Defaults to the user that launched this script.
#
#		METASERVER_HOST_IP
#			The address of the server that will run the Meta Server process
#			Defaults to "localhost"
#
#		START_QFS_SH_DEBUG
#			A boolean value indicating whether debug logging should be emitted from this script.
#

# remote_command_exec
#	Executes a command remotely
#
#	Arguments
#		$1 - the server address to connect to
#		$2 - The user to execute the command as
#		$3 - the command to execute
#
function remote_command_eval () 
{
	if [ "$START_QFS_SH_DEBUG" = true ]; then
		echo "Running: ssh -l ${2} ${1} \"${3}\""
	fi
	local RESULTS
    RESULTS=$(ssh -l ${2} ${1} "${3}")
}

this="${BASH_SOURCE:-$0}"
bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)

PKILL_CMD=${PKILL_CMD:-"/usr/bin/pkill"}
QFS_HOME=${QFS_HOME:-"/usr/local/qfs"}
QFS_CONF_DIR=${QFS_CONF_DIR:-"${QFS_HOME}/conf"}


if [[ -f "${QFS_CONF_DIR}/qfs-env.sh" ]]; then
	if [ "$START_QFS_SH_DEBUG" = true ]; then
		echo "Sourcing environment variables from ${QFS_CONF_DIR}/qfs-env.sh"
    fi
    source "${QFS_CONF_DIR}/qfs-env.sh"
fi

#
# set needed environment variables if not set already
#
QFS_USER=${QFS_USER:-$USER}
METASERVER_HOST_IP=${METASERVER_HOST_IP:-"localhost"}

#
# Stop the Web UI
#

echo "Stopping Meta Server Web UI on ${METASERVER_HOST_IP}"
remote_command_eval $METASERVER_HOST_IP $QFS_USER "${PKILL_CMD} -c -f qfsstatus.py"
if [ $? -eq 0 ]; then
	echo "Meta Server Web UI stopped."
else
	echo "Failed to stop Meta Server Web UI"
fi

#
# Stop the Meta Server
#
echo "Stopping Meta Server on ${METASERVER_HOST_IP}"
remote_command_eval $METASERVER_HOST_IP $QFS_USER "${PKILL_CMD} -c metaserver"
if [ $? -eq 0 ]; then
	echo "Meta Server stopped."
else
	echo "Failed to stop Meta Server"
fi

#
# Stop each Chunk Server
#
QFS_CHUNK_SERVERS_FILE="${QFS_CONF_DIR}/chunk_servers"

if [[ -f "${QFS_CHUNK_SERVERS_FILE}" ]]; then
	CHUNK_SERVER_LIST="$(< ${QFS_CHUNK_SERVERS_FILE})"
	
    for chunk_server in $CHUNK_SERVER_LIST; do
        echo "${chunk_server} - Stopping ChunkServer"
        if [[ $chunk_server = *[!\ ]* ]]; then
        	remote_command_eval $chunk_server $QFS_USER "${PKILL_CMD} -c chunkserver"
			if [ $? -eq 0 ]; then
				echo "${chunk_server} - ChunkServer stopped."
			else
				echo "${chunk_server} - Failed to stop ChunkServer"
			fi
		fi
    done
    
    echo "Done stopping Chunk Servers"
else
	echo "Could not find chunk_servers file. No Chunk Servers stopped."
fi
