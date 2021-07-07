#!/usr/bin/env bash

#
# start-qfs.sh
#
#	Written by Michael Kamprath <michael@kamprath.net>
#
# 	Starts QFS by launch the Meta Server on the node this script is run on and
#   launching a Chunk Server on each node the in ${QFS_CONF_DIR}/chunk_servers.
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
#		QFS_BIN_DIR
#			The directory path of QFS executables.
#			Defaults to "${QFS_HOME}/bin"
#
#		QFS_LOGS_DIR
#			The directory where logs should be written.
#			Defaults to "${QFS_HOME}/logs"
#
#		METASERVER_CONF_FILENAME
#			The file name used for the configuration of the Meta Server
#			Defaults to "Metaserver.prp"
#
#		CHUNKSERVER_CONF_FILENAME
#			The file name used for the configuration of the Chunk Server 
#			Defaults to "Chunkserver.prp"
#
#		METASERVER_WEBUI_CONF_FILENAME
#			The file name used for the configuration of the Meta Server Web UI
#			Defaults to "webUI.cfg"
#
#		METASERVER_LOG_FILENAME
#			The file name for the log of stdout for the Meta Server 
#			Defaults to "metaserver.log"
#
#		CHUNKSERVER_LOG_FILENAME
#			The file name for the log of stdout from the Chunk Server
#			Defaults to "chunkserver.log"
#
#		WEBUI_LOG_FILENAME
#			The file name for the log of stdout from the Mea Server Web UI
#			Defaults to "qfsWebUI.log"
#
#		NOHUP_CMD
#			Complete filepath to the nohup command
#			Defaults to "/usr/bin/nohup"
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

function qfs_usage
{
  echo "Usage: start-qfs.sh"
}

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

NOHUP_CMD=${NOHUP_CMD:-"/usr/bin/nohup"}
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
QFS_BIN_DIR=${QFS_BIN_DIR:-"${QFS_HOME}/bin"}
QFS_LOGS_DIR=${QFS_LOGS_DIR:-"${QFS_HOME}/logs"}
METASERVER_CONF_FILENAME=${METASERVER_CONF_FILENAME:-"Metaserver.prp"}
CHUNKSERVER_CONF_FILENAME=${CHUNKSERVER_CONF_FILENAME:-"Chunkserver.prp"}
METASERVER_WEBUI_CONF_FILENAME=${METASERVER_WEBUI_CONF_FILENAME:-"webUI.cfg"}
METASERVER_LOG_FILENAME=${METASERVER_LOG_FILENAME:-"metaserver.log"}
CHUNKSERVER_LOG_FILENAME=${CHUNKSERVER_LOG_FILENAME:-"chunkserver.log"}
WEBUI_LOG_FILENAME=${WEBUI_LOG_FILENAME:-"qfsWebUI.log"}
METASERVER_LOG_FILEPATH=${METASERVER_LOG_FILEPATH:-"${QFS_LOGS_DIR}/${METASERVER_LOG_FILENAME}"}
CHUNKSERVER_LOG_FILEPATH=${CHUNKSERVER_LOG_FILEPATH:-"${QFS_LOGS_DIR}/${CHUNKSERVER_LOG_FILENAME}"}
QFS_METASERVER_START_CMD=${QFS_METASERVER_START_CMD:-"${QFS_BIN_DIR}/metaserver ${QFS_CONF_DIR}/${METASERVER_CONF_FILENAME}"}
QFS_CHUNKSERVER_START_CMD=${QFS_CHUNKSERVER_START_CMD:-"${QFS_BIN_DIR}/chunkserver ${QFS_CONF_DIR}/${CHUNKSERVER_CONF_FILENAME}"}

#
# Start MetaServer
#

echo "Starting Meta Server with logging to ${METASERVER_LOG_FILEPATH}"
EVAL_METASERVER_CMD="${NOHUP_CMD} ${QFS_METASERVER_START_CMD} &>${METASERVER_LOG_FILEPATH} &"
remote_command_eval $METASERVER_HOST_IP $QFS_USER "${EVAL_METASERVER_CMD}"
if [ $? -eq 0 ]; then
	echo "Meta Server started."
else
	echo "Meta Server failed to launch. Aborting!"
	exit 1
fi

#
# Start Chunk Servers - assumes 
# 
QFS_CHUNK_SERVERS_FILE="${QFS_CONF_DIR}/chunk_servers"

if [[ -f "${QFS_CHUNK_SERVERS_FILE}" ]]; then
	CHUNK_SERVER_LIST="$(< ${QFS_CHUNK_SERVERS_FILE})"
	
    for chunk_server in $CHUNK_SERVER_LIST; do
        echo "${chunk_server} - Starting ChunkServer"
        if [[ $chunk_server = *[!\ ]* ]]; then
        	remote_command_eval $chunk_server $QFS_USER "${NOHUP_CMD} ${QFS_CHUNKSERVER_START_CMD} &>${CHUNKSERVER_LOG_FILEPATH} &"
			if [ $? -eq 0 ]; then
				echo "${chunk_server} - ChunkServer started."
			else
				echo "${chunk_server} - Failed to start ChunkServer."
			fi
		fi
    done
    
    echo "Done launching Chunk Servers"
else
	echo "Could not find chunk_servers file. No Chunk Servers launched."
fi

#
# Start Meta Server Web UI
#

EVAL_WEBUI_CMD="${NOHUP_CMD} ${QFS_HOME}/webui/qfsstatus.py ${QFS_CONF_DIR}/${METASERVER_WEBUI_CONF_FILENAME} &>${QFS_LOGS_DIR}/${WEBUI_LOG_FILENAME} &"
echo "Starting Meta Server Web UI"
remote_command_eval $METASERVER_HOST_IP $QFS_USER "${EVAL_WEBUI_CMD}"
if [ $? -eq 0 ]; then
	echo "Meta Server Web UI started."
else
	echo "Meta Server Web UI failed to launch."
	exit 1
fi
