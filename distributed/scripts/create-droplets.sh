#!/usr/bin/env bash

# Usage: ./create-droplets.sh numChildren numStreams

NUM_CHILDREN=${1:-0}
NUM_STREAMS=${2:-0}
NUM_EVENTS=${3:-100000000}

ROOT_TAG="root"
CHILD_TAG="child"
STREAM_TAG="stream"

CHILD_PORT=4060
ROOT_CONTROL_PORT=4055
ROOT_WINDOW_PORT=4056

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
INIT_SCRIPT_FILE="$FILE_DIR/init.sh"
SSH_KEY=$(doctl compute ssh-key list --format="ID" --no-header | head -n 1)

function create_init_script {
    local CLASS_NAME="$1"
    local FILE_NAME=`mktemp`
    local JAVA_ARGS=${@:2}
    cat "$INIT_SCRIPT_FILE" > ${FILE_NAME}
    echo -e "\n" >> ${FILE_NAME}
    echo "echo \"java -cp \$CLASSPATH de.tub.dima.scotty.distributed.executables.$CLASS_NAME ${JAVA_ARGS}\" > ~/run.sh" >> ${FILE_NAME}
    echo "chmod +x ~/run.sh" >> ${FILE_NAME}
    echo ${FILE_NAME}
}

function creat_droplet {
    local TAG_NAME="$1"
    local SCRIPT="$2"
    local DROPLET_NAME="$3"
    local WAIT="$4"
    local ITERATION=${5:-0}
    local NO_HEADER=false
    if [[ ${ITERATION} -gt 1 ]]; then
        NO_HEADER=true
    fi

    doctl compute droplet create ${DROPLET_NAME} --image ubuntu-18-04-x64 \
                                      --size s-2vcpu-2gb \
                                      --region fra1 \
                                      --tag-name "$TAG_NAME" \
                                      --ssh-keys "$SSH_KEY" \
                                      --user-data-file "$SCRIPT" \
                                      --format="ID,Name" \
                                      --wait=${WAIT} \
                                      --no-header=${NO_HEADER}
}

function get_ips {
    local TAG_NAME="$1"
    doctl compute droplet list --format="PublicIPv4" --no-header --tag-name="$TAG_NAME"
}

echo -e "Creating root node\n=================="
ROOT_SETUP_SCRIPT=$(create_init_script DistributedRootMain ${ROOT_CONTROL_PORT} ${ROOT_WINDOW_PORT} /tmp/scotty-res ${NUM_CHILDREN})
creat_droplet "$ROOT_TAG" "$ROOT_SETUP_SCRIPT" "root" true
echo

if [[ "$NUM_CHILDREN" -gt "0" ]]; then
    echo -e "Creating child nodes\n===================="

    ROOT_IP=$(get_ips ${ROOT_TAG})

    for i in $(seq ${NUM_CHILDREN}); do
        CHILD_SETUP_SCRIPT=$(create_init_script DistributedChildMain ${ROOT_IP} ${ROOT_CONTROL_PORT} ${ROOT_WINDOW_PORT} ${CHILD_PORT} "$i")
        creat_droplet "$CHILD_TAG" "$CHILD_SETUP_SCRIPT" "child-$i" false ${i}
    done
    echo
fi

if [[ "$NUM_STREAMS" -gt "0" ]]; then
    NUM_READY_CHILDREN=0
    while [[ ${NUM_READY_CHILDREN} -lt ${NUM_CHILDREN} ]]; do
        let "difference = ${NUM_CHILDREN} - ${NUM_READY_CHILDREN}"
        echo -ne "\rWaiting for $difference more child node(s) to get an IP..."
        sleep 5
        NUM_READY_CHILDREN=$(get_ips "$CHILD_TAG" | wc -l)
    done
    echo

    CHILD_IPS=($(get_ips "$CHILD_TAG"))

    echo
    echo -e "Creating stream nodes\n====================="
    let "stream_max_idx = ${NUM_STREAMS} - 1"
    for i in $(seq 0 ${stream_max_idx}); do
        let "child_idx = ${i} % ${NUM_CHILDREN}"
        let "stream_id = ${i} + 1"
        STREAM_SETUP_SCRIPT=$(create_init_script InputStreamMain ${CHILD_IPS[$child_idx]} ${CHILD_PORT} ${NUM_EVENTS} ${stream_id})
        creat_droplet "$STREAM_TAG" "$STREAM_SETUP_SCRIPT" "stream-$stream_id" false ${stream_id}
    done
fi
