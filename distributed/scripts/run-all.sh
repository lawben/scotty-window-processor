#!/usr/bin/env bash

NUM_EXPECTED_DROPLETS=${1}
DELETE_AFTER=${2:-""}

KNOWN_HOSTS_FILE=/tmp/known_hosts
RUN_FILES_DIR=$(mktemp -d)

echo "Writing logs to $RUN_FILES_DIR"


function get_all_ips {
    doctl compute droplet list --format="PublicIPv4" --no-header
}

function ssh_cmd {
    local ip=${1}
    local cmd=${2}
    ssh -o UserKnownHostsFile=${KNOWN_HOSTS_FILE} "root@$ip" "$cmd" 2>/dev/null
}

function run_droplet {
    ssh_cmd ${1} "~/run.sh" > "$RUN_FILES_DIR/node-$ip.log"
}

function check_ready {
    local result=$(ssh_cmd ${1} "ls")
    if [[ ${result} == *"run.sh"* ]]; then
        echo "ready"
    fi
}

echo "Getting IPs..."
ALL_IPS=($(get_all_ips))
while [[ ${#ALL_IPS[@]} -lt ${NUM_EXPECTED_DROPLETS} ]]; do
    let "difference = ${NUM_EXPECTED_DROPLETS} - ${#ALL_IPS[@]}"
    echo -ne "\rWaiting for $difference more node(s) to get an IP..."
    sleep 5
    ALL_IPS=($(get_all_ips))
done
echo
echo "All IPs (${#ALL_IPS[@]}): ${ALL_IPS[@]}"
echo

echo "Adding IPs to $KNOWN_HOSTS_FILE"
> ${KNOWN_HOSTS_FILE}
for ip in ${ALL_IPS[@]}; do
    SCAN_OUTPUT=""
    echo "Adding $ip"
    while [[ ${SCAN_OUTPUT} == "" ]]; do
        SCAN_OUTPUT=$(ssh-keyscan -H -t ecdsa-sha2-nistp256 ${ip} 2>/dev/null)
        if [[ ${SCAN_OUTPUT} == "" ]]; then
            sleep 5
        fi
    done
    echo ${SCAN_OUTPUT} >> ${KNOWN_HOSTS_FILE}
done
echo

echo "Waiting for node setup to complete..."
READY_IPS=()
while [[ ${#READY_IPS[@]} -lt ${NUM_EXPECTED_DROPLETS} ]]; do
    UNREADY_IPS=($(echo ${ALL_IPS[@]} ${READY_IPS[@]} | tr ' ' '\n' | sort | uniq -u | tr '\n' ' '))
    echo -ne "\rWaiting for ${#UNREADY_IPS[@]} more node(s) to become ready..."

    for ip in ${UNREADY_IPS[@]}; do
        READY_STATUS=$(check_ready ${ip})
        if [[ ${READY_STATUS} == "ready" ]]; then
            READY_IPS+=(${ip})
        fi
    done
    sleep 10
done
echo -e "\n"


echo "Setup done. Starting \`run.sh\` on all nodes."
for ip in ${ALL_IPS[@]}; do
    run_droplet ${ip} &
done

echo
read -n 1 -p "Press [ENTER] to end script..." dummy

echo
echo "Ending script by killing all PIDs..."
for ip in ${ALL_IPS[@]}; do
    ssh_cmd ${ip} "kill -9 \$(cat /tmp/RUN_PID)"
done

echo "Killed all PIDs."

if [[ ${DELETE_AFTER} == "delete" ]]; then
    echo "Deleting all droplets..."
    doctl compute droplet delete --force $(doctl compute droplet list --format="ID" --no-header)
fi

echo "Done."
