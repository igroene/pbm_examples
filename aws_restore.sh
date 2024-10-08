#!/bin/bash

# Function to print usage
print_usage() {
    echo "Usage: $0 <backup_name> --config-servers=server1,server2,... --shard0=server3,server4,... --arbiters=arb1,arb2 --mongos=mongos1,mongos2,... [--execute]"
}

# Function to parse command line arguments
parse_arguments() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --config-servers=*)
                IFS=',' read -r -a config_servers <<< "${1#*=}"
                ;;
            --shard*)
                shard_name=$(echo "$1" | cut -d'=' -f1 | sed 's/--//')
                servers=$(echo "$1" | cut -d'=' -f2)
                shard_names+=("$shard_name")
                shard_servers+=("$servers")
                ;;
            --arbiters=*)
                IFS=',' read -r -a arbiter_servers <<< "${1#*=}"
                ;;
            --mongos=*)
                IFS=',' read -r -a mongos_servers <<< "${1#*=}"
                ;;
            --execute)
                dry_run=false
                ;;
            *)
                echo "Unknown argument: $1"
                print_usage
                exit 1
                ;;
        esac
        shift
    done
}

# Function to execute SSH commands
ssh_execute() {
    local host="$1"
    local command="$2"
    ssh "$host" "$command" 2>/dev/null
}

# Function to stop services
stop_services() {
    local servers=("$@")
    local service_name="$2"
    shift
    for server in "${servers[@]}"; do
        ssh_execute "$server" "sudo systemctl stop $service_name"
    done
}

# Function to process backup restoration for a specific server
process_backup_restoration() {
    local server="$1"
    local snap_server="$2"
    local snap_date="$3"
    ssh_execute "$server" "sudo umount /var/lib/mongo"
    
    # Detach EBS volume ending with "data"
    volume_id=$(aws ec2 describe-volumes --filters "Name=tag:Name,Values=$server-data" --query "Volumes[*].VolumeId" --output text)
    local availability_zone=$(aws ec2 describe-volumes --volume-ids "$volume_id" --query "Volumes[0].AvailabilityZone" --output text)

    if [ -n "$volume_id" ]; then
        aws ec2 detach-volume --volume-id "$volume_id"

        # Create new volume from snapshot
        local snapshot_id
        snapshot_id=$(aws ec2 describe-snapshots --filters "Name=tag:Name,Values=$snap_server-data" "Name=description,Values=*$snap_date*" --query "Snapshots[*].SnapshotId" --output text)
        new_volume_id=$(aws ec2 create-volume --snapshot-id "$snapshot_id" --availability-zone "$availability_zone" --volume-type gp2 --query "VolumeId" --output text)
        
        # Attach and mount the new volume
        aws ec2 attach-volume --volume-id "$new_volume_id" --instance-id $(aws ec2 describe-instances --filters "Name=tag:Name,Values=$server" --query "Reservations[*].Instances[*].InstanceId" --output text) --device /dev/sdf
        ssh_execute "$server" "sudo mount /dev/nvme1n1 /var/lib/mongo"
}

# Main script logic
if [ "$#" -lt 2 ]; then
    print_usage
    exit 1
fi

# Parse the backup name
backup_name="$1"
shift
dry_run=true
config_servers=()
shard_names=()
shard_servers=()
arbiter_servers=()
mongos_servers=()

parse_arguments "$@"

# Get the first config server
config_server="${config_servers[0]}"
# Run pbm describe-backup on the first config server
pbm_output=$(ssh "$config_server" "bash -l pbm describe-backup '$backup_name'")

# Extract required information from pbm_output
replset_names=($(echo "$pbm_output" | grep '^- name: ' | awk '{print $3}'))
snap_nodes=($(echo "$pbm_output" | grep node | awk '{print $2}' | cut -d ':' -f1))
snap_date=$(echo "$backup_name" | sed 's/T/-/; s/:/-/g; s/Z//')
config_replset_name=$(echo "$pbm_output" | grep -B5 'configsvr: true' | grep '\- name' | awk '{print $3}')

# Stop services
stop_services "${mongos_servers[@]}" "mongos"
stop_services "${arbiter_servers[@]}" "mongod"

# Run pbm restore on the first config server
ssh_execute "$config_server" "bash -l pbm restore --external"

# get the snap to restore for config servers
for i in "${!replset_names[@]}"; do
    if [ "${replset_names[i]}" = "$config_replset_name" ]; then
        snap_server="${snap_nodes[i]}"
    fi
done

# Process config servers
for data_server in "${config_servers[@]}"; do
    process_backup_restoration "$data_server" "$snap_server" "$snap_date"
done

# Process shard servers
for j in "${!shard_names[@]}"; do
    shard_name=${shard_names[$j]}
    IFS=',' read -r -a servers <<< "${shard_servers[$j]}"

    # Get the snap to restore for this shard
    for i in "${!replset_names[@]}"; do
        if [ "${replset_names[i]}" = "$shard_name" ]; then
            snap_server="${snap_nodes[i]}"
        fi
    done

    for data_server in "${servers[@]}"; do
        process_backup_restoration "$data_server" "$snap_server" "$snap_date"
    done
done

# Finish the restore process
echo "Finishing restore on $config_server..."
ssh_execute "$config_server" "bash -l pbm restore-finish -c /etc/pbm-storage.conf"

all_shard_servers=()
for j in "${!shard_names[@]}"; do
    IFS=',' read -r -a servers <<< "${shard_servers[$j]}"
    for server in "${servers[@]}"; do
        all_shard_servers+=("$server")
    done
done

# Start mongod services
for server in "${config_servers[@]}" "${all_shard_servers[@]}" "${arbiter_servers[@]}"; do
    echo "Starting mongod on $server..."
    ssh_execute "$server" "sudo systemctl start mongod"
done

# Start pbm services
for server in "${config_servers[@]}" "${all_shard_servers[@]}"; do
    echo "Starting pbm-agent on $server..."
    ssh_execute "$server" "sudo systemctl start pbm-agent"
done

# Start services for mongos servers
for mongos_server in "${mongos_servers[@]}"; do
    echo "Starting mongos on $mongos_server..."
    ssh_execute "$mongos_server" "sudo systemctl start mongos"
done

