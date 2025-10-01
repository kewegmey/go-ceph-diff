# RBD incremental backup script.

set -euo pipefail

full_backup=0

# Parse arguments for --full
args=()
for arg in "$@"; do
    if [[ "$arg" == "--full" ]]; then
        full_backup=1
    else
        args+=("$arg")
    fi
done

if [[ ${#args[@]} -lt 2 ]]; then
    echo "Usage: $0 [--full] <rbd_image1> [<rbd_image2> ...] <destination_zfs_dataset>"
    exit 1
fi

# Get destination dataset (last argument)
dest_dataset="${args[@]: -1}"
# Get images (all but last argument)
images=("${args[@]:0:${#args[@]}-1}")

timestamp="$(date +%Y%m%d-%H%M%S)"

for img in "${images[@]}"; do
    # Split pool/image
    pool="${img%%/*}"
    image="${img##*/}"

    snap="autobackup-${timestamp}"

    echo "Creating snapshot ${pool}/${image}@${snap}..."
    rbd snap create "${pool}/${image}@${snap}"

    # Check if zvol exists
    zvol_path="${dest_dataset}/${pool}/${image}"
    if ! zfs list -H -o name "${zvol_path}" &>/dev/null; then
        echo "Creating dataset ${dest_dataset}/${pool} if needed..."
        zfs list -H -o name "${dest_dataset}/${pool}" &>/dev/null || zfs create "${dest_dataset}/${pool}"

        echo "Getting RBD image size..."
        img_size=$(rbd info --format json "${pool}/${image}" | jq -r '.size')
        echo "Creating zvol ${zvol_path} with size ${img_size}..."
        zfs create -sV "${img_size}B" "${zvol_path}"
        full_backup=1
    fi
    if [[ "${full_backup}" -eq 1 ]]; then
        echo "Exporting full RBD image to zvol..."
        for i in {1..3}; do
            if [ -e "/dev/zvol/${zvol_path}" ]; then
                break
            fi
            echo "Waiting for /dev/zvol/${zvol_path} to appear..."
            sleep 2
        done
        rbd export --export-format 1 "${pool}/${image}@${snap}" - | dd of="/dev/zvol/${zvol_path}" bs=4M status=none
    else
        echo "Performing incremental backup for ${pool}/${image}..."
        # Find second most recent snapshot
        last_snap=$(rbd snap ls "${pool}/${image}" | awk '/autobackup-/ {print $2}' | sort | tail -n 2 | head -n 1)
        if [[ -z "${last_snap}" ]]; then
            echo "No previous backup snapshot found, exiting."
            exit 1
        else
            echo "Exporting diff from ${last_snap} to ${snap}..."
            for i in {1..3}; do
            if [ -e "/dev/zvol/${zvol_path}" ]; then
                break
            fi
            echo "Waiting for /dev/zvol/${zvol_path} to appear..."
            sleep 2
            done
            cmd="rbd export-diff --from-snap \"${last_snap}\" \"${pool}/${image}@${snap}\" - | ./rbd-diff-apply --output \"/dev/zvol/${zvol_path}\""
            echo "Running: $cmd"
            eval "$cmd"
        fi
    fi
    zfs snapshot "${zvol_path}@${snap}"
    echo "Backup of ${pool}/${image} completed."    
done
