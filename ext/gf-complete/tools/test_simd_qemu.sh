#!/bin/bash -e

# This script will use QEMU to test gf-complete especially SIMD support
# on different architectures and cpus. It will boot a qemu machine 
# and run an Ubuntu cloud image. All testing will happen inside the 
# QEMU machine. 

# The following packages are required:
#   qemu-system-aarch64
#   qemu-system-arm
#   qemu-system-x86_64
#   genisoimage


script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
qemu_dir="${script_dir}/.qemu"
ssh_port=2222
ssh_pubkey_file="${qemu_dir}/qemu.pub"
ssh_key_file="${qemu_dir}/qemu"

mkdir -p "${qemu_dir}"

cleanup() {
    if [[ -n "$(jobs -p)" ]]; then
        echo killing qemu processes "$(jobs -p)"
        kill $(jobs -p)
    fi
}

trap cleanup EXIT

start_qemu() {
    arch=$1
    cpu=$2

    image_version="xenial"
    image_url_base="http://cloud-images.ubuntu.com/${image_version}/current"

    case $arch in
        i[[3456]]86*|x86_64*|amd64*)
            image_kernel="${image_version}-server-cloudimg-amd64-vmlinuz-generic"
            image_initrd="${image_version}-server-cloudimg-amd64-initrd-generic"
            image_disk="${image_version}-server-cloudimg-amd64-disk1.img"
            ;;
        aarch64*) 
            image_kernel="${image_version}-server-cloudimg-arm64-vmlinuz-generic"
            image_initrd="${image_version}-server-cloudimg-arm64-initrd-generic"
            image_disk="${image_version}-server-cloudimg-arm64-disk1.img"
            ;;
        arm*) 
            image_kernel="${image_version}-server-cloudimg-armhf-vmlinuz-lpae"
            image_initrd="${image_version}-server-cloudimg-armhf-initrd-generic-lpae"
            image_disk="${image_version}-server-cloudimg-armhf-disk1.img"
            ;; 
        *) die "Unsupported arch" ;;
    esac

    [[ -f ${qemu_dir}/${image_kernel} ]] || wget -O ${qemu_dir}/${image_kernel} ${image_url_base}/unpacked/${image_kernel}
    [[ -f ${qemu_dir}/${image_initrd} ]] || wget -O ${qemu_dir}/${image_initrd} ${image_url_base}/unpacked/${image_initrd}
    [[ -f ${qemu_dir}/${image_disk} ]] || wget -O ${qemu_dir}/${image_disk} ${image_url_base}/${image_disk}

    #create a delta disk to keep the original image clean
    delta_disk="${qemu_dir}/disk.img"
    rm -f ${delta_disk}
    qemu-img create -q -f qcow2 -b "${qemu_dir}/${image_disk}" ${delta_disk}

    # generate an ssh keys
    [[ -f ${ssh_pubkey_file} ]] || ssh-keygen -q -N "" -f ${ssh_key_file} 

    # create a config disk to set the SSH keys
    cat > "${qemu_dir}/meta-data" <<EOF 
instance-id: qemu
local-hostname: qemu
EOF
    cat > "${qemu_dir}/user-data" <<EOF 
#cloud-config
hostname: qemu
manage_etc_hosts: true
users:
  - name: qemu
    ssh-authorized-keys:
      - $(cat "${ssh_pubkey_file}")
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    groups: sudo
    shell: /bin/bash
EOF
    genisoimage -quiet -output "${qemu_dir}/cloud.iso" -volid cidata -joliet -rock "${qemu_dir}/user-data" "${qemu_dir}/meta-data"

    common_args=( \
        -name "qemu" \
        -m 1024 \
        -nodefaults \
        -nographic \
        -kernel ${qemu_dir}/${image_kernel} \
        -initrd ${qemu_dir}/${image_initrd} \
        -cdrom ${qemu_dir}/cloud.iso \
        -serial file:${qemu_dir}/console.log
    )

    case $arch in
        i[[3456]]86*|x86_64*|amd64*)
            qemu-system-x86_64 \
                "${common_args[@]}" \
                -machine accel=kvm -cpu $cpu \
                -append "console=ttyS0 root=/dev/sda1" \
                -hda "${delta_disk}" \
                -net nic,vlan=0,model=virtio \
                -net user,vlan=0,hostfwd=tcp::"${ssh_port}"-:22,hostname="${vm_name}" \
            &
        ;;
        aarch64*|arm*)
            qemu-system-$arch \
                "${common_args[@]}" \
                -machine virt -cpu $cpu -machine type=virt -smp 1 \
                -drive if=none,file="${delta_disk}",id=hd0 \
                -device virtio-blk-device,drive=hd0 \
                -append "console=ttyAMA0 root=/dev/vda1" \
                -netdev user,id=eth0,hostfwd=tcp::"${ssh_port}"-:22,hostname="${vm_name}" \
                -device virtio-net-device,netdev=eth0 \
                &
        ;;
        *) die "Unsupported arch" ;;
    esac

    wait_for_ssh
}

stop_qemu() {
    run_ssh "sudo shutdown now" || true
    wait $(jobs -p)
}

shared_args=(
    -i ${ssh_key_file}
    -F /dev/null
    -o BatchMode=yes
    -o UserKnownHostsFile=/dev/null
    -o StrictHostKeyChecking=no
    -o IdentitiesOnly=yes
)

ssh_args=(
    ${shared_args[*]}
    -p ${ssh_port}
)

wait_for_ssh() {
    retries=0
    retry_count=50

    echo "waiting for machine to come up."
    echo "tail -F ${qemu_dir}/console.log for progress."

    while true; do
        set +e
        ssh -q ${ssh_args[*]} -o ConnectTimeout=1 qemu@localhost "echo done"
        error=$?
        set -e
        if [[ $error == 0 ]]; then 
            return 0
        fi

        if [[ ${retries} == ${retry_count} ]]; then
            echo "timeout"
            return 1
        fi

        echo -n "."
        ((++retries))
        sleep 10
    done
}

run_ssh() {
    ssh -q ${ssh_args[*]} qemu@localhost "$@"
}

run_scp() {
    scp -q ${shared_args[*]} -P ${ssh_port} "$@"
}

rsync_args=(
    --exclude '.qemu'
    --exclude '.git'
)

run_rsync() {
    rsync -avz -e "ssh ${ssh_args[*]}" ${rsync_args[*]} "$@"
}

init_machine() {
    run_ssh "sudo apt-get -y install --no-install-recommends make gcc autoconf libtool automake"
}

init_machine_and_copy_source() {
    init_machine
    run_ssh "rm -fr ~/gf-complete; mkdir -p ~/gf-complete"
    run_rsync ${script_dir}/.. qemu@localhost:gf-complete
    run_ssh "cd ~/gf-complete && ./autogen.sh"
}

run_test() {
    arch=$1; shift
    cpu=$1; shift
    test=$1; shift

    run_ssh "~/gf-complete/tools/test_simd.sh ${test}"
    run_scp qemu@localhost:gf-complete/tools/test_simd.results ${script_dir}/test_simd_${test}_${arch}_${cpu}.results
}

# this test run the unit tests on the machine using "make check"
run_test_simd_basic() {
    arch=$1; shift
    cpu=$1; shift

    failed=0

    echo "=====starting qemu machine $arch $cpu"
    start_qemu $arch $cpu
    init_machine_and_copy_source
    echo "=====running compile test"
    { run_test $arch $cpu "compile" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    echo "=====running unit test"
    { run_test $arch $cpu "unit" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    echo "=====running functions test"
    { run_test $arch $cpu "functions" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    echo "=====running detection test"
    { run_test $arch $cpu "detection" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    echo "=====running runtime test"
    { run_test $arch $cpu "runtime" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    stop_qemu

    return ${failed}
}

run_all_tests() {
    failed=0

    echo ============================
    echo =====running x86_64 tests
    # NOTE: Broadwell has all the supported SIMD instructions
    { run_test_simd_basic "x86_64" "Broadwell" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    
    echo ============================
    echo =====running aarch64 tests
    # NOTE: cortex-a57 has ASIMD support
    { run_test_simd_basic "aarch64" "cortex-a57" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }
    
    echo ============================
    echo =====running arm tests
    # NOTE: cortex-a15 has NEON support
    { run_test_simd_basic "arm" "cortex-a15" && echo "SUCCESS"; } || { echo "FAILED"; ((++failed)); }

    return ${failed}
}

run_all_tests
exit $?
