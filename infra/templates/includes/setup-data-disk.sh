# Mount NVMe instance storage at /data (two disks -> RAID 0, one disk -> plain ext4).
# Included into !Sub UserData blocks: escape literal dollar-brace expressions as ${!name}.
if lsblk | grep -q nvme1n1 && lsblk | grep -q nvme2n1; then
  if [ ! -e /dev/md0 ]; then
    mdadm --create --verbose /dev/md0 --level=0 --raid-devices=2 /dev/nvme1n1 /dev/nvme2n1 --assume-clean --run
    mkdir -p /etc/mdadm
    mdadm --detail --scan >> /etc/mdadm/mdadm.conf
  fi

  if ! blkid /dev/md0; then
    mkfs.ext4 -F /dev/md0
  fi

  mkdir -p /data
  echo "/dev/md0 /data ext4 defaults,nofail 0 2" >> /etc/fstab
  mount /data
elif lsblk | grep -q nvme1n1; then
  # -F: mkfs prompts on a whole device and there is no stdin under cloud-init.
  if ! blkid /dev/nvme1n1; then
    mkfs.ext4 -F /dev/nvme1n1
  fi

  mkdir -p /data
  echo "/dev/nvme1n1 /data ext4 defaults,nofail 0 2" >> /etc/fstab
  mount /data
fi

mkdir -p /data/tmp
