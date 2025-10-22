resource "proxmox_vm_qemu" "CONTROL" {

  count = 1 

  name        = "control"
  vmid        = "102"
  target_node = "proxmox"
  agent       = 0

  clone   = "RockyTemplate"
  cores   = 2
  sockets = 1
  cpu     = "host"
  memory  = 4096

  scsihw = "virtio-scsi-pci"

  disks {
    ide {
      ide0 {
        cloudinit {
          storage = "HDD"
        }
      }
    }
    scsi {
      scsi0 {
        disk {
          size      = "80G"
          storage   = "HDD"
          iothread  = false
          replicate = false

        }
      }
    }
  }

  boot      = "order=scsi0"
  skip_ipv6 = true

  os_type = "cloud-init"

  ciuser     = var.username
  cipassword = var.userPassword
  nameserver = "10.0.0.2"
  ipconfig0  = "ip=10.0.0.3/24,gw=10.0.0.1"
  sshkeys = var.sshKey
  onboot = true
  network {
    model  = "virtio"
    bridge = "internal"

  }

}
