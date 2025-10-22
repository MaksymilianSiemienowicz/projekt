resource "proxmox_vm_qemu" "IPA" {

  count = 1 

  name        = "ipavm"
  vmid        = "101"
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
          size      = "60G"
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
  nameserver = "1.1.1.1"
  ipconfig0  = "ip=10.0.0.2/24,gw=10.0.0.1"
  sshkeys = var.sshKey
  onboot = true
  network {
    model  = "virtio"
    bridge = "internal"

  }

}
