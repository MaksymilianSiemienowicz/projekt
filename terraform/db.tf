resource "proxmox_vm_qemu" "db" {

  count = 3

  name        = "db${count.index + 1}"
  vmid        = "40${count.index}"
  target_node = "proxmox"
  agent       = 0

  clone   = "UbuntuTemplate"
  cores   = 4
  sockets = 1
  cpu     = "host"
  memory  = 8200

  scsihw = "virtio-scsi-pci"

  disks {
    ide {
      ide0 {
        cloudinit {
          storage = "SSD1"
        }
      }
    }
    scsi {
      scsi0 {
        disk {
          size      = "200G"
          storage   = "SSD1"
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
  ipconfig0  = "ip=10.0.0.11${count.index + 1}/24,gw=10.0.0.1"
  sshkeys = var.sshKey
  onboot = true
  network {
    model  = "virtio"
    bridge = "internal"

  }

}
