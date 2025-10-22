resource "proxmox_vm_qemu" "kafka" {

  count = 3

  name        = "kafka${count.index + 1}"
  vmid        = "20${count.index}"
  target_node = "proxmox"
  agent       = 0

  clone   = "UbuntuTemplate"
  cores   = 2
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
          size      = "100G"
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
  ipconfig0  = "ip=10.0.0.10${count.index + 1}/24,gw=10.0.0.1"
  sshkeys = var.sshKey
  onboot = true
  network {
    model  = "virtio"
    bridge = "internal"

  }

}
