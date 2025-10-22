variable "userPassword" {
	type = string
	sensitive = true
	description = "Password for VM user"
}

variable "username" {
	type = string
	description = "Default vm username"
}

variable "sshKey" {
	type = string
	description = "Ssh key form service node"
	sensitive = true
}
