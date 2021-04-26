#### Initial Setup

Below are steps to be followed to setup the cloud infrastructure for the project

* Provision a storage Account in US East, create a container and fileshare under the storage Account

* Provision a 2019-Datacenter Windows Azure VM. Access via RDP and setup Python, VSCode on the machine to execute the ingestion script
   * [[Reference](https://docs.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-windows)] Instructions to Create and manage Azure Files share with Windows virtual machines

* Mount the Storage Account FileShare in Azure VM to save the files via ingestion script
