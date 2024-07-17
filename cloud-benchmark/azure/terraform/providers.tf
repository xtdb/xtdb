terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.111.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "benchmark-terraform"
    storage_account_name = "benchmarkterraform"
    container_name       = "benchmarkterraform"
    key                  = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}
