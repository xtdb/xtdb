output "kafka_bootstrap_server" {
  value = "${azurerm_container_app.kafka_app.name}:9092"
}
