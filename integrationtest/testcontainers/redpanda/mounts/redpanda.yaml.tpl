# Injected by testcontainers
redpanda:
  admin:
    address: 0.0.0.0
    port: 9644

  kafka_api:
    - name: external
      address: 0.0.0.0
      port: 9092
      authentication_method: {{ .KafkaAPI.AuthenticationMethod }}

    # This listener is required for the schema registry client. The schema
    # registry client connects via an advertised listener like a normal Kafka
    # client would do. It can't use the other listener because the mapped
    # port is not accessible from within the Redpanda container.
    - name: internal
      address: 0.0.0.0
      port: 9093
      authentication_method: {{ if .KafkaAPI.EnableAuthorization }}sasl{{ else }}none{{ end }}

  {{ range .KafkaAPI.Listeners }}
    - name: {{ .Name }}
      address: {{ .Address }}
      port: {{ .Port }}
      authentication_method: {{ .AuthenticationMethod }}
  {{ end }}

  advertised_kafka_api:
    - name: external
      address: {{ .KafkaAPI.AdvertisedHost }}
      port: {{ .KafkaAPI.AdvertisedPort }}
    - name: internal
      address: 127.0.0.1
      port: 9093
  {{ range .KafkaAPI.Listeners }}
    - name: {{ .Name }}
      {{ if .AdvertisedAddress }}
      address: {{ .AdvertisedAddress }}
      {{ else }}
      address: {{ $.KafkaAPI.AdvertisedHost }}
      {{ end }}
      port: {{ .AdvertisedPort }}
  {{ end }}

  {{ with .RPCListener }}
  rpc_server:
    address: "0.0.0.0"
    port: 30092

  advertised_rpc_api:
    address: {{ .AdvertisedHost }}
    port: {{ .AdvertisedPort }}
  {{ end }}

  {{ if .SeedServers }}
  empty_seed_starts_cluster: false
  seed_servers:
    {{ range .SeedServers }}
    - host:
        address: {{ .Address }}
        port: {{ .Port }}
    {{ end }}
  {{ end }}

  {{ if .EnableTLS }}
  admin_api_tls:
    - enabled: true
      cert_file: /etc/redpanda/cert.pem
      key_file: /etc/redpanda/key.pem
  kafka_api_tls:
    - name: external
      enabled: true
      cert_file: /etc/redpanda/cert.pem
      key_file: /etc/redpanda/key.pem
  {{ end }}

schema_registry:
  schema_registry_api:
  - address: "0.0.0.0"
    name: main
    port: 8081
    authentication_method: {{ .SchemaRegistry.AuthenticationMethod }}

  {{ if .EnableTLS }}
  schema_registry_api_tls:
  - name: main
    enabled: true
    cert_file: /etc/redpanda/cert.pem
    key_file: /etc/redpanda/key.pem
  {{ end }}

schema_registry_client:
  brokers:
    - address: localhost
      port: 9093

auto_create_topics_enabled: {{ .AutoCreateTopics }}

