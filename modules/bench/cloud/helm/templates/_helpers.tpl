{{- /* Return Kafka host from a bootstrap servers string (first entry) */ -}}
{{- define "xtdb.kafka.host" -}}
{{- $bootstrap := (default "" .) -}}
{{- $first := (splitList "," $bootstrap | first) -}}
{{- $parts := splitList ":" $first -}}
{{- $host := "localhost" -}}
{{- if ge (len $parts) 1 -}}
{{- $host = index $parts 0 -}}
{{- end -}}
{{- $host -}}
{{- end -}}

{{- /* Return Kafka port from a bootstrap servers string (first entry) */ -}}
{{- define "xtdb.kafka.port" -}}
{{- $bootstrap := (default "" .) -}}
{{- $first := (splitList "," $bootstrap | first) -}}
{{- $parts := splitList ":" $first -}}
{{- $port := "9092" -}}
{{- if ge (len $parts) 2 -}}
{{- $port = index $parts 1 -}}
{{- end -}}
{{- $port -}}
{{- end -}}
