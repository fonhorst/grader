{{/*
Clickhouse common labels
*/}}
{{- define "clickhouse.labels" -}}
service: {{ .Values.labels.service }}
{{- end }} 