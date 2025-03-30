{{/*
Expand the name of the chart.
*/}}
{{- define "hdfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hdfs.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if and .Release.Name (ne .Release.Name "") }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* 
Calculate heap size from memory resources by subtracting overhead
*/}}
{{- define "hdfs.calculateHeapSize" -}}
{{- $memory := . | replace "Gi" "000" | replace "Mi" "" | int -}}
{{- sub $memory 500 -}}
{{- end -}} 