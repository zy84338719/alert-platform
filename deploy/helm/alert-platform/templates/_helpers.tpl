{{- define "alert-platform.fullname" -}}{{ .Release.Name }}-alert-platform{{- end -}}
{{- define "alert-platform.labels" -}}
app.kubernetes.io/name: alert-platform
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}
