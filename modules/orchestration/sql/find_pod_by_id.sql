SELECT id AS "id: PodId",
       canvas AS "canvas: CanvasId",
       server AS "server: ServerId",
       name,
       comment,
       port,
       bind_ip,
       advertise_ip,
       ingress AS "ingress: IngressKind",
       receive_proxy_protocol AS "receive_proxy_protocol: ProxyProtocolVersion",
       tls_sni,
       tls_dns_provider AS "tls_dns_provider: DnsProviderId",
       tls_domain_id,
       tls_acme_directory,
       route AS "route: Json<Route>"
FROM orchestration_pod
WHERE id = $1
