#!/usr/bin/env python3
"""
Monitor contínuo de telemetria, saturação de recursos e Circuit Breaker ativo para testes de carga do GoFleet.
Coleta métricas da API (Prometheus), RabbitMQ (management/metrics) e Docker stats a cada 1 segundo.
Gera log temporal detalhado e possui Circuit Breaker automático que interrompe o container de carga k6 se limites forem violados.
Suporta modo --wait-drain para monitorar até a drenagem completa de filas e outbox.
"""

import sys
import time
import json
import urllib.request
import urllib.error
import subprocess
import argparse
from datetime import datetime, timezone

API_METRICS_URL = "http://127.0.0.1:8080/metrics"

def parse_prometheus_text(content):
    metrics = {}
    for line in content.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 2:
            k, v = parts[0], parts[1]
            try:
                metrics[k] = float(v)
            except ValueError:
                pass
    return metrics

def get_api_metrics():
    try:
        req = urllib.request.Request(API_METRICS_URL)
        with urllib.request.urlopen(req, timeout=0.8) as resp:
            return parse_prometheus_text(resp.read().decode('utf-8'))
    except Exception:
        pass

    try:
        cmd = ["docker", "run", "--rm", "--network", "projeto-integrador-4_default", "curlimages/curl:latest", "-s", "http://api:8080/metrics"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2.0, text=True)
        if res.returncode == 0:
            return parse_prometheus_text(res.stdout)
    except Exception:
        pass
    return {}

def get_rabbitmq_queues():
    try:
        cmd = ["docker", "exec", "-i", "logistics-rabbitmq", "rabbitmqctl", "list_queues", "name", "messages", "messages_ready", "messages_unacknowledged", "--formatter", "json"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2.0, text=True)
        if res.returncode == 0:
            data = json.loads(res.stdout)
            queues = {}
            for item in data:
                queues[item.get("name")] = {
                    "messages": item.get("messages", 0),
                    "ready": item.get("messages_ready", 0),
                    "unack": item.get("messages_unacknowledged", 0),
                }
            return queues
    except Exception:
        pass
    return {}

def get_docker_stats():
    try:
        cmd = ["docker", "stats", "--no-stream", "--format", "{{.Name}}: {{.CPUPerc}} / {{.MemUsage}}", "logistics-postgres", "logistics-api", "logistics-rabbitmq"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2.5, text=True)
        if res.returncode == 0:
            lines = res.stdout.strip().splitlines()
            stats = {}
            for line in lines:
                parts = line.split(":")
                if len(parts) == 2:
                    stats[parts[0].strip()] = parts[1].strip()
            return stats
    except Exception:
        pass
    return {}

def get_simulator_status():
    try:
        cmd = ["docker", "ps", "-a", "--filter", "name=logistics-simulator", "--format", "{{.Status}}"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2.0, text=True)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except Exception:
        pass
    return "UNKNOWN"

def trigger_circuit_breaker(reason, k6_container):
    now_iso = datetime.now(timezone.utc).isoformat()
    print(f"\n🚨 [{now_iso}] [CIRCUIT BREAKER TRIP] Interrupção automática ativada: {reason}!", file=sys.stderr)
    stopped = False
    if k6_container:
        try:
            # Verifica se o container k6 existe e está em execução
            check_cmd = ["docker", "ps", "-q", "-f", f"name={k6_container}"]
            c_res = subprocess.run(check_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3, text=True)
            cid = c_res.stdout.strip()
            if cid:
                print(f"🛑 [{now_iso}] Interrompendo container de carga ativo: {k6_container} (ID: {cid})...", file=sys.stderr)
                stop_res = subprocess.run(["docker", "stop", "-t", "2", k6_container], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                if stop_res.returncode == 0:
                    print(f"✅ [{now_iso}] Container {k6_container} interrompido com sucesso pelo Circuit Breaker.", file=sys.stderr)
                    stopped = True
                else:
                    print(f"⚠️ [{now_iso}] Falha ao parar container: {stop_res.stderr}", file=sys.stderr)
            else:
                print(f"ℹ️ [{now_iso}] Container {k6_container} não está mais em execução.", file=sys.stderr)
        except Exception as e:
            print(f"Erro ao interromper container k6: {e}", file=sys.stderr)
    return {
        "tripped": True,
        "reason": reason,
        "timestamp_iso": now_iso,
        "target_container": k6_container,
        "stopped_successfully": stopped
    }

def main():
    parser = argparse.ArgumentParser(description="GoFleet Telemetry Monitor & Active Circuit Breaker")
    parser.add_argument("--duration", type=int, default=60, help="Duração padrão de amostragem em segundos (usada se --wait-drain for falso)")
    parser.add_argument("--max-duration", type=int, default=900, help="Limite máximo de segurança em segundos (padrão: 15 min)")
    parser.add_argument("--wait-drain", action="store_true", default=False, help="Mantém a coleta ativa até que todas as filas e outbox drenem")
    parser.add_argument("--output", type=str, default="telemetry_timeline.jsonl", help="Arquivo de saída JSONL")
    parser.add_argument("--k6-container", type=str, default="", help="Nome do container k6 para interrupção de emergência")
    parser.add_argument("--enable-circuit-breaker", action="store_true", default=True, help="Ativa circuit breakers automáticos")
    args = parser.parse_args()

    effective_limit = args.max_duration if args.wait_drain else args.duration
    print(f"Iniciando monitoramento de telemetria. Limite: {effective_limit}s (WaitDrain: {args.wait_drain}, CB: {args.enable_circuit_breaker}, Target: {args.k6_container})...")

    records = []
    prev_wait_count = None
    prev_wait_duration = None
    start_time = time.time()

    initial_dlq = None
    consecutive_sql_saturation = 0
    consecutive_outbox_backlog = 0
    consecutive_idle_ticks = 0
    has_seen_traffic = False

    circuit_breaker_result = None

    with open(args.output, "w") as f:
        while time.time() - start_time < effective_limit:
            now = time.time()
            elapsed = round(now - start_time, 2)
            api_m = get_api_metrics()
            queues = get_rabbitmq_queues()
            d_stats = get_docker_stats()
            sim_status = get_simulator_status()

            # Extração de métricas SQL
            open_conns = api_m.get('go_sql_open_connections{db_name="logistics_db"}', 0)
            in_use = api_m.get('go_sql_in_use_connections{db_name="logistics_db"}', 0)
            idle = api_m.get('go_sql_idle_connections{db_name="logistics_db"}', 0)
            wait_count = api_m.get('go_sql_wait_count_total{db_name="logistics_db"}', 0)
            wait_duration = api_m.get('go_sql_wait_duration_seconds_total{db_name="logistics_db"}', 0)

            delta_wait_count = 0 if prev_wait_count is None else max(0, wait_count - prev_wait_count)
            delta_wait_duration = 0 if prev_wait_duration is None else max(0, wait_duration - prev_wait_duration)
            prev_wait_count = wait_count
            prev_wait_duration = wait_duration

            # Outbox e filas
            outbox_pending = api_m.get('app_outbox_events_pending', 0)
            outbox_processed = api_m.get('app_outbox_events_processed_total{status="success"}', 0)
            
            proj_q = queues.get("api.delivery_projection", {}).get("messages", 0)
            dlq_q = queues.get("api.delivery_projection.dlq", {}).get("messages", 0)
            sim_q = queues.get("simulator.queue", {}).get("messages", 0)

            if initial_dlq is None:
                initial_dlq = dlq_q

            # Detecção de início de tráfego
            if in_use > 0 or outbox_pending > 0 or proj_q > 0 or sim_q > 0:
                has_seen_traffic = True

            rec = {
                "elapsed_s": elapsed,
                "timestamp": now,
                "sql_open_conns": open_conns,
                "sql_in_use": in_use,
                "sql_idle": idle,
                "sql_wait_count_total": wait_count,
                "delta_wait_count": delta_wait_count,
                "sql_wait_duration_total_s": wait_duration,
                "delta_wait_duration_s": delta_wait_duration,
                "outbox_pending_gauge": outbox_pending,
                "outbox_processed_total": outbox_processed,
                "queue_projection": proj_q,
                "queue_sim": sim_q,
                "queue_dlq": dlq_q,
                "simulator_status": sim_status,
                "docker_stats": d_stats,
            }
            records.append(rec)
            f.write(json.dumps(rec) + "\n")
            f.flush()

            # Resumo em stderr para visualização humana
            print(f"[{elapsed:5.1f}s] SQL InUse: {in_use:2.0f}/50 | Waits: {wait_count:3.0f} (+{delta_wait_count:2.0f}/s) | "
                  f"Outbox Pend: {outbox_pending:4.0f} | Q_Sim: {sim_q:5.0f} | Q_Proj: {proj_q:4.0f} | DLQ: {dlq_q} | "
                  f"Sim: {sim_status[:12]} | PG: {d_stats.get('logistics-postgres', 'N/A')} | Rabbit: {d_stats.get('logistics-rabbitmq', 'N/A')}")

            # -------------------------------------------------------------
            # Avaliação de Circuit Breakers Automáticos
            # -------------------------------------------------------------
            if args.enable_circuit_breaker:
                # 1. Qualquer mensagem NOVA na DLQ é fatal
                if dlq_q > initial_dlq:
                    circuit_breaker_result = trigger_circuit_breaker(
                        f"Nova mensagem na DLQ detectada (inicial={initial_dlq}, atual={dlq_q})", 
                        args.k6_container
                    )
                    break

                # 2. Saturação do pool com esperas por 5s consecutivos
                if in_use >= 50 and delta_wait_count > 100:
                    consecutive_sql_saturation += 1
                    if consecutive_sql_saturation >= 5:
                        circuit_breaker_result = trigger_circuit_breaker(
                            f"Saturação do pool SQL por 5s seguidos (50/50 em uso, esperas > 100/s)", 
                            args.k6_container
                        )
                        break
                else:
                    consecutive_sql_saturation = 0

                # 3. Explosão do backlog do Outbox (> 5.000 pendentes por 5s)
                if outbox_pending > 5000:
                    consecutive_outbox_backlog += 1
                    if consecutive_outbox_backlog >= 5:
                        circuit_breaker_result = trigger_circuit_breaker(
                            f"Backlog de Outbox excedeu 5.000 mensagens pendentes por 5s", 
                            args.k6_container
                        )
                        break
                else:
                    consecutive_outbox_backlog = 0

            # -------------------------------------------------------------
            # Avaliação de Drenagem Completa (--wait-drain)
            # -------------------------------------------------------------
            if args.wait_drain and has_seen_traffic:
                # Drenagem completa ocorre quando:
                # 1. Outbox pending zerado
                # 2. Fila do simulador zerada
                # 3. Fila de projeção zerada
                # 4. Conexões em uso zeradas
                # 5. Simulador concluído (Exited)
                is_idle = (outbox_pending == 0 and sim_q == 0 and proj_q == 0 and in_use == 0)
                sim_done = ("Exited" in sim_status or sim_status == "")
                if is_idle and sim_done:
                    consecutive_idle_ticks += 1
                    if consecutive_idle_ticks >= 8: # 8 segundos estáveis em repouso absoluto
                        print(f"\n🏁 [{elapsed:5.1f}s] Drenagem completa e repouso confirmados (Outbox=0, Filas=0, Sim={sim_status}).")
                        break
                else:
                    consecutive_idle_ticks = 0

            time.sleep(1.0)

    print(f"Coleta de telemetria concluída. Total de amostras gravadas: {len(records)}.")
    if circuit_breaker_result:
        print(f"Status do Circuit Breaker: DISPARADO ({circuit_breaker_result['reason']}) em {circuit_breaker_result['timestamp_iso']}.")
    else:
        print("Status do Circuit Breaker: NÃO DISPARADO (Normal).")

if __name__ == "__main__":
    main()
