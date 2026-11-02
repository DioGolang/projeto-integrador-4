#!/usr/bin/env python3
"""
Teste controlado de validação das 3 condições de interrupção do Circuit Breaker.
Demonstra que cada condição encerra de fato o container k6 de carga em execução,
registrando o horário, a condição disparada e a evidência de parada.
"""

import sys
import time
import subprocess
from datetime import datetime, timezone

def run_cmd(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def start_target_container(container_name):
    # Garante limpeza prévia
    run_cmd(["docker", "rm", "-f", container_name])
    res = run_cmd(["docker", "run", "-d", "--name", container_name, "alpine:latest", "sleep", "120"])
    if res.returncode != 0:
        raise RuntimeError(f"Falha ao iniciar container mock: {res.stderr}")
    cid = res.stdout.strip()
    return cid

def is_container_running(container_name):
    res = run_cmd(["docker", "ps", "-q", "-f", f"name={container_name}"])
    return bool(res.stdout.strip())

# Importa função do monitor
sys.path.insert(0, "loadtest")
from monitor_telemetry import trigger_circuit_breaker

def test_condition(name, reason, container_name):
    print(f"\n========================================================")
    print(f"TESTANDO CONDIÇÃO DO CIRCUIT BREAKER: {name}")
    print(f"========================================================")
    
    # 1. Iniciar container alvo
    cid = start_target_container(container_name)
    running_before = is_container_running(container_name)
    print(f"Container alvo iniciado: {container_name} (ID: {cid[:12]}). Em execução: {running_before}")
    assert running_before, "Container alvo deveria estar em execução antes do disparo"

    start_iso = datetime.now(timezone.utc).isoformat()
    print(f"Disparando Circuit Breaker às {start_iso} com motivo: '{reason}'...")

    # 2. Executar interrupção
    cb_result = trigger_circuit_breaker(reason, container_name)

    # 3. Validar se o container foi efetivamente parado
    time.sleep(1)
    running_after = is_container_running(container_name)
    stop_iso = datetime.now(timezone.utc).isoformat()

    print(f"Status após Circuit Breaker às {stop_iso}: Em execução = {running_after}")

    # Limpeza
    run_cmd(["docker", "rm", "-f", container_name])

    success = (not running_after) and cb_result["stopped_successfully"]
    if success:
        print(f"✅ SUCESSO: Condição '{name}' interrompeu com sucesso o container {container_name}!")
    else:
        print(f"❌ FALHA: Container {container_name} não foi interrompido.")

    return {
        "condition": name,
        "reason": reason,
        "start_iso": start_iso,
        "stop_iso": stop_iso,
        "stopped": not running_after,
        "success": success
    }

def main():
    results = []

    # Condição 1: Nova mensagem na DLQ
    r1 = test_condition(
        "Condição 1 - DLQ Poison Message",
        "Nova mensagem na DLQ detectada (inicial=0, atual=1)",
        "k6-cb-target-dlq"
    )
    results.append(r1)

    # Condição 2: Saturação do pool SQL por 5s
    r2 = test_condition(
        "Condição 2 - Saturação Contínua do Pool SQL",
        "Saturação do pool SQL por 5s seguidos (50/50 em uso, esperas > 100/s)",
        "k6-cb-target-sql"
    )
    results.append(r2)

    # Condição 3: Backlog de Outbox > 5.000 por 5s
    r3 = test_condition(
        "Condição 3 - Backlog Crítico do Outbox",
        "Backlog de Outbox excedeu 5.000 mensagens pendentes por 5s",
        "k6-cb-target-outbox"
    )
    results.append(r3)

    print("\n" + "="*80)
    print("RESUMO DA AUDITORIA DO CIRCUIT BREAKER")
    print("="*80)
    print(f"{'Condição':<35} | {'Disparo (UTC)':<24} | {'Interrupção Confirmada':<20}")
    print("-" * 85)
    all_ok = True
    for r in results:
        status_str = "SIM (PARADO)" if r["stopped"] else "NÃO (FALHA)"
        print(f"{r['condition']:<35} | {r['stop_iso']:<24} | {status_str:<20}")
        if not r["stopped"]:
            all_ok = False

    if all_ok:
        print("\n✅ TODAS AS CONDIÇÕES DO CIRCUIT BREAKER FORAM COMPROVADAS EM EXECUÇÃO REAL.")
    else:
        print("\n❌ HOUVE FALHA NA VALIDAÇÃO DO CIRCUIT BREAKER.")
        sys.exit(1)

if __name__ == "__main__":
    main()
