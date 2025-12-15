Simulacao de um Sistema de Comunicacao entre Nos Distribuidos (Python)
======================================================================

Overview
--------
- Objetivo: simular uma rede de mensagens/sensores entre nos distribuidos, com lider para coordenacao, carimbo de tempo (Lamport), eleicao Bully e exclusao mutua Ricart-Agrawala protegendo estado replicado via 2PC.
- Transporte: asyncio TCP com envelopes JSON (`src/common/messages.py`), com retries e backoff.
- Nos: `src/node/node.py` compoe Bully, Ricart-Agrawala, sincronizacao de relogio fisico (Cristian), replicacao 2PC e roteamento/caixa de correio (mailbox) para entrega indireta.
- Clientes: `src/clients/enqueue_client.py` (produtor de mensagens) e `src/clients/worker_client.py` (consumidor), ambos com IDs e Lamport.

Exemplos de uso
---------------
- Mensagem de sensor: `python -m src.clients.enqueue_client --id 9000 --payload '{"sensor":"temp","value":25}' --host 127.0.0.1 --port 8000`
- Consumo/ack: `python -m src.clients.worker_client --id 9001 --host 127.0.0.1 --port 8000`
- Broadcast/roteamento: use peers via `--peers host1:port1,host2:port2` para tentar entrega mesmo se o lider mudar; roteamento TTL e mailbox cuidam do encaminhamento.

Project layout
--------------
- `src/common`: config, Lamport clock, logging, message schema.
- `src/algorithms`: Bully e Ricart-Agrawala.
- `src/node`: ciclo de vida, transporte, replicacao 2PC, time sync, roteamento e mailbox.
- `src/clients`: CLIs de produtor/consumidor.
- `src/scripts`: orquestracao local (`run-local.ps1`).
- `tests/unit`: Lamport, Bully, Ricart-Agrawala; `tests/integration` cobre eleicao + replicacao/quorum.
- `scripts/bench.py`: benchmark de enqueue (latencia/throughput); `run-local.ps1`: sobe N nos locais para testes manuais.

Arquitetura (resumo)
--------------------
- Bully para eleicao (ID mais alto vence), Ricart-Agrawala para exclusao mutua, Lamport como relogio logico e Cristian para offset fisico.
- Lider replica fila via 2PC (REPL_PREPARE/COMMIT/ABORT) com quorum; tarefas persistidas em `data/tasks_<id>.json`.
- Roteamento P2P com TTL e mailbox para entrega indireta; reentrega se faltar ACK (`task_ack_timeout`).

Running locally
---------------
- Pre-requisitos: Python 3.11+, pip.
- Instalar deps:
  ```
  python -m venv .venv
  .\.venv\Scripts\activate
  pip install -r requirements.txt
  ```
- Subir 3 nos (PowerShell): `.\run-local.ps1 -Nodes 3`
- Produtor: `python -m src.clients.enqueue_client --id 9000 --host 127.0.0.1 --port 8000 --payload "hello world"`
- Consumidor: `python -m src.clients.worker_client --id 9001 --host 127.0.0.1 --port 8000`
- Testes: `pytest`
- Benchmark: `python scripts/bench.py --messages 60 --concurrency 15` (sobe e derruba o cluster automaticamente).

Testes manuais rapidos
----------------------
1) Suba 3 nos: `.\run-local.ps1 -Nodes 3`
2) Enqueue: `python -m src.clients.enqueue_client --id 9000 --host 127.0.0.1 --port 8000 --payload "hello"`
3) Dequeue/ACK: `python -m src.clients.worker_client --id 9001 --host 127.0.0.1 --port 8000`
4) Falha do lider: derrube o processo do lider (maior ID), repita enqueue/dequeue e veja reeleicao nos logs.
5) Verifique replica em disco: pare os nos e confira `data/tasks_<id>.json`.

Experimentos sugeridos
----------------------
- Carga crescente: variar `--messages` e `--concurrency` no `scripts/bench.py`.
- Falha de lider em carga: matar o processo do lider no meio do benchmark e medir tempo de recuperacao.
- Contencao mutex: rodar varios enqueues/dequeues simultaneos e inspecionar ordenacao via logs (`REQUEST_CS/REPLY_CS`).

Resultados de desempenho (enqueue)
----------------------------------
| Workload               | Messages | Conc | Throughput (msg/s) | Avg (s) | p50 (s) | p90 (s) | p95 (s) | p99 (s) |
|------------------------|----------|------|--------------------|---------|---------|---------|---------|---------|
| Enqueue-only benchmark | 60       | 15   | 14.4               | 0.9084  | 0.9967  | 1.0841  | 1.0943  | 1.1108  |
| Enqueue-only benchmark | 10       | 3    | 14.0               | 0.1963  | 0.2160  | 0.2320  | 0.2329  | 0.2337  |

Behavior and limits
-------------------
- Estado de mensagens e mantido em memoria e persistido em `data/tasks_<id>.json`, replicado via 2PC para seguidores; falha durante prepare/commit ainda pode perder mensagens nao commitadas se quorum falhar.
- Heartbeats do lider com eleicao Bully; sincronizacao fisica (Cristian) e logica (Lamport).
- Ricart-Agrawala serializa acesso ao estado; roteamento P2P com TTL e mailbox para comunicacao indireta.
- Reentrega em falta de ACK (`task_ack_timeout`); abort automatico de prepares antigos.
