### Parte A — Resolução de problema real

Contexto geral: sexta-feira o gateway aprovou US$ 1,3M (1.587 transações), mas no nosso banco só apareceram 421 `order.approved`. Ou seja, o funil que vai do webhook até `lead_events` quebrou em algum ponto.

#### 1) Hipóteses (do mais provável ao menos provável) (não consegui encontrar 5 hipóteses)
Enunciado: `5 hipóteses iniciais ranqueadas por probabilidade, com a justificativa do ranking.`
- 1) Chave de criptografia errada/rotacionada no gateway “grummer” → falha de decrypt em massa.  
  Quando a chave quebra, o decrypt falha logo no começo e mandamos tudo para `lead.dead.decrypt_failed`, sem gerar `processed_webhooks` nem `lead_events`.
- 2) Mudança de contrato no payload (ex.: `payment.status` ≠ "approved" ou campo renomeado) → API não publica `lead.received`.  
  A API só publica para `lead.received` quando `event = 'order.approved'` e `payment.status = 'approved'`.
- 3) Gateway não enviou (ou houve uma sequência grande de falhas na rota HTTP).  
  Dashboard de gateway mede aprovação financeira, não garante entrega de webhook. Menos provável, mas possível problema relacionado a infra (DNS/timeout/5xx).

#### 2) O que verificar primeiro e por quê
Enunciado: `Que dados, logs e queries você consultaria primeiro e por quê. Inclua queries SQL e comandos do RabbitMQ que você usaria.`
- RabbitMQ (verificar o estado atual das filas)
  - Ver profundidade da fila e consumidores nas filas críticas:
    ```bash
    rabbitmqadmin -u guest -p guest list queues name messages_ready messages_unacknowledged consumers
    rabbitmqadmin -u guest -p guest list consumers
    # Ou via HTTP API
    curl -s -u guest:guest http://localhost:15672/api/queues/%2F/lead.received | jq '.messages,.consumers'
    curl -s -u guest:guest http://localhost:15672/api/queues/%2F/lead.dead.decrypt_failed | jq '.messages'
    curl -s -u guest:guest http://localhost:15672/api/consumers | jq '.[].queue.name'
    ```
  - Pegar algumas mensagens de amostra (conteúdo e formato):
    ```bash
    rabbitmqadmin -u guest -p guest get queue=lead.received requeue=false count=5
    rabbitmqadmin -u guest -p guest get queue=lead.dead.decrypt_failed requeue=false count=5
    ```

- Banco (MySQL) — contagens por estágio e dead letters (janela da sexta)
  ```sql
  -- delimito o período
  SET @t0 = '2026-05-15 00:00:00';
  SET @t1 = '2026-05-15 23:59:59';

  -- 1) Webhooks recebidos pela API, por gateway e status de decrypt
  SELECT gateway,
         SUM(decrypted_body IS NOT NULL) AS decrypted_ok,
         SUM(decrypted_body IS NULL)     AS decrypt_failed,
         COUNT(*)                        AS total
  FROM raw_payloads
  WHERE received_at BETWEEN @t0 AND @t1
  GROUP BY gateway;

  -- 2) Webhooks válidos marcados como processados (por evento)
  SELECT event, COUNT(*)
  FROM processed_webhooks
  WHERE processed_at BETWEEN @t0 AND @t1
    AND event = 'order.approved'
  GROUP BY event;

  -- 3) Leads efetivamente materializados
  SELECT COUNT(*) AS approved_events
  FROM lead_events
  WHERE event = 'order.approved'
    AND gateway_time BETWEEN @t0 AND @t1;

  -- 4) Dead letters por origem (API ou consumer)
  SELECT origin, COUNT(*)
  FROM lead_dead_letter
  WHERE created_at BETWEEN @t0 AND @t1
  GROUP BY origin
  ORDER BY COUNT(*) DESC;

  -- 5) Se desconfiar em falha de contrato, verificar status no JSON
  SELECT JSON_UNQUOTE(JSON_EXTRACT(decrypted_body, '$.payment.status')) AS pay_status,
         COUNT(*)
  FROM raw_payloads
  WHERE decrypted_body IS NOT NULL
    AND JSON_UNQUOTE(JSON_EXTRACT(decrypted_body, '$.event')) = 'order.approved'
    AND received_at BETWEEN @t0 AND @t1
  GROUP BY pay_status;
  ```

- Logs
  - API: procurar por "Decryption failed", "Encrypted payload validation failed", "Published lead.received".
  - Consumer webhook: "processed lead.received", "processing attempt failed", "publish error", DLQ `lead.dead.consumer_failed`.

Por que nessa ordem: Rabbit mostra de cara se o consumidor parou; a base revela em qual estágio o volume despenca (decrypt, processed, event); e DLQ/Logs mostram o motivo (chave, schema, erro de DB/publicação).

#### 3) Como diferenciar os cenários
Enunciado: `Como você diferenciaria entre os cenários: (a) gateway nunca enviou, (b) webhook chegou mas o decrypt falhou, (c) lead foi publicado na fila mas o consumer travou, (d) consumer publicou mas o distribuidor não consumiu.`
- Passo 1 — Comparar `raw_payloads` com o total do gateway (1.587):
  - Se `raw_payloads` ≈ 1.587 → o gateway enviou (descarto a).
  - Se bem abaixo → provável (a) gateway não enviou/entregou.
- Passo 2 — Diferenças entre `raw_payloads` e `processed_webhooks`/`lead.dead.decrypt_failed`:
  - Muitos `decrypted_body IS NULL` e DLQ `lead.dead.decrypt_failed` alto → (b) decrypt falhou (chave/cabeçalhos).
- Passo 3 — Diferenças entre `processed_webhooks` e `lead_events`:
  - `processed_webhooks` ≈ 1.587, `lead.received` com `messages_ready` alto e `consumers=0/1` travado → (c) consumer travou.
  - `processed_webhooks` ≈ 1.587, `lead.received` baixo e `lead_events` baixo também → a API não publicou (provável mudança de contrato).
- Passo 4 — Verificação de distribuição entre os canais:
  - Se `lead_events` ≈ 1.587 e `distribution_status` com muito `pending` e filas `dist.*` cheias → (d) distribuidor não consumiu.

Consulta auxiliar para o Passo 3 (gap entre processado e event):
```sql
-- Transações aprovadas (processadas) sem o respectivo evento materializado
SELECT p.transaction_id
FROM processed_webhooks p
WHERE p.event = 'order.approved'
  AND p.processed_at BETWEEN @t0 AND @t1
  AND NOT EXISTS (
    SELECT 1 FROM lead_events le
    WHERE le.transaction_id = p.transaction_id
      AND le.event = 'order.approved'
  );
```

#### 4) Reprocessamento dos 1.166 faltantes (sem duplicar os 421)
Enunciado: `Plano de reprocessamento dos 1.166 leads faltantes, sem duplicar os 421 que já entraram. Inclua o SQL e a estratégia de fila.`

No projeto já temos idempotência (`orders.uq_orders_gateway_tx`, `lead_events.uq_order_event`, `distribution_status.uq_dist_order_channel`), então posso republicar com segurança só os que faltam — depois de corrigir a causa raiz.

- Passo A — Montar a lista dos faltantes (base: `processed_webhooks` e JSON de `raw_payloads`):
  ```sql
  -- Candidatos a reprocessar com corpo já decriptado
  SELECT rp.id              AS id_raw_payload,
         rp.correlation_id  AS correlation_id,
         rp.gateway,
         rp.received_at,
         rp.decrypted_body  AS payload_json
  FROM processed_webhooks p
  JOIN raw_payloads rp
    ON JSON_UNQUOTE(JSON_EXTRACT(rp.decrypted_body, '$.transaction_id')) = p.transaction_id
  WHERE p.event = 'order.approved'
    AND p.processed_at BETWEEN @t0 AND @t1
    AND NOT EXISTS (
      SELECT 1 FROM lead_events le
      WHERE le.transaction_id = p.transaction_id
        AND le.event = 'order.approved'
    );

  -- Se houve decrypt falho no período (grummer):
  SELECT id, correlation_id, gateway, received_at, original_body
  FROM raw_payloads
  WHERE gateway = 'grummer'
    AND decrypted_body IS NULL
    AND received_at BETWEEN @t0 AND @t1; -- após corrigir a chave, re-decriptar e incluir
  ```

- Passo B — Estratégia de fila
  - Primeiro, corrigir a causa raiz (ex.: chave AES).
  - Publicar direto em `lead.received` em lotes pequenos (respeitando `prefetch`/capacidade) — ou usar uma fila dedicada `lead.received.reprocess` com um consumer temporário para drená-la.
  - Comandos (exemplo com `rabbitmqadmin`), usando o mesmo formato da API:
    ```bash
    # Para cada linha (id_raw_payload, correlation_id, gateway, received_at, payload_json):
    rabbitmqadmin -u guest -p guest publish routing_key=lead.received \
      payload='{"correlation_id":"<corr>","id_raw_payload":<id>,"id_processed_webhook":null,"gateway":"<gw>","received_at":"<ts>","payload":<payload_json>}'
    ```
  - Monitorar `lead.received` (dreno), `lead.dead.consumer_failed` (não deve crescer) e aplicar backpressure se necessário.

- Passo C — Verificação pós-reprocessamento
  - Confirmar `lead_events` = 1.587 para o período e conciliar com o dashboard do gateway.
  - Conferir `distribution_status` (ao menos `pending` criado para todos; entregas variam por canal).

#### 5) Medidas preventivas (alertas, métricas, mudanças de código)
Enunciado: `3 medidas preventivas (alertas, métricas, mudanças de código) para essa classe de bug não voltar.`

- Alertas/métricas ponta a ponta e por estágio
  - Reconciliation contínua: comparar, por gateway e janela de 5–10 min, `count(gateway.approved)` vs `lead_events(event='order.approved')`. Alertar se o gap for muito alto dentro desta janela de tempo.
  - Lag/saúde de filas: alertar se `lead.received.messages_ready > 0` por > 3 min, `consumers=0`, ou `connection`/`channel` inativos.

- Mudanças de código/operações
  - Rotação segura de chave: variável `GRUMMER_AES256_KEY_BASE64` versionada (usando a prática de "canary release"); validar a chave com payload de teste no healthcheck.
  - Job oficial de reprocessamento: CLI/endpoint que lê `raw_payloads` (inclui re-decriptação quando preciso) e republica de forma idempotente.

- Observabilidade e idempotência reforçadas
  - Criação de dashboards para cada fase da esteira de integração.
  - Métrica de “aprovações filtradas” (quando `event='order.approved'` mas `payment.status != 'approved'`) para detectar mudanças de contrato ou até mesmo alguma mudança de negócio.

### Parte B — Decisões de arquitetura

#### 1) Idempotência — por que `transaction_id + event` (e não só `transaction_id`)?

Enunciado: `Idempotência. Por que a chave natural é transaction_id + event (order + event) e não só transaction_id? O que a inclusão do event na chave permite que a chave só com transaction_id não permitiria Em que cenário cada uma falha?`

- O que acrescentar `event` ao fluxo e à base permite:
  - Registrar o ciclo de vida completo da transação (ex.: `order.approved`, `order.canceled`, `order.pending`).
  - Reprocessar/republicar de forma seletiva por evento (ex.: só `order.approved`) sem bloquear outros eventos do mesmo pedido.
  - Disparar automações diferentes por tipo de evento (ex.: distribuição de leads só em `order.approved`).
  - Auditar decisões: cada mudança de estado vira um registro único em `lead_events` (como no `uq_order_event`).
- Quando a utilização de apenas `transaction_id` falha:
  - Transações com múltiplos eventos (ex.: `approved` → `refund`) — o segundo evento seria descartado como “duplicado”, corrompendo o estado e a auditoria.
- Quando `transaction_id + event` falha (ou precisa de refinamento):
  - Eventos multi-ocorrência para um mesmo evento. Solução: analisar o contexto e ampliar esta chave composta.
  - Presença de eventos, vindos do gateway, com nomes distintos mas que, na prática, para a esteira de integração, significam a mesma coisa (ex.: `dispute.lost` vs `chargeback`). Solução: normalizar os eventos antes de processá-los (ex.: definição de um ENUM interno).
- Trade-offs:
  - Chave simples `transaction_id`: índice menor e consultas mais baratas, mas perde semântica e gera inconsistência de estado.
  - Chave composta `transaction_id + event`: índice maior e mais linhas em `lead_events`, porém correto, auditável e seguro para reprocessamento.

#### 2) Cripto — AES-256-CBC vs AES-256-GCM para um novo webhook da GEX

Enunciado: `2. Cripto. AES-256-CBC vs AES-256-GCM: qual você escolheria para um webhook novo da GEX (não-grummer)? Por quê? A quais ataques o CBC é vulnerável e o GCM não é?`

- Escolha: AES-256-GCM (AEAD).
- Por quê:
  - Confidencialidade + integridade/autenticidade no mesmo primitivo (tag GCM). Evita aceitar payloads alterados em trânsito.
  - Performance melhor e constante com AES-NI; menos round-trips (uma operação ao invés de “cifra + MAC” separados).
  - Suporte a AAD: podemos “amarrar” `gateway`, `transaction_id`, `event`, `timestamp` aos cabeçalhos (se AAD não conferir, rejeita).
- A quais ataques o CBC é vulnerável e o GCM não:
  - Padding oracle (Vaudenay): vazamento de plaintext via diferenças de erro de padding.
  - Malleability/bit-flipping: sem MAC correto, é possível alterar bits previsivelmente sem quebrar a descriptografia.
  - IV previsível/reatilização de IV: pode vazar XOR do primeiro bloco/permitir manipulações no primeiro bloco.
  - GCM não sofre esses vetores porque autentica o ciphertext; porém é crítico NÃO reutilizar nonce/IV no GCM (reutilização é catastrófica — facilita forja de tags e vazamento de plaintexts).
- Boas práticas GEX (GCM):
  - Nonce de 96 bits único por mensagem (aleatório criptográfico ou derivado determinístico de `gateway_event_id` via HKDF/HMAC) e incluído no cabeçalho.
  - Cabeçalhos: `kid` (versão da chave), `iv`/`nonce`, `tag`, `cipher=AES-256-GCM`; payload em Base64.
  - Fail-closed: divergência de tag/AAD → 401/422 e NUNCA tenta processar; rotation com `kid` + canário.

#### 3) Backpressure — SMS com 90% de erro: como proteger o resto do sistema?

Enunciado: `3. Backpressure. Se o canal SMS começar a falhar (provedor com 90% de erro), como você protege o resto do sistema? Por que RabbitMQ + retry exponencial sozinho não basta?`

- Por que RabbitMQ + retry exponencial sozinho não basta:
  - Head-of-line blocking: mensagens com erro ocupam consumidores por muito tempo, atrasando mensagens válidas.
  - Retry storm: mesmo com backoff, grande volume falho reentra e satura filas/threads/conexões de I/O.
  - Sem circuito semântico: a fila não sabe quando “desistir temporariamente” do provedor; mantém esforço inútil e custo.
- Arquitetura/procedimentos (GEX):
  - Bulkheads por canal: fan-out do evento para filas específicas (`dist.sms`, `dist.whatsapp`, `dist.email`), cada uma com `prefetch`, concorrência e rate-limit próprios.
  - Retry com isolamento: `dist.sms.retry` e `dist.sms.dead` (parking-lot) após N tentativas; nada volta para a fila “quente”.
  - Circuit breaker: ao detectar um grande volume de mensagens com erro, dentro de uma determinada janela de tempo, pausar o consumo da fila, e redirecionar as mensagens para outra fila com um TTL maior. Reabrir quando a saúde normalizar.
  - Orçamentação/quotas: limite de “in-flight” por canal e shed de carga (se backlog > threshold, manter novos SMS em `pending` no DB e não enfileirar até a drenagem).
  - Observabilidade: dashboards por canal (fila, DLQ, taxa de erro, latência de provedor) + alerta quando `messages_ready` fica alto por N minutos.
- Trade-offs:
  - Maior complexidade operacional (mais filas/exchanges e políticas de DLX/TTL).

#### 4) Migração entre linguagens — quando vale mover receiver+decrypt (Python → Go) e quando não vale

Enunciado: `4. Migração entre linguagens. Cite 3 sinais que te diriam que vale migrar a parte de receiver+decrypt para outra linguagem (ex.: Python - Go), e 3 sinais que te diriam que NÃO vale. Use o contexto da GEX descrito acima, sem responder no genérico.`

- 3 sinais de que vale a pena migrar:
  - Gargalo comprovado no decrypt/ingest: CPU > 80% em `integration_api` só cifrando/decifrando.
  - Concurrency bound: muitos workers Python e, ainda assim, throughput efetivo baixo por limites de GIL/overhead de asyncio em tarefas CPU-bound; Go entrega melhor paralelismo para CPU-bound (AES-GCM nativo) e menor overhead por goroutine.
  - Operacional/custo: containers Python grandes (mem > 400–500 MB por réplica), cold start lento e pressão de GC sob pico; estimativa mostra redução de custo/latência no caminho crítico movendo só o receiver+decrypt para um binário Go leve (sidecar) mantendo o contrato do `lead.received`.
- 3 sinais de que não vale a pena migrar:
  - Métricas indicam que o gargalo real está no MySQL (`sp_insert_lead`) ou no RabbitMQ/rede (I/O-bound), não no decrypt. Trocar a linguagem não resolveria a causa raíz.
  - Estabilidade e segurança: fluxo crítico de cripto já está testado (test vectors), auditado e sem incidentes; reimplementar eleva risco de regressão/segurança e consome tempo de engenharia.
  - Time/stack: equipe com maior proficiência em Python, SLOs atendidos (p95 bom, erros baixos) e escala horizontal barata o suficiente.
