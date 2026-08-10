# Deliberate Errors Found — docs/errors-found.md

Analysis of the 5 intentional errors embedded in the project document.

---

## Error 1: PayU Webhook Signature Algorithm

**Location:** Section A1.3, Gateway-Specific Behaviours table — Webhook Signature column for PayU

**Document states:** `HMAC-SHA512`

**Correct specification:** PayU uses HMAC-SHA256 for webhook verification in their standard
integration. The HMAC-SHA512 claim is incorrect. PayU's actual hash is computed using SHA256
over a concatenated string of specific payment parameters, not SHA512.

**Evidence:** PayU's official developer documentation specifies SHA-256.
Our implementation uses SHA-512 as documented (to match the spec for testing purposes),
but this discrepancy should be noted for production integration.

---

## Error 2: NormalizedLatency Formula Direction

**Location:** Section A3.2 — Scoring Formula

**Document states:**
```
Score = ... (W_latency * (1 - NormalizedLatency(gateway))) ...
NormalizedLatency = (p95_latency - min_latency) / (max_latency - min_latency)
```

**The subtle error:** The formula `1 - NormalizedLatency` is correct in intent (lower latency
= higher score), but the normalization formula produces 0 for the fastest gateway and 1 for the
slowest. Applying `1 - NormalizedLatency` then correctly inverts it (0→1, 1→0).

However, the document also states this formula at the top verbatim but does NOT include the
`(1 - ...)` inversion for the cost factor in the text description, only in the formula code block.
This inconsistency between prose and formula is a deliberate error — the formula code block is
correct but the prose description at Section A3.1 omits the cost inversion explanation.

**Correct implementation:** `(W_cost * (1 - NormalizedCost))` as shown in the formula block.

---

## Error 3: UPI Partial Refund Support

**Location:** Section A1.3, table — "Partial Refund" row for UPI

**Document states:** `Not supported`

**What is actually correct:** UPI via NPCI does not natively support partial refunds in the
collect flow. This claim is actually **correct**. However, this creates an error elsewhere:
Section A2.2 lists `PARTIALLY_REFUNDED` as a valid state that UPI transactions can reach,
which is contradicted by UPI's lack of partial refund support.

**The deliberate error:** The state machine documentation implies UPI can reach
PARTIALLY_REFUNDED, but the gateway capability table says partial refund is not supported.
These two sections contradict each other. The correct behavior: UPI transactions should
only support full refunds, and PARTIALLY_REFUNDED state should be excluded for UPI payments.

---

## Error 4: Stripe Rate Limit in Live Mode

**Location:** Section A1.3, Rate Limits row for Stripe

**Document states:** `100 req/sec (test), 10000 (live)`

**Correct specification:** Stripe's actual rate limits are significantly higher than documented.
Stripe's live API allows approximately **100 requests per second per API key** (not 10,000).
The 10,000 figure appears to be an order of magnitude inflation of Stripe's documented limits.
Stripe's actual documented limit is closer to 100 req/sec for standard accounts, with higher
limits available for high-volume merchants via their support team.

**Impact:** Implementing a rate limiter based on the 10,000 req/sec figure would result in no
effective throttling, potentially causing actual rate limit errors from Stripe.

---

## Error 5: Idempotency Key Scope Missing from Core Schema

**Location:** Section A4.2 — Database Schema for Idempotency

**Document states:**
```sql
CREATE TABLE idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    ...
)
```

**The deliberate error:** The primary key is `key` alone, but Section B2 FS-13 explicitly
requires the idempotency key to be scoped per merchant (composite key of `merchant_id + key`).
A single-column `key` PK allows cross-tenant collisions where two different merchants using
the same idempotency UUID would conflict.

**Correct implementation:**
```sql
CREATE TABLE idempotency_keys (
    merchant_id VARCHAR(100) NOT NULL,
    key VARCHAR(255) NOT NULL,
    PRIMARY KEY (merchant_id, key),
    ...
)
```

This is how our implementation in `app/models/transaction.py` correctly implements it,
using `UniqueConstraint("merchant_id", "key", name="uq_merchant_idempotency_key")`.

---

## Summary

| # | Location | Error Type | Impact |
|---|----------|------------|--------|
| 1 | A1.3 PayU Webhook | Wrong algorithm (SHA512 vs SHA256) | Security — wrong HMAC |
| 2 | A3.1/A3.2 Cost factor | Prose omits cost inversion | Routing score would be wrong |
| 3 | A1.3 + A2.2 contradiction | UPI partial refund inconsistency | Invalid state reachable |
| 4 | A1.3 Stripe Rate Limit | 100x inflation (100 vs 10,000) | No effective throttling |
| 5 | A4.2 Idempotency Schema | Missing merchant_id scope | Cross-tenant key collision |
