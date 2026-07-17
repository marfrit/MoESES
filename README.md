# MoESES — MoE Streaming Expert Server

**Serving DeepSeek-class MoE models on 2× NVIDIA DGX Spark by streaming routed experts on demand from a cheap SSD/RAM storage box over RoCE.**

> Status: **design stage / call for builders.** The topology is specced, the Sparks exist, the storage box does not (yet). If you build this before I do, I get my evaluation for free and you get a YouTube video. Everybody wins. Issues and results very welcome.

## The problem

DeepSeek V3/R1-class models are ~671B parameters. Two DGX Sparks give you 256 GB of combined LPDDR5X — not enough for the full weight set, even at int4 (~370 GB for one expert set). But MoE models only *activate* ~37B parameters per token, and expert selection is highly predictable (~72% router hit rate on a warm cache in prior work).

So: keep the dense/shared weights and hot experts resident on the Sparks, and **page cold experts over the network** from a box whose only jobs are (a) a big shared page cache and (b) fast sequential reads.

Prior art: the [Colibri](https://github.com/example) approach does expert paging via `pread()` on Apple Silicon, CPU-only, ceiling ~1 tok/s. MoESES is the same storage idea, but feeding **GPU inference** — which moves the bottleneck from the pager to the link, and makes 200G networking actually worth having.

## Topology: switchless triangle

Each DGX Spark has 2× QSFP112 ports (ConnectX-7). Three point-to-point DAC links, no switch:

```
        bosch (DGX Spark)
        /               \
   QSFP112            QSFP112
      /                   \
 escher (DGX) --------- muse (storage box)
   QSFP112   QSFP112   (dual-port ConnectX-6 Dx)
```

- **bosch ↔ escher**: NCCL / RoCE clustering link for the resident/dense shards (NVIDIA-approved QSFP112 DAC; a generic FS 400G DAC has also been observed training at 200G).
- **bosch ↔ muse** and **escher ↔ muse**: expert stream, NVMe-oF over RDMA (RoCEv2), QSFP56 200G DACs. QSFP56 DACs plug directly into QSFP112 cages (same 4-lane form factor; the CX-7 negotiates down).

### GB10 ConnectX-7 quirks you must know

- Each physical QSFP cage carries **2× 100G MACs on two separate PCIe Gen5 x4 links**. One MAC unbonded = ~100 Gbit/s. To reach 200G per cage you must bond the two MACs **across the two different x4 links** — bonding same-link MACs caps at ~92–95 Gbit/s.
- Many units ship firmware-pinned at ~13–16 Gbit/s despite negotiating 200G. Fix: ConnectX-7 firmware update **plus a full cold power-drain** (not just a reboot). Verify with Perftest before believing any number.
- The "second cage shows DOWN" symptom is a cabling artifact, not a hardware lock. Both cages are independently usable; RoCE to third-party endpoints works (validated by third parties with stock perftest — see ServeTheHome's GB10 networking deep-dive).

## Why 256 GB RAM on the storage box is the whole point

Both Sparks read the **same** read-only expert set. The storage box's Linux page cache therefore acts as a **shared warm-expert L2**: an expert pulled for bosch is already RAM-resident when escher asks. EPYC's 8 DDR4 channels (~150 GB/s) fan that cache out to both links. RAM size directly buys aggregate tok/s — it is the best €/token on the storage side. The SSDs only need to cover cache misses.

## Parts list (storage box "muse")

Indicative German used-market prices (eBay.de / Kleinanzeigen / servershop24), mid-2026. Volatile — especially DDR4, which is trending up. Treat as a floor, not a budget.

| Part | SKU | ~€ |
|---|---|---|
| Motherboard | Supermicro **H12SSL-i** (SP3, PCIe 4.0) or ASRock Rack **ROMED8-2T** | 350–550 |
| CPU | AMD **EPYC 7302/7302P** (16c Rome) or **7313** (Milan) | 100–250 |
| RAM | **8× 32 GB DDR4-3200 ECC RDIMM** (Samsung M393A4K40DB3-CWE / Micron MTA36ASF4G72PZ-3G2) | 280–400+ |
| NIC | Mellanox **ConnectX-6 Dx MCX623106AS-CDAT** (2× QSFP56 200GbE) | 250–450 |
| Cables | 2× QSFP56 200G DAC (FS Q56-PC0xx / Mellanox MCP1650-V001E30); keep a QSFP28 100G DAC as fallback | 40–70 ea |
| SSDs | 4× Gen4 NVMe 2 TB striped (~28 GB/s) — WD SN850X or Lexar NM790 — on an ASUS Hyper M.2 x16 carrier (board must expose x4x4x4x4 bifurcation; H12SSL and ROMED8-2T do) | 90–130 ea |
| Cooler / PSU / case | Noctua NH-U14S TR4-SP3, 650 W Gold, any case with airflow over the M.2 carrier | ~280 |

**All-in: roughly 1.7–2.8 k€** depending on market luck. The NVMe-oF target does essentially no CPU work — you are buying PCIe lanes and memory channels, not cores.

Compute side (not included): 2× DGX Spark, which you presumably already regret buying.

## Expected performance (back of envelope)

DeepSeek int4, ~37B active params/token, most of it routed experts:

| Link config | Bandwidth | Est. token ceiling* |
|---|---|---|
| 100G unbonded (day 1) | ~12.5 GB/s | ~2–4 tok/s |
| 200G bonded per Spark | ~25 GB/s | ~5–8 tok/s |

\* Assuming ~72% of expert fetches are absorbed by the muse RAM tier + locally pinned hot experts, leaving ~3–5 GB of miss traffic per token. These numbers are hypotheses to be destroyed by measurement, not promises.

The big software lever: **router-logit prefetching** — request next-layer experts while the current layer computes, hiding link latency behind GPU work.

## Software plan

**Storage (muse):** stock Linux + `nvmet` / `nvmet-rdma` exporting the mdraid0 stripe as one namespace. `nvmet-tcp` is the guaranteed fallback if RoCEv2 PFC/ECN tuning fights DGX-OS.

**Sparks:** `nvme connect -t rdma` (or `-t tcp`), then mount/mmap the remote namespace as the expert store.

**Inference engine:** fork of **llama.cpp**, not vLLM. Rationale:

- llama.cpp already loads weights via `mmap` — mount the NVMe-oF namespace and experts page over the link on day one with zero code changes (slow and blind, but a real baseline).
- MoE routing lives in the ggml graph; the point where router logits are known before expert FFNs execute is where the async prefetcher hooks in.
- vLLM assumes GPU-resident weights at load; PagedAttention pages KV-cache, not weights. Retrofitting an SSD tier means fighting the model loader, scheduler, and PyTorch memory management simultaneously.
- Study first: **ktransformers** (GPU + CPU/RAM expert offload, router-aware placement — closest existing design) and **DeepSpeed ZeRO-Inference** NVMe offload (batch-oriented, but the offload code exists).

The missing piece to build: ggml has no concept of "this tensor is not resident yet, stall/prefetch" mid-graph. That async expert pager + CUDA-side integration is the actual project.

## Milestones

1. **Protocol validation (0 €):** `nvme connect -t rdma` from bosch to a throwaway `nvmet-rdma` target on any Linux box with any RDMA NIC (an old ConnectX-3/4 suffices). Proves DGX-OS talks NVMe-oF/RoCE to third-party hardware before money moves.
2. **muse build + link bring-up:** firmware-update + cold-drain both Sparks, perftest each link, verify QSFP56 DAC trains in the QSFP112 cage.
3. **Naive baseline:** mmap DeepSeek expert set over NVMe-oF, run unmodified llama.cpp, measure. Zero code.
4. **Bonding:** 2× 100G MACs → 200G per Spark, re-measure.
5. **Async prefetcher:** router-logit-driven expert prefetch in the llama.cpp fork. This is where the tok/s lives.

## Risk register

- RoCE to third-party target on *this specific* DGX-OS image: high confidence, unverified. Milestone 1 exists for exactly this. NVMe/TCP is the fallback.
- QSFP112 cage ↔ QSFP56 DAC negotiation: expected fine, test before bulk-buying cables.
- DDR4 RDIMM prices: rising. Buy RAM first.
- The prefetcher is real systems work. The hardware is the easy half.

## License / contributing

Do whatever you want with this design. If you build it, open an issue with your numbers — especially tok/s at milestones 3–5 and whether the QSFP56 DAC trained cleanly. Benchmarks > opinions.

---

*Named after the guy who parted a large body of water so his people could cross. Here it's expert weights crossing a QSFP link, served by a host called `muse` to two Sparks called `bosch` and `escher`.*
